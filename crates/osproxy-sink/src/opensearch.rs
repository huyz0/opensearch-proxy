//! The direct-to-OpenSearch [`Sink`]: maps each [`WriteOp`] to a REST call and
//! delivers it over a pooled HTTP connection.
//!
//! Connection reuse (TCP, and TLS once the crypto seam is wired) comes from
//! `hyper-util`'s pooled client, so repeated writes to a cluster amortize the
//! handshake (NFR-P). M1 speaks cleartext HTTP to a configured per-cluster
//! endpoint; the TLS [`CryptoProvider`](osproxy_spi) connector attaches here in
//! the transport slice without changing this mapping.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_core::ClusterId;
use osproxy_spi::Protocol;
use serde_json::Value;

use crate::ack::{OpResult, WriteAck};
use crate::batch::{WriteBatch, WriteOp};
use crate::error::SinkError;
use crate::read::{CountOutcome, ReadOp, ReadOutcome, Reader, SearchOp, SearchOutcome};
use crate::sink::Sink;
use crate::wire::{build_request, doc_uri, parse_result};

type HttpClient = Client<HttpConnector, Full<Bytes>>;

/// One cluster's base URL plus its own pooled HTTP/1.1 and HTTP/2 clients.
///
/// Each cluster owns its pools (not a single shared client), so connection-pool
/// state is **sharded per cluster** — a busy cluster's pool lock never contends
/// with another's (NFR-P, `docs/01` §7).
#[derive(Debug)]
struct ClusterPool {
    base: String,
    client_h1: HttpClient,
    client_h2: HttpClient,
}

impl ClusterPool {
    /// Builds the per-cluster pools for a base URL.
    fn new(base: String) -> Self {
        Self {
            base,
            client_h1: Client::builder(TokioExecutor::new()).build_http(),
            client_h2: Client::builder(TokioExecutor::new())
                .http2_only(true)
                .build_http(),
        }
    }

    /// The pooled client for a resolved upstream protocol: the HTTP/2
    /// (prior-knowledge) client for `Http2`/`Grpc`, the HTTP/1.1 client otherwise.
    fn client(&self, protocol: Protocol) -> &HttpClient {
        match protocol {
            Protocol::Http2 | Protocol::Grpc => &self.client_h2,
            _ => &self.client_h1,
        }
    }
}

/// The default per-request upstream deadline: a hung upstream that accepts the
/// connection but never responds must not stall the request forever (NFR-R7).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// A [`Sink`] that writes directly to OpenSearch clusters over pooled HTTP.
///
/// Holds a `ClusterPool` per cluster — its own base URL and pooled HTTP/1.1
/// and HTTP/2 (prior-knowledge) clients. Each operation selects the client
/// matching its resolved upstream [`Protocol`] (`docs/04` §7), so the proxy can
/// speak h2 to a cluster that supports it while defaulting to h1. Every dispatch
/// is bounded by a per-request timeout so a stuck upstream fails fast (NFR-R7).
#[derive(Debug)]
pub struct OpenSearchSink {
    clusters: HashMap<ClusterId, ClusterPool>,
    timeout: Duration,
}

impl OpenSearchSink {
    /// Builds a sink that routes each cluster to its configured base URL, giving
    /// each cluster its own pooled clients (sharded pools, `docs/01` §7).
    #[must_use]
    pub fn new(endpoints: HashMap<ClusterId, String>) -> Self {
        let clusters = endpoints
            .into_iter()
            .map(|(cluster, base)| (cluster, ClusterPool::new(base)))
            .collect();
        Self {
            clusters,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Sets the per-request upstream timeout (builder style).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Resolves a cluster's pool, or a transport error if it is unconfigured.
    fn pool_for(&self, cluster: &ClusterId) -> Result<&ClusterPool, SinkError> {
        self.clusters.get(cluster).ok_or(SinkError::Transport {
            kind: "no endpoint configured for target cluster",
        })
    }

    /// Sends a request over `client`, bounded by the per-request timeout: a
    /// transport failure or an elapsed deadline is a retryable [`SinkError`].
    async fn send(
        &self,
        client: &HttpClient,
        req: Request<Full<Bytes>>,
        fail_kind: &'static str,
    ) -> Result<Response<Incoming>, SinkError> {
        match tokio::time::timeout(self.timeout, client.request(req)).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(SinkError::Transport { kind: fail_kind }),
            Err(_elapsed) => Err(SinkError::Transport {
                kind: "upstream timeout",
            }),
        }
    }

    /// POSTs a (partition-filtered) query body to `{index}/{verb}` and returns
    /// the upstream status and raw response body. Shared by search and count.
    async fn post_query(&self, verb: &str, op: &SearchOp) -> Result<(u16, Vec<u8>), SinkError> {
        let pool = self.pool_for(&op.target.cluster)?;
        let uri = format!("{}/{}/{verb}", pool.base, op.target.index.as_str());
        let req = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(op.body.clone())))
            .map_err(|_| SinkError::Transport {
                kind: "building upstream query request",
            })?;

        let resp = self
            .send(pool.client(op.protocol), req, "upstream query failed")
            .await?;
        let status = resp.status().as_u16();
        if status >= 500 {
            return Err(SinkError::Upstream {
                status,
                retryable: matches!(status, 502..=504),
            });
        }
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|_| SinkError::Transport {
                kind: "reading upstream query response",
            })?
            .to_bytes()
            .to_vec();
        Ok((status, body))
    }

    /// Sends a single operation and parses its result.
    async fn dispatch(&self, op: &WriteOp) -> Result<OpResult, SinkError> {
        let pool = self.pool_for(&op.target.cluster)?;
        let (req, fallback_id) = build_request(&pool.base, &op.target.index, &op.doc)?;

        let resp = self
            .send(pool.client(op.protocol), req, "upstream request failed")
            .await?;
        let status = resp.status().as_u16();
        if status >= 500 {
            return Err(SinkError::Upstream {
                status,
                retryable: matches!(status, 502..=504),
            });
        }

        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|_| SinkError::Transport {
                kind: "reading upstream response",
            })?
            .to_bytes();
        Ok(parse_result(&body, fallback_id, status))
    }
}

impl Reader for OpenSearchSink {
    async fn get(&self, op: ReadOp) -> Result<ReadOutcome, SinkError> {
        let pool = self.pool_for(&op.target.cluster)?;
        let uri = doc_uri(
            &pool.base,
            &op.target.index,
            Some(&op.id),
            op.routing.as_deref(),
        );
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Full::new(Bytes::new()))
            .map_err(|_| SinkError::Transport {
                kind: "building upstream read request",
            })?;

        let resp = self
            .send(pool.client(op.protocol), req, "upstream read failed")
            .await?;
        let status = resp.status().as_u16();
        // 404 is a normal "document does not exist"; only 5xx is a real failure.
        if status >= 500 {
            return Err(SinkError::Upstream {
                status,
                retryable: matches!(status, 502..=504),
            });
        }
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|_| SinkError::Transport {
                kind: "reading upstream read response",
            })?
            .to_bytes()
            .to_vec();
        Ok(if status == 200 {
            ReadOutcome::found(status, body)
        } else {
            ReadOutcome::not_found(status, body)
        })
    }

    async fn search(&self, op: SearchOp) -> Result<SearchOutcome, SinkError> {
        let (status, body) = self.post_query("_search", &op).await?;
        Ok(SearchOutcome::new(status, body))
    }

    async fn count(&self, op: SearchOp) -> Result<CountOutcome, SinkError> {
        let (status, body) = self.post_query("_count", &op).await?;
        let count = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|v| v.get("count").and_then(Value::as_u64))
            .unwrap_or(0);
        Ok(CountOutcome::new(status, count))
    }
}

impl Sink for OpenSearchSink {
    async fn write(&self, batch: WriteBatch) -> Result<WriteAck, SinkError> {
        // M1 batches are single-op; the loop is the M3 bulk seam (writes to one
        // target are issued in order to preserve item positioning).
        let mut results = Vec::with_capacity(batch.len());
        for op in batch.ops() {
            results.push(self.dispatch(op).await?);
        }
        Ok(WriteAck::new(results))
    }
}
