//! The direct-to-OpenSearch [`Sink`]: maps each [`WriteOp`] to a REST call and
//! delivers it over a pooled HTTP connection.
//!
//! Connection reuse (TCP, and TLS once the crypto seam is wired) comes from
//! `hyper-util`'s pooled client, so repeated writes to a cluster amortize the
//! handshake (NFR-P). M1 speaks cleartext HTTP to a configured per-cluster
//! endpoint; the TLS [`CryptoProvider`](osproxy_spi) connector attaches here in
//! the transport slice without changing this mapping.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_core::{Clock, ClusterId, SystemClock};
use osproxy_spi::Protocol;
use serde_json::Value;

use crate::ack::{OpResult, WriteAck};
use crate::batch::{WriteBatch, WriteOp};
use crate::breaker::Breaker;
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
    /// Passive health breaker: a run of failures opens it and the cluster is
    /// shed until a cooldown elapses (health-checked eviction).
    breaker: Breaker,
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
            breaker: Breaker::default(),
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

/// How many consecutive transport/timeout failures open a cluster's breaker.
const DEFAULT_FAILURE_THRESHOLD: u32 = 5;

/// How long a cluster is shed once its breaker opens, before a half-open trial.
const DEFAULT_COOLDOWN: Duration = Duration::from_secs(5);

/// A [`Sink`] that writes directly to OpenSearch clusters over pooled HTTP.
///
/// Holds a `ClusterPool` per cluster — its own base URL and pooled HTTP/1.1
/// and HTTP/2 (prior-knowledge) clients. Each operation selects the client
/// matching its resolved upstream [`Protocol`] (`docs/04` §7), so the proxy can
/// speak h2 to a cluster that supports it while defaulting to h1. Every dispatch
/// is bounded by a per-request timeout so a stuck upstream fails fast (NFR-R7),
/// and a per-cluster circuit breaker sheds a cluster that keeps failing.
pub struct OpenSearchSink {
    clusters: HashMap<ClusterId, ClusterPool>,
    timeout: Duration,
    failure_threshold: u32,
    cooldown: Duration,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for OpenSearchSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected `Clock` is not `Debug`; the rest is the useful shape.
        f.debug_struct("OpenSearchSink")
            .field("clusters", &self.clusters)
            .field("timeout", &self.timeout)
            .field("failure_threshold", &self.failure_threshold)
            .field("cooldown", &self.cooldown)
            .finish_non_exhaustive()
    }
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
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            cooldown: DEFAULT_COOLDOWN,
            clock: Arc::new(SystemClock),
        }
    }

    /// Sets the per-request upstream timeout (builder style).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the circuit-breaker thresholds: open after `failure_threshold`
    /// consecutive failures, shed for `cooldown` before a half-open trial.
    #[must_use]
    pub fn with_breaker(mut self, failure_threshold: u32, cooldown: Duration) -> Self {
        self.failure_threshold = failure_threshold;
        self.cooldown = cooldown;
        self
    }

    /// Swaps the clock the breaker reads (tests inject a `ManualClock`).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Resolves a cluster's pool, or a transport error if it is unconfigured.
    fn pool_for(&self, cluster: &ClusterId) -> Result<&ClusterPool, SinkError> {
        self.clusters.get(cluster).ok_or(SinkError::Transport {
            kind: "no endpoint configured for target cluster",
        })
    }

    /// Sends a request to a cluster's pool, bounded by the per-request timeout
    /// and gated by the cluster's circuit breaker.
    ///
    /// A shed request (breaker open) and a transport/timeout failure are both
    /// retryable [`SinkError`]s; failures feed the breaker so a persistently
    /// failing cluster is evicted until it recovers (health-checked eviction).
    async fn send(
        &self,
        pool: &ClusterPool,
        protocol: Protocol,
        req: Request<Full<Bytes>>,
        fail_kind: &'static str,
    ) -> Result<Response<Incoming>, SinkError> {
        if !pool.breaker.allows(self.clock.now(), self.cooldown) {
            return Err(SinkError::Transport {
                kind: "cluster shed (circuit open)",
            });
        }
        match tokio::time::timeout(self.timeout, pool.client(protocol).request(req)).await {
            Ok(Ok(resp)) => {
                pool.breaker.record_success();
                Ok(resp)
            }
            Ok(Err(_)) => {
                pool.breaker
                    .record_failure(self.clock.now(), self.failure_threshold);
                Err(SinkError::Transport { kind: fail_kind })
            }
            Err(_elapsed) => {
                pool.breaker
                    .record_failure(self.clock.now(), self.failure_threshold);
                Err(SinkError::Transport {
                    kind: "upstream timeout",
                })
            }
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
            .send(pool, op.protocol, req, "upstream query failed")
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
            .send(pool, op.protocol, req, "upstream request failed")
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
            .send(pool, op.protocol, req, "upstream read failed")
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
