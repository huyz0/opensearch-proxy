//! The direct-to-OpenSearch [`Sink`]: maps each [`WriteOp`] to a REST call and
//! delivers it over a pooled HTTP connection.
//!
//! Connection reuse (TCP, and TLS once the crypto seam is wired) comes from
//! `hyper-util`'s pooled client, so repeated writes to a cluster amortize the
//! handshake (NFR-P). M1 speaks cleartext HTTP to a configured per-cluster
//! endpoint; the TLS [`CryptoProvider`](osproxy_spi) connector attaches here in
//! the transport slice without changing this mapping.

use std::collections::HashMap;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_core::ClusterId;
use serde_json::Value;

use crate::ack::{OpResult, WriteAck};
use crate::batch::{WriteBatch, WriteOp};
use crate::error::SinkError;
use crate::read::{CountOutcome, ReadOp, ReadOutcome, Reader, SearchOp, SearchOutcome};
use crate::sink::Sink;
use crate::wire::{build_request, doc_uri, parse_result};

type HttpClient = Client<HttpConnector, Full<Bytes>>;

/// A [`Sink`] that writes directly to OpenSearch clusters over pooled HTTP.
#[derive(Debug)]
pub struct OpenSearchSink {
    client: HttpClient,
    /// Per-cluster base URL (scheme + authority), e.g. `http://10.0.0.1:9200`.
    endpoints: HashMap<ClusterId, String>,
}

impl OpenSearchSink {
    /// Builds a sink that routes each cluster to its configured base URL.
    ///
    /// The pooled client is shared across all clusters; connections are keyed by
    /// authority internally, so per-cluster reuse is automatic.
    #[must_use]
    pub fn new(endpoints: HashMap<ClusterId, String>) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self { client, endpoints }
    }

    /// Resolves a cluster's base URL, or a transport error if unconfigured.
    fn base_for(&self, cluster: &ClusterId) -> Result<&str, SinkError> {
        self.endpoints
            .get(cluster)
            .map(String::as_str)
            .ok_or(SinkError::Transport {
                kind: "no endpoint configured for target cluster",
            })
    }

    /// POSTs a (partition-filtered) query body to `{index}/{verb}` and returns
    /// the upstream status and raw response body. Shared by search and count.
    async fn post_query(&self, verb: &str, op: &SearchOp) -> Result<(u16, Vec<u8>), SinkError> {
        let base = self.base_for(&op.target.cluster)?;
        let uri = format!("{base}/{}/{verb}", op.target.index.as_str());
        let req = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(op.body.clone())))
            .map_err(|_| SinkError::Transport {
                kind: "building upstream query request",
            })?;

        let resp = self
            .client
            .request(req)
            .await
            .map_err(|_| SinkError::Transport {
                kind: "upstream query failed",
            })?;
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
        let base = self.base_for(&op.target.cluster)?;
        let (req, fallback_id) = build_request(base, &op.target.index, &op.doc)?;

        let resp = self
            .client
            .request(req)
            .await
            .map_err(|_| SinkError::Transport {
                kind: "upstream request failed",
            })?;
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
        let base = self.base_for(&op.target.cluster)?;
        let uri = doc_uri(base, &op.target.index, Some(&op.id), op.routing.as_deref());
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Full::new(Bytes::new()))
            .map_err(|_| SinkError::Transport {
                kind: "building upstream read request",
            })?;

        let resp = self
            .client
            .request(req)
            .await
            .map_err(|_| SinkError::Transport {
                kind: "upstream read failed",
            })?;
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
