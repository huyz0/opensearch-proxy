//! OTLP/HTTP span exporter.
//!
//! Ships the shape-only OTLP `ResourceSpans` payloads built by
//! [`osproxy_observe::resource_spans`] to a collector's `/v1/traces` endpoint
//! over HTTP with `Content-Type: application/json` (the OTLP/HTTP JSON binding).
//!
//! Export is **read-only and never on the request's critical path** (`docs/05`,
//! ADR-005): [`SpanExporter::export`] returns immediately and the actual POST
//! runs in a spawned task whose result is ignored — a slow or down collector can
//! never add latency to, or fail, a client request. Telemetry is best-effort by
//! construction.
#![deny(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Method, Request};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_observe::SpanExporter;
use serde_json::Value;
use tokio::sync::Semaphore;

/// Per-export deadline: a hung collector connection cannot outlive this, so
/// background export tasks never accumulate indefinitely.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on concurrent in-flight exports. When the collector is slow/down, exports
/// beyond this are **dropped** (best-effort telemetry) rather than queued — so a
/// failing collector cannot grow memory/FDs without bound under load.
const MAX_INFLIGHT: usize = 256;

/// An [`SpanExporter`] that POSTs OTLP/HTTP JSON spans to a collector.
///
/// Construct it with the collector's base URL (e.g.
/// `http://otel-collector:4318`); the exporter appends the standard
/// `/v1/traces` path. A pooled HTTP client is reused across exports.
///
/// **Must be constructed within a Tokio runtime** (it captures the runtime
/// handle to spawn background sends). Outside a runtime, export is a no-op rather
/// than a panic — telemetry is best-effort and never affects the caller.
#[derive(Clone, Debug)]
pub struct OtlpHttpExporter {
    endpoint: String,
    client: Client<HttpConnector, Full<Bytes>>,
    handle: Option<tokio::runtime::Handle>,
    inflight: Arc<Semaphore>,
}

impl OtlpHttpExporter {
    /// Builds an exporter targeting `endpoint_base` (the collector base URL).
    /// The OTLP `/v1/traces` path is appended automatically.
    #[must_use]
    pub fn new(endpoint_base: &str) -> Self {
        let endpoint = format!("{}/v1/traces", endpoint_base.trim_end_matches('/'));
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self {
            endpoint,
            client,
            handle: tokio::runtime::Handle::try_current().ok(),
            inflight: Arc::new(Semaphore::new(MAX_INFLIGHT)),
        }
    }

    /// The full `/v1/traces` URL this exporter posts to.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl SpanExporter for OtlpHttpExporter {
    fn export(&self, payload: Value) {
        // No runtime to spawn on (constructed outside Tokio): drop, never panic.
        let Some(handle) = self.handle.clone() else {
            return;
        };
        // Bound concurrency: if too many exports are already in flight (a slow or
        // down collector), drop this one rather than queueing unboundedly.
        let Ok(permit) = Arc::clone(&self.inflight).try_acquire_owned() else {
            return;
        };
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        // Fire-and-forget: the POST runs in the background under a deadline and its
        // result is discarded, so collector latency/failure never reaches the
        // request path.
        handle.spawn(async move {
            let _permit = permit; // released when the export finishes or times out
            let Ok(body) = serde_json::to_vec(&payload) else {
                return;
            };
            let Ok(req) = Request::builder()
                .method(Method::POST)
                .uri(&endpoint)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body)))
            else {
                return;
            };
            let _ = tokio::time::timeout(EXPORT_TIMEOUT, client.request(req)).await;
        });
    }
}
