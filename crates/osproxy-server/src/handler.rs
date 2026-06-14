//! The ingress handler: authenticates (stub), builds a request context, and
//! drives the engine pipeline, mapping the outcome to an HTTP response.

use std::sync::atomic::{AtomicU64, Ordering};

use osproxy_core::{ErrorCode, PrincipalId, RequestId};
use osproxy_engine::{Pipeline, RequestError};
use osproxy_sink::OpenSearchSink;
use osproxy_spi::{HeaderView, Principal, Protocol, RequestCtx};
use osproxy_transport::{IngressHandler, IngressRequest, IngressResponse};

use crate::tenancy::ReferenceTenancy;

/// The concrete pipeline this binary serves.
pub type AppPipeline = Pipeline<ReferenceTenancy, OpenSearchSink>;

/// Adapts the engine pipeline to the transport's [`IngressHandler`] contract.
#[derive(Debug)]
pub struct AppHandler {
    pipeline: AppPipeline,
    request_seq: AtomicU64,
}

impl AppHandler {
    /// Wraps a pipeline.
    #[must_use]
    pub fn new(pipeline: AppPipeline) -> Self {
        Self {
            pipeline,
            request_seq: AtomicU64::new(0),
        }
    }

    /// A per-request correlation id. A monotonic counter is enough for the
    /// reference binary; a real deployment would carry a propagated trace id.
    fn next_request_id(&self) -> RequestId {
        let n = self.request_seq.fetch_add(1, Ordering::Relaxed) + 1;
        RequestId::from(format!("req-{n}").as_str())
    }
}

impl IngressHandler for AppHandler {
    async fn handle(&self, req: IngressRequest) -> IngressResponse {
        // Stub authentication: M1 has no mTLS/token yet, so every caller is the
        // same anonymous principal. Auth attaches here in the next slice.
        let principal = Principal::new(PrincipalId::from("anonymous"));
        let request_id = self.next_request_id();

        let ctx = RequestCtx::new(
            &principal,
            &request_id,
            req.method,
            req.endpoint,
            Protocol::Http1,
            &req.logical_index,
            HeaderView::new(&req.headers),
            &req.body,
        );

        match self.pipeline.handle(&ctx).await {
            Ok(resp) => IngressResponse::json(resp.status, resp.body),
            Err(err) => IngressResponse::json(status_for(&err), error_body(&err)),
        }
    }
}

/// Maps a request-path error to an HTTP status, by its stable code.
fn status_for(err: &RequestError) -> u16 {
    match err.code() {
        ErrorCode::PartitionUnresolved | ErrorCode::UnsupportedEndpoint => 400,
        ErrorCode::AuthFailed => 401,
        ErrorCode::Unauthorized => 403,
        ErrorCode::PlacementMissing => 404,
        ErrorCode::StaleEpoch => 409,
        ErrorCode::UpstreamFailed => 502,
        ErrorCode::PlacementBackendUnavailable | ErrorCode::Overloaded => 503,
        // ErrorCode is non-exhaustive; an unmapped code is an internal fault.
        _ => 500,
    }
}

/// A value-free JSON error body carrying the stable code and retryability, so a
/// client or LLM can act on it without any tenant data leaking (NFR-S2).
fn error_body(err: &RequestError) -> Vec<u8> {
    format!(
        r#"{{"error":"{}","retryable":{}}}"#,
        err.code().as_slug(),
        err.retryable(),
    )
    .into_bytes()
}
