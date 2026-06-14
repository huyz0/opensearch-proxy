//! The ingress handler: authenticates the caller, builds a request context, and
//! drives the engine pipeline, mapping the outcome to an HTTP response.

use std::sync::atomic::{AtomicU64, Ordering};

use osproxy_core::{ErrorCode, RequestId};
use osproxy_engine::{Pipeline, RequestError};
use osproxy_sink::OpenSearchSink;
use osproxy_spi::{AuthError, Authenticator, ClientCredentials, HeaderView, Protocol, RequestCtx};
use osproxy_transport::{IngressHandler, IngressRequest, IngressResponse};

use crate::tenancy::ReferenceTenancy;

/// The concrete pipeline this binary serves.
pub type AppPipeline = Pipeline<ReferenceTenancy, OpenSearchSink>;

/// Adapts the engine pipeline to the transport's [`IngressHandler`] contract,
/// authenticating each request with the configured [`Authenticator`].
#[derive(Debug)]
pub struct AppHandler<A> {
    pipeline: AppPipeline,
    authenticator: A,
    request_seq: AtomicU64,
}

impl<A: Authenticator> AppHandler<A> {
    /// Wraps a pipeline and an authenticator.
    #[must_use]
    pub fn new(pipeline: AppPipeline, authenticator: A) -> Self {
        Self {
            pipeline,
            authenticator,
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

impl<A: Authenticator> IngressHandler for AppHandler<A> {
    async fn handle(&self, req: IngressRequest) -> IngressResponse {
        let request_id = self.next_request_id();

        // Proxy admin endpoint: the LLM-facing /debug/explain/{request_id}.
        // Returns only shape-level data (docs/05 §6); it would be auth-gated in
        // a real deployment, which attaches with the TLS/mTLS slice.
        if let Some(id) = req.path.strip_prefix("/debug/explain/") {
            return match self.pipeline.explain(&RequestId::from(id)) {
                Some(doc) => IngressResponse::json(200, doc.to_string().into_bytes()),
                None => IngressResponse::json(404, br#"{"error":"unknown_request_id"}"#.to_vec()),
            };
        }

        // Authenticate before any routing. The bearer token is consumed here and
        // never reaches the pipeline or telemetry.
        let principal = match self
            .authenticator
            .authenticate(&credentials_from(&req.headers))
            .await
        {
            Ok(principal) => principal,
            Err(err) => {
                return IngressResponse::json(err.http_status(), auth_error_body(&err))
                    .with_header("x-request-id", request_id.as_str());
            }
        };

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

        // Echo the request id so a client (or LLM) can fetch its
        // /debug/explain/{id} afterward.
        let response = match self.pipeline.handle(&ctx).await {
            Ok(resp) => IngressResponse::json(resp.status, resp.body),
            Err(err) => IngressResponse::json(status_for(&err), error_body(&err)),
        };
        response.with_header("x-request-id", request_id.as_str())
    }
}

/// Extracts client credentials from request headers. M1 reads a bearer token
/// from `Authorization`; the mTLS client-cert subject attaches with the TLS
/// slice.
fn credentials_from(headers: &[(String, String)]) -> ClientCredentials {
    let bearer_token = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .and_then(|(_, v)| v.strip_prefix("Bearer "))
        .map(str::to_owned);
    ClientCredentials {
        bearer_token,
        client_cert_subject: None,
    }
}

/// A value-free JSON body for an auth failure.
fn auth_error_body(err: &AuthError) -> Vec<u8> {
    format!(r#"{{"error":"{}"}}"#, err.code().as_slug()).into_bytes()
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
