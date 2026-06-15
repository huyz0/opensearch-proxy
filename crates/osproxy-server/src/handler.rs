//! The ingress handler: authenticates the caller, builds a request context, and
//! drives the engine pipeline, mapping the outcome to an HTTP response.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use osproxy_core::{Clock, ErrorCode, RequestId};
use osproxy_engine::{Pipeline, RequestError};
use osproxy_observe::{InMemoryDirectiveStore, Metrics, PoolSnapshot};
use osproxy_sink::OpenSearchSink;
use osproxy_spi::{
    AuthError, Authenticator, ClientCredentials, HeaderView, HttpMethod, Protocol, RequestCtx,
};
use osproxy_transport::{IngressHandler, IngressRequest, IngressResponse};

use crate::directives_api::decode_directive_set;
use crate::log::{NoLog, RequestLog};
use crate::tenancy::ReferenceTenancy;

/// The privileged fleet-directive admin channel: a shared store to publish into,
/// gated by a bearer token, with a clock to resolve relative TTLs.
struct DirectiveAdmin {
    store: Arc<InMemoryDirectiveStore>,
    token: String,
    clock: Arc<dyn Clock>,
}

/// The concrete pipeline this binary serves.
pub type AppPipeline = Pipeline<ReferenceTenancy, OpenSearchSink>;

/// Adapts the engine pipeline to the transport's [`IngressHandler`] contract,
/// authenticating each request with the configured [`Authenticator`].
pub struct AppHandler<A> {
    pipeline: AppPipeline,
    authenticator: A,
    request_seq: AtomicU64,
    request_log: Box<dyn RequestLog>,
    directive_admin: Option<DirectiveAdmin>,
    metrics: Metrics,
}

impl<A> std::fmt::Debug for AppHandler<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected logger is not `Debug`; show whether it is enabled.
        f.debug_struct("AppHandler")
            .field("logging", &self.request_log.enabled())
            .finish_non_exhaustive()
    }
}

impl<A: Authenticator> AppHandler<A> {
    /// Wraps a pipeline and an authenticator (no request logging by default).
    #[must_use]
    pub fn new(pipeline: AppPipeline, authenticator: A) -> Self {
        Self {
            pipeline,
            authenticator,
            request_seq: AtomicU64::new(0),
            request_log: Box::new(NoLog),
            directive_admin: None,
            metrics: Metrics::new(),
        }
    }

    /// The pipeline this handler serves — a read-only accessor for introspection
    /// (e.g. the perf harness reading upstream `pool_stats` after a load run).
    #[must_use]
    pub fn pipeline(&self) -> &AppPipeline {
        &self.pipeline
    }

    /// Builds the shape-only `/metrics` snapshot JSON: request tallies plus every
    /// configured cluster's upstream pool-reuse counters. No tenant data, so it is
    /// always safe to expose.
    fn metrics_snapshot(&self) -> String {
        let pools = self
            .pipeline
            .sink()
            .pool_stats_all()
            .into_iter()
            .map(|(id, s)| PoolSnapshot {
                cluster: id.as_str().to_owned(),
                opened: s.opened,
                dispatched: s.dispatched,
                reused: s.reused(),
            })
            .collect();
        self.metrics.snapshot(pools).to_json()
    }

    /// Sets the structured per-request logger (builder style). Default: no logs.
    #[must_use]
    pub fn with_request_log(mut self, request_log: Box<dyn RequestLog>) -> Self {
        self.request_log = request_log;
        self
    }

    /// Enables the `POST /admin/directives` channel (builder style): publishes a
    /// fleet directive set into `store` when the request carries the bearer
    /// `token`. Without this, the endpoint reports `not_enabled`.
    #[must_use]
    pub fn with_directive_admin(
        mut self,
        store: Arc<InMemoryDirectiveStore>,
        token: String,
        clock: Arc<dyn Clock>,
    ) -> Self {
        self.directive_admin = Some(DirectiveAdmin {
            store,
            token,
            clock,
        });
        self
    }

    /// A per-request correlation id. A monotonic counter is enough for the
    /// reference binary; a real deployment would carry a propagated trace id.
    fn next_request_id(&self) -> RequestId {
        let n = self.request_seq.fetch_add(1, Ordering::Relaxed) + 1;
        RequestId::from(format!("req-{n}").as_str())
    }

    /// Handles `POST /admin/directives`: publishes a fleet directive set into the
    /// shared store when enabled and the bearer token matches. Fail-closed at
    /// every step — disabled, wrong method, bad token, or malformed body all leave
    /// the active set unchanged.
    fn publish_directives(&self, req: &IngressRequest) -> IngressResponse {
        let Some(admin) = &self.directive_admin else {
            return IngressResponse::json(404, br#"{"error":"not_enabled"}"#.to_vec());
        };
        if req.method != HttpMethod::Post {
            return IngressResponse::json(405, br#"{"error":"method_not_allowed"}"#.to_vec());
        }
        if !crate::bearer::matches(&req.headers, &admin.token) {
            return IngressResponse::json(401, br#"{"error":"unauthorized"}"#.to_vec());
        }
        match decode_directive_set(&req.body, admin.clock.as_ref()) {
            Ok(set) => {
                let count = set.len();
                admin.store.publish(set);
                IngressResponse::json(200, format!(r#"{{"published":{count}}}"#).into_bytes())
            }
            Err(reason) => {
                IngressResponse::json(400, format!(r#"{{"error":"{reason}"}}"#).into_bytes())
            }
        }
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

        // Break-glass read: the forensic tape captured while a `ring_buffer`
        // directive was in effect (`docs/05` §5), oldest first. Shape-only like
        // every explain document; same auth-gating note as `/debug/explain`.
        if req.path == "/debug/breakglass" {
            let tape = serde_json::Value::Array(self.pipeline.break_glass().snapshot());
            return IngressResponse::json(200, tape.to_string().into_bytes());
        }

        // Always-on operational snapshot — the one introspection surface meant to
        // stay enabled in production, where `/debug/*` is off. Shape-only (counts,
        // rates, cluster ids), so it needs no auth and leaks nothing. Per instance:
        // an external aggregator rolls the fleet up.
        if req.path == "/metrics" {
            return IngressResponse::json(200, self.metrics_snapshot().into_bytes());
        }

        // Privileged: publish a fleet directive set. Token-gated and fail-closed —
        // a forged token or a malformed body changes nothing (`docs/05` §3).
        if req.path == "/admin/directives" {
            return self.publish_directives(&req);
        }

        // Authenticate before any routing. The bearer token is consumed here and
        // never reaches the pipeline or telemetry.
        let principal = match self
            .authenticator
            .authenticate(&credentials_from(&req))
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
        )
        .with_doc_id(req.doc_id.as_deref());

        // Echo the request id so a client (or LLM) can fetch its
        // /debug/explain/{id} afterward.
        let (response, ok) = match self.pipeline.handle(&ctx).await {
            Ok(resp) => {
                let ok = (200..300).contains(&resp.status);
                (IngressResponse::json(resp.status, resp.body), ok)
            }
            Err(err) => (
                IngressResponse::json(status_for(&err), error_body(&err)),
                false,
            ),
        };
        // Tally the data-plane outcome for the /metrics snapshot (shape-only).
        self.metrics.record(ok);

        // Structured request log (opt-in): emit the shape-only explain document,
        // which carries the request's trace_id, so logs join the trace/spans.
        if self.request_log.enabled() {
            if let Some(record) = self.pipeline.explain(&request_id) {
                self.request_log.emit(&record);
            }
        }

        response.with_header("x-request-id", request_id.as_str())
    }
}

/// Extracts client credentials from a request: a bearer token from
/// `Authorization` and the verified mTLS client-certificate identity, if any.
fn credentials_from(req: &IngressRequest) -> ClientCredentials {
    ClientCredentials {
        bearer_token: crate::bearer::parse(&req.headers).map(str::to_owned),
        client_cert_subject: req.client_cert_subject.clone(),
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
