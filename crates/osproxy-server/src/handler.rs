//! The ingress handler: authenticates the caller, builds a request context, and
//! drives the engine pipeline, mapping the outcome to an HTTP response.
//
// JUSTIFY(file-length): the single ingress-orchestration point — pre-auth
// introspection routing, the TLS and auth/authz gates, and data-plane dispatch
// are one cohesive flow over the handler's private state; splitting it would
// force those fields pub(crate) and scatter the request lifecycle across files.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use osproxy_core::{Clock, ErrorCode, RequestId};
use osproxy_engine::{Pipeline, RequestError};
use osproxy_observe::{DirectiveStore, InMemoryDirectiveStore, Metrics, PoolSnapshot};
use osproxy_sink::OpenSearchSink;
use osproxy_spi::{
    Action, AuthError, Authenticator, Authorizer, ClientCredentials, HeaderView, HttpMethod,
    RequestCtx,
};
use osproxy_tenancy::TenancyRouter;
use osproxy_transport::{IngressHandler, IngressRequest, IngressResponse};

use crate::auth::AllowAllAuthorizer;
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
pub type AppPipeline = Pipeline<TenancyRouter<ReferenceTenancy>, OpenSearchSink>;

/// Adapts the engine pipeline to the transport's [`IngressHandler`] contract,
/// authenticating each request with the configured [`Authenticator`] and, after
/// authentication, authorizing it with the configured [`Authorizer`] (default
/// [`AllowAllAuthorizer`] — no second policy layer until one is supplied).
pub struct AppHandler<A, Z = AllowAllAuthorizer> {
    pipeline: AppPipeline,
    authenticator: A,
    authorizer: Z,
    request_seq: AtomicU64,
    request_log: Box<dyn RequestLog>,
    directive_admin: Option<DirectiveAdmin>,
    metrics: Metrics,
    /// When true (default), a body-mutating request over cleartext is refused
    /// (NFR-S1) — the proxy must terminate TLS to rewrite the stream. An operator
    /// on a trusted network can opt out.
    require_tls_for_mutation: bool,
    /// When true (default), the pre-auth `/debug/explain` and `/debug/breakglass`
    /// surfaces are served. They are shape-only, but still expose operational
    /// metadata to anyone who can reach the port, so production deployments turn
    /// them off; disabled, both report `not_enabled` (`/metrics` stays on).
    debug_endpoints: bool,
}

impl<A, Z> std::fmt::Debug for AppHandler<A, Z> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected logger is not `Debug`; show whether it is enabled.
        f.debug_struct("AppHandler")
            .field("logging", &self.request_log.enabled())
            .finish_non_exhaustive()
    }
}

impl<A: Authenticator> AppHandler<A, AllowAllAuthorizer> {
    /// Wraps a pipeline and an authenticator (no request logging by default, and
    /// the allow-all authorizer until [`Self::with_authorizer`] supplies one).
    #[must_use]
    pub fn new(pipeline: AppPipeline, authenticator: A) -> Self {
        Self {
            pipeline,
            authenticator,
            authorizer: AllowAllAuthorizer,
            request_seq: AtomicU64::new(0),
            request_log: Box::new(NoLog),
            directive_admin: None,
            metrics: Metrics::new(),
            require_tls_for_mutation: true,
            debug_endpoints: true,
        }
    }
}

impl<A: Authenticator, Z: Authorizer> AppHandler<A, Z> {
    /// Sets the post-authentication [`Authorizer`] (builder style). Replaces the
    /// default allow-all policy; the principal is already resolved, so the
    /// authorizer decides only whether that principal may perform the action.
    #[must_use]
    pub fn with_authorizer<Z2: Authorizer>(self, authorizer: Z2) -> AppHandler<A, Z2> {
        AppHandler {
            pipeline: self.pipeline,
            authenticator: self.authenticator,
            authorizer,
            request_seq: self.request_seq,
            request_log: self.request_log,
            directive_admin: self.directive_admin,
            metrics: self.metrics,
            require_tls_for_mutation: self.require_tls_for_mutation,
            debug_endpoints: self.debug_endpoints,
        }
    }

    /// Sets whether the pre-auth `/debug/explain` and `/debug/breakglass`
    /// surfaces are served (builder style). Default `true`; set `false` in
    /// production so operational metadata is not exposed unauthenticated.
    #[must_use]
    pub fn with_debug_endpoints(mut self, enabled: bool) -> Self {
        self.debug_endpoints = enabled;
        self
    }

    /// Sets whether body-mutating requests are refused over cleartext (NFR-S1).
    /// Builder style; default `true` (enforce). Pass `false` only on a trusted
    /// network where the operator accepts mutating over cleartext.
    #[must_use]
    pub fn with_require_tls_for_mutation(mut self, require: bool) -> Self {
        self.require_tls_for_mutation = require;
        self
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
        // Publishing a fleet directive set is a privileged mutation carrying a
        // bearer token; refuse it over cleartext (same NFR-S1 stance as the data
        // plane) so the token is never exposed on the wire. The introspection
        // routes short-circuit before the data-plane TLS gate, so enforce it here.
        if self.require_tls_for_mutation && !req.secure {
            return IngressResponse::json(403, br#"{"error":"tls_required"}"#.to_vec());
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

    /// The pre-auth introspection and control-plane surfaces, in one place: the
    /// shape-only `/debug/*` tools, the always-on `/metrics` snapshot, and the
    /// token-gated `/admin/directives` (GET reads, POST publishes). Returns `Some`
    /// when `req` targets one of them, else `None` (the request is data plane).
    fn introspection_route(&self, req: &IngressRequest) -> Option<IngressResponse> {
        // /debug/*: the shape-only diagnostics surfaces, served only when enabled
        // (off in production so operational metadata is not exposed unauthenticated).
        // Disabled, they report `not_enabled` rather than 404, to distinguish "turned
        // off here" from "no such route".
        if req.path.starts_with("/debug/") {
            if !self.debug_endpoints {
                return Some(IngressResponse::json(
                    404,
                    br#"{"error":"not_enabled"}"#.to_vec(),
                ));
            }
            // /debug/explain/{id}: the shape-only causal trace for one request.
            if let Some(id) = req.path.strip_prefix("/debug/explain/") {
                return Some(match self.pipeline.explain(&RequestId::from(id)) {
                    Some(doc) => IngressResponse::json(200, doc.to_string().into_bytes()),
                    None => {
                        IngressResponse::json(404, br#"{"error":"unknown_request_id"}"#.to_vec())
                    }
                });
            }
            // /debug/breakglass: the forensic tape captured under a ring_buffer
            // directive (`docs/05` §5), oldest first. Shape-only like the explain doc.
            if req.path == "/debug/breakglass" {
                let tape = serde_json::Value::Array(self.pipeline.break_glass().snapshot());
                return Some(IngressResponse::json(200, tape.to_string().into_bytes()));
            }
        }
        // /metrics: the always-on, prod-safe operational snapshot (shape-only
        // counts/rates/cluster ids, so no auth; see `metrics_snapshot`).
        if req.path == "/metrics" {
            return Some(IngressResponse::json(
                200,
                self.metrics_snapshot().into_bytes(),
            ));
        }
        // /admin/directives: privileged control-plane settings — GET introspects
        // what this instance applies, POST publishes a new set; both token-gated
        // and fail-closed (a forged token reveals/changes nothing, `docs/05` §3).
        if req.path == "/admin/directives" {
            return Some(match req.method {
                HttpMethod::Get => self.introspect_directives(req),
                _ => self.publish_directives(req),
            });
        }
        None
    }

    /// Handles `GET /admin/directives`: returns the control-plane settings this
    /// instance is currently applying — the read side of the directive store, so
    /// an agent can see what is in effect (per instance; the replicating store
    /// keeps the fleet consistent). Token-gated like the publish path (the
    /// targeting selectors are operator config) and fail-closed: disabled or a bad
    /// token reveals nothing.
    fn introspect_directives(&self, req: &IngressRequest) -> IngressResponse {
        let Some(admin) = &self.directive_admin else {
            return IngressResponse::json(404, br#"{"error":"not_enabled"}"#.to_vec());
        };
        if !crate::bearer::matches(&req.headers, &admin.token) {
            return IngressResponse::json(401, br#"{"error":"unauthorized"}"#.to_vec());
        }
        let view = admin.store.load().introspect(admin.clock.now());
        IngressResponse::json(200, view.to_string().into_bytes())
    }
}

impl<A: Authenticator, Z: Authorizer> IngressHandler for AppHandler<A, Z> {
    async fn handle(&self, req: IngressRequest) -> IngressResponse {
        let request_id = self.next_request_id();

        // Introspection + admin surfaces short-circuit before auth; the data plane
        // continues below.
        if let Some(resp) = self.introspection_route(&req) {
            return resp;
        }

        // NFR-S1: the proxy mutates the body (inject/filter/strip) for every
        // tenancy-aware endpoint; mutating an encrypted stream requires
        // terminating TLS, so a mutating request over cleartext is refused before
        // it is processed. Read-only admin/unknown pass-through is unaffected.
        if self.require_tls_for_mutation && req.endpoint.is_tenancy_aware() && !req.secure {
            return IngressResponse::json(403, br#"{"error":"tls_required"}"#.to_vec())
                .with_header("x-request-id", request_id.as_str());
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

        // Authorize the resolved principal against the action. The default
        // allow-all authorizer makes this a no-op; a supplied policy can decline
        // (403) without ever seeing the credentials, which were consumed above.
        let action = Action {
            endpoint: req.endpoint,
            logical_index: req.logical_index.clone(),
        };
        if let Err(err) = self.authorizer.authorize(&principal, &action).await {
            return IngressResponse::json(err.http_status(), auth_error_body(&err))
                .with_header("x-request-id", request_id.as_str());
        }

        // The credentials were consumed above; strip the `Authorization` header so
        // the bearer token never reaches the pipeline, observability, or logs. The
        // partition header, `traceparent`, and `x-debug-directive` are preserved —
        // the engine still needs them.
        let safe_headers = crate::bearer::without_authorization(&req.headers);
        let ctx = RequestCtx::new(
            &principal,
            &request_id,
            req.method,
            req.endpoint,
            req.protocol,
            &req.logical_index,
            HeaderView::new(&safe_headers),
            &req.body,
        )
        .with_doc_id(req.doc_id.as_deref())
        .with_query(req.query.as_deref())
        .with_path(&req.path);

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
