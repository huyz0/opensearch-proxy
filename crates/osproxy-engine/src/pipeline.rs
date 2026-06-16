//! The request pipeline: orchestrates a classified request through routing,
//! transform, and delivery, returning a response for the transport to write.
//!
//! M1 implements the single-document ingest path (`docs/04` §1): resolve the
//! routing decision, build the epoch-stamped write batch, dispatch it to the
//! sink, and shape the acknowledgement into an OpenSearch-style response. M2
//! adds the get-by-id read path (`docs/04` §5): resolve, map the logical id to
//! the physical id, fetch, and shape the stored document back into the client's
//! logical view. Search and bulk attach here in later milestones.

use std::sync::Arc;

use osproxy_core::{Clock, CursorSigner, EndpointKind, RequestId, SystemClock};
use osproxy_observe::{
    explain_json, resource_spans, BreakGlassBuffer, ClassifyInfo, DiagLevel, DirectiveSet,
    DirectiveStore, DirectiveVerifier, EgressInfo, ExplainStore, NoVerifier, NoopExporter,
    RequestAttrs, RequestTrace, SpanExporter,
};
use osproxy_sink::{Reader, Sink};
use osproxy_spi::{RequestCtx, SpiError, TenancySpi};
use osproxy_tenancy::TenancyRouter;
use serde_json::Value;

use crate::error::RequestError;
use crate::observe::{error_context, logical_index};

/// How many recent request explanations `/debug/explain` retains per instance.
const EXPLAIN_CAPACITY: usize = 1024;

/// How many explanations the break-glass tape holds once a `ring_buffer`
/// directive turns it on. Bounded so an "on" directive cannot grow memory.
const BREAK_GLASS_CAPACITY: usize = 256;

/// The response the pipeline produces for a handled request.
///
/// A status plus a JSON body, mirroring the relevant fields of an OpenSearch
/// response so the transport can relay it to the client unchanged.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PipelineResponse {
    /// The HTTP status to return to the client.
    pub status: u16,
    /// The JSON response body.
    pub body: Vec<u8>,
}

/// Orchestrates requests through a tenancy router and a sink.
///
/// Generic over the [`TenancySpi`] implementation and the [`Sink`], so the hot
/// path is monomorphized (no dyn dispatch) and tests can swap in an in-memory
/// sink.
pub struct Pipeline<T, S> {
    pub(crate) router: TenancyRouter<T>,
    pub(crate) sink: S,
    pub(crate) retry: crate::RetryPolicy,
    explain: Arc<ExplainStore>,
    exporter: Arc<dyn SpanExporter>,
    clock: Arc<dyn Clock>,
    service_name: String,
    /// The verbosity applied when no directive raises it. Default [`DiagLevel::Shape`]
    /// so a configured exporter exports every request; lower it to `Off` to make
    /// export purely directive-driven (targeted sampling).
    baseline: DiagLevel,
    /// The fleet-wide directive source, polled fresh per request so an operator
    /// can flip verbosity without a restart. Defaults to an empty static set.
    directive_store: Arc<dyn DirectiveStore>,
    verifier: Arc<dyn DirectiveVerifier>,
    /// The break-glass tape, captured into only when a `ring_buffer` directive
    /// applies to a request. Empty (near-zero cost) until then.
    break_glass: Arc<BreakGlassBuffer>,
    /// Signs/verifies scroll & PIT affinity envelopes (`docs/03` §6). `None` =
    /// affinity **off** (the opt-in default): cursor requests fail closed with a
    /// `CursorUnresolvable` error rather than route blindly.
    pub(crate) cursor_signer: Option<Arc<dyn CursorSigner>>,
    /// The admin pass-through policy (`docs/03` §6): which cluster answers
    /// allow-listed `_cat`/`_cluster`/`_nodes` requests, and which path prefixes
    /// are permitted. `None` = reject all admin requests (the default).
    pub(crate) admin_policy: Option<crate::admin::AdminPolicy>,
}

/// The diagnostics decision for one request: how much to record/export, and
/// whether to capture it into the break-glass tape.
#[derive(Clone, Copy)]
struct Diagnostics {
    level: DiagLevel,
    capture: bool,
}

impl<T, S> std::fmt::Debug for Pipeline<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected exporter/clock are not `Debug`; show the rest of the shape.
        f.debug_struct("Pipeline")
            .field("retry", &self.retry)
            .field("service_name", &self.service_name)
            .field("exporting", &self.exporter.enabled())
            .finish_non_exhaustive()
    }
}

impl<T: TenancySpi, S: Sink + Reader> Pipeline<T, S> {
    /// Builds a pipeline from a router and a sink (default backend-retry policy,
    /// no span export).
    pub fn new(router: TenancyRouter<T>, sink: S) -> Self {
        Self {
            router,
            sink,
            retry: crate::RetryPolicy::default(),
            explain: Arc::new(ExplainStore::new(EXPLAIN_CAPACITY)),
            exporter: Arc::new(NoopExporter),
            clock: Arc::new(SystemClock),
            service_name: "osproxy".to_owned(),
            baseline: DiagLevel::Shape,
            // An empty static set as the default store: `Arc<DirectiveSet>` is
            // itself a `DirectiveStore`, so this is `Arc<dyn DirectiveStore>` over
            // a constant snapshot. Swap it for a fleet store via builder.
            directive_store: Arc::new(Arc::new(DirectiveSet::new())),
            verifier: Arc::new(NoVerifier),
            break_glass: Arc::new(BreakGlassBuffer::new(BREAK_GLASS_CAPACITY)),
            cursor_signer: None,
            admin_policy: None,
        }
    }

    /// Enables opt-in admin pass-through (`docs/03` §6): allow-listed
    /// `_cat`/`_cluster`/`_nodes` requests are forwarded verbatim to `policy`'s
    /// cluster. Without this, every admin request is rejected (the default).
    #[must_use]
    pub fn with_admin_passthrough(mut self, policy: crate::admin::AdminPolicy) -> Self {
        self.admin_policy = Some(policy);
        self
    }

    /// Enables opt-in scroll/PIT cursor affinity (`docs/03` §6) with `signer`
    /// signing the cluster↔cursor envelope. Without this, cursor requests fail
    /// closed (`CursorUnresolvable`) rather than route to an unknown cluster.
    #[must_use]
    pub fn with_cursor_signer(mut self, signer: Arc<dyn CursorSigner>) -> Self {
        self.cursor_signer = Some(signer);
        self
    }

    /// Sets the placement-backend retry policy (builder style).
    #[must_use]
    pub fn with_retry_policy(mut self, retry: crate::RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Sets the OTLP span exporter (builder style). Default is no export.
    #[must_use]
    pub fn with_exporter(mut self, exporter: Arc<dyn SpanExporter>) -> Self {
        self.exporter = exporter;
        self
    }

    /// Swaps the clock used to stamp span timestamps (tests inject a `ManualClock`).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the `service.name` reported on exported spans (builder style).
    #[must_use]
    pub fn with_service_name(mut self, service_name: impl Into<String>) -> Self {
        self.service_name = service_name.into();
        self
    }

    /// Sets the baseline diagnostics level applied to every request before
    /// directives (builder style). Default [`DiagLevel::Shape`]; set to
    /// [`DiagLevel::Off`] to export only what a directive selects.
    #[must_use]
    pub fn with_baseline_level(mut self, baseline: DiagLevel) -> Self {
        self.baseline = baseline;
        self
    }

    /// Sets a fixed set of active diagnostics directives (builder style). For a
    /// fleet-wide, restart-free source use [`Pipeline::with_directive_store`].
    #[must_use]
    pub fn with_directives(mut self, directives: Arc<DirectiveSet>) -> Self {
        // `Arc<DirectiveSet>` is itself a `DirectiveStore` (a constant snapshot).
        self.directive_store = Arc::new(directives);
        self
    }

    /// Sets the fleet-wide directive store (builder style). The pipeline polls it
    /// fresh per request, so a controller publishing a new set flips verbosity
    /// across the fleet without a restart (`docs/05` §3).
    #[must_use]
    pub fn with_directive_store(mut self, store: Arc<dyn DirectiveStore>) -> Self {
        self.directive_store = store;
        self
    }

    /// Sets the verifier for the signed `X-Debug-Directive` header (builder
    /// style). Default rejects all headers; a real verifier enables the surgical,
    /// single-request directive channel.
    #[must_use]
    pub fn with_directive_verifier(mut self, verifier: Arc<dyn DirectiveVerifier>) -> Self {
        self.verifier = verifier;
        self
    }

    /// Shares the break-glass tape (builder style), so a debug endpoint can read
    /// the captured sequence and tests can inspect it.
    #[must_use]
    pub fn with_break_glass(mut self, break_glass: Arc<BreakGlassBuffer>) -> Self {
        self.break_glass = break_glass;
        self
    }

    /// The assembled `/debug/explain` document for a past request, if retained.
    #[must_use]
    pub fn explain(&self, request_id: &RequestId) -> Option<Value> {
        self.explain.get(request_id)
    }

    /// The break-glass tape — the explanations captured while a `ring_buffer`
    /// directive was in effect (`docs/05` §5).
    #[must_use]
    pub fn break_glass(&self) -> &Arc<BreakGlassBuffer> {
        &self.break_glass
    }

    /// The underlying sink (e.g. to inspect what an in-memory sink recorded).
    #[must_use]
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Handles an authenticated request, dispatching on its endpoint class.
    ///
    /// Records a shape-only causal trace for every request (success or failure)
    /// into the explain store, so `/debug/explain/{id}` can reconstruct it
    /// (`docs/05`).
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`] if the endpoint is unsupported in M1, routing
    /// fails, the body transform fails, or the sink rejects the write.
    pub async fn handle(&self, ctx: &RequestCtx<'_>) -> Result<PipelineResponse, RequestError> {
        // Only pay for span timing/encoding when an exporter is actually active —
        // "Off" stays near-zero cost (`docs/05`).
        let exporting = self.exporter.enabled();
        let start_nanos = if exporting {
            self.clock.unix_nanos()
        } else {
            0
        };

        let mut trace = RequestTrace::new();
        // The same W3C context propagated to downstream calls is recorded here, so
        // `/debug/explain` and the exported OTLP span share the request's ids.
        trace.record_context(crate::endpoints::wire_trace(ctx));
        trace.record_classify(ClassifyInfo {
            endpoint: ctx.endpoint(),
            logical_index: logical_index(ctx.logical_index()),
        });

        let result = self.dispatch(ctx, &mut trace).await;
        match &result {
            Ok(resp) => trace.record_egress(EgressInfo {
                status: resp.status,
                response_bytes: resp.body.len(),
            }),
            Err(err) => trace.record_error(error_context(err)),
        }
        self.explain.record(ctx.request_id().clone(), &trace);

        let diag = self.diagnostics(ctx, &trace);

        // Break-glass: capture the explanation into the bounded tape when a
        // `ring_buffer` directive selected this request (`docs/05` §5). Off by
        // default, so this stays empty until an operator flips it on.
        if diag.capture {
            self.break_glass
                .capture(explain_json(ctx.request_id(), &trace));
        }

        // Export the span when an exporter is active AND the diagnostics level for
        // this request reaches at least `Shape` — so directives can restrict export
        // to a targeted, sampled subset (`docs/05` §3). Background, best-effort.
        if exporting && diag.level >= DiagLevel::Shape {
            let end_nanos = self.clock.unix_nanos();
            if let Some(payload) = resource_spans(
                &self.service_name,
                ctx.request_id(),
                &trace,
                start_nanos,
                end_nanos,
            ) {
                self.exporter.export(payload);
            }
        }
        result
    }

    /// The diagnostics decision for a finished request: the baseline level raised
    /// by any directive that targets it (by tenant/index/principal/endpoint),
    /// plus whether any applying directive wants break-glass capture. Evaluated at
    /// the current time so expiry/sampling apply; the directive store is polled
    /// fresh and the signed header verified exactly once.
    fn diagnostics(&self, ctx: &RequestCtx<'_>, trace: &RequestTrace) -> Diagnostics {
        let attrs = RequestAttrs {
            tenant: trace.resolved_partition(),
            index: ctx.logical_index(),
            principal: ctx.principal_id(),
            endpoint: ctx.endpoint(),
        };
        let now = self.clock.now();
        let request = ctx.request_id();
        // Poll the fleet directive store fresh (a cheap Arc clone of the current
        // snapshot) so a published flip takes effect without a restart.
        let snapshot = self.directive_store.load();
        let mut level = self.baseline.max(snapshot.evaluate(&attrs, now, request));
        let mut capture = snapshot.wants_ring_buffer(&attrs, now, request);
        // Fold in a verified single-request directive from the signed
        // `X-Debug-Directive` header, if present and valid (`docs/05` §3).
        if let Some(directive) = ctx
            .headers()
            .get("x-debug-directive")
            .and_then(|h| self.verifier.verify(h))
        {
            if let Some(from_header) = directive.level_if_applies(&attrs, now, request) {
                level = level.max(from_header);
                capture |= directive.ring_buffer;
            }
        }
        Diagnostics { level, capture }
    }

    /// Dispatches on endpoint class, recording the per-stage spans into `trace`.
    async fn dispatch(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        match ctx.endpoint() {
            EndpointKind::IngestDoc => self.ingest_doc(ctx, trace).await,
            EndpointKind::IngestBulk => self.ingest_bulk(ctx, trace).await,
            EndpointKind::GetById => self.get_by_id(ctx, trace).await,
            EndpointKind::MultiGet => self.multi_get(ctx, trace).await,
            EndpointKind::DeleteById => self.delete_by_id(ctx, trace).await,
            EndpointKind::Search => self.search(ctx, trace).await,
            EndpointKind::MultiSearch => self.multi_search(ctx, trace).await,
            EndpointKind::Count => self.count(ctx, trace).await,
            EndpointKind::Cursor => self.cursor(ctx, trace).await,
            EndpointKind::Admin => self.admin(ctx, trace).await,
            other => Err(RequestError::Spi(SpiError::UnsupportedEndpoint {
                endpoint: other,
            })),
        }
    }
}

#[cfg(test)]
#[path = "pipeline_tests.rs"]
mod tests;
