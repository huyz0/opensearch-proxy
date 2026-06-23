//! The request pipeline: orchestrates a classified request through routing,
//! transform, and delivery, returning a response for the transport to write.
//!
//! M1 implements the single-document ingest path (`docs/04` §1): resolve the
//! routing decision, build the epoch-stamped write batch, dispatch it to the
//! sink, and shape the acknowledgement into an OpenSearch-style response. M2
//! adds the get-by-id read path (`docs/04` §5): resolve, map the logical id to
//! the physical id, fetch, and shape the stored document back into the client's
//! logical view. Search and bulk attach here in later milestones.
//
// JUSTIFY(file-length): this is the central request orchestrator, the lifecycle
// (classify → route → transform → dispatch → trace → diagnostics decision) is one
// cohesive flow, and the per-request directive evaluation it owns is the seam
// every observability/capture feature attaches to. Tests already live in
// `pipeline_tests.rs`; splitting the flow itself would scatter the lifecycle.

use std::sync::Arc;

use osproxy_core::{Clock, CursorSigner, EndpointKind, RequestId, SystemClock};
use osproxy_observe::{
    explain_json, resource_spans, BreakGlassBuffer, ClassifyInfo, DiagLevel, DiagnosticSink,
    DirectiveSet, DirectiveStore, DirectiveVerifier, EgressInfo, ExplainStore, NoVerifier,
    NoopDiagnosticSink, NoopExporter, RequestAttrs, RequestTrace, SpanExporter,
};
use osproxy_sink::{ByteBody, Reader, Sink, StreamingForward};
use osproxy_spi::{RequestCtx, SpiError};
use osproxy_tenancy::Router;
use serde_json::Value;

use crate::error::RequestError;
use crate::observe::{error_context, logical_index};
use crate::search_stream::StreamSearch;

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
    /// The response content type. `None` ⇒ `application/json`: every response the
    /// proxy *shapes* is JSON, so that is the default. It is set only on the
    /// verbatim admin/cursor passthrough, where the upstream may answer with a
    /// non-JSON type (e.g. `_cat` returns `text/plain`); forcing `application/json`
    /// there would mislabel the body (`docs/03` §6).
    pub content_type: Option<String>,
}

impl PipelineResponse {
    /// A JSON response, the shape every tenancy-aware endpoint returns.
    #[must_use]
    pub fn json(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            body,
            content_type: None,
        }
    }

    /// Carries the upstream content type verbatim (the admin/cursor passthrough),
    /// so a non-JSON upstream body is not mislabeled `application/json`.
    #[must_use]
    pub fn with_content_type(mut self, content_type: Option<String>) -> Self {
        self.content_type = content_type;
        self
    }
}

/// Orchestrates requests through a tenancy router and a sink.
///
/// Generic over the [`Router`] implementation and the [`Sink`], so the hot path
/// is monomorphized (no dyn dispatch), a deployment can supply its own router,
/// and tests can swap in an in-memory sink.
pub struct Pipeline<R, S> {
    pub(crate) router: R,
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
    /// Whether traffic capture is on for every request before any directive.
    /// Default `false`: capture is off until a published `capture` directive
    /// selects requests (capture on demand). Set `true` for an always-capture
    /// deployment (e.g. a dedicated capture/migration proxy).
    baseline_capture: bool,
    /// The fleet-wide directive source, polled fresh per request so an operator
    /// can flip verbosity without a restart. Defaults to an empty static set.
    directive_store: Arc<dyn DirectiveStore>,
    verifier: Arc<dyn DirectiveVerifier>,
    /// The break-glass tape, captured into only when a `ring_buffer` directive
    /// applies to a request. Empty (near-zero cost) until then.
    break_glass: Arc<BreakGlassBuffer>,
    /// The fleet-coherent diagnostic sink: a directive-selected capture is pushed
    /// here (keyed by `trace_id`) as well as into the local break-glass ring, so an
    /// aggregator can serve it fleet-wide. Default [`NoopDiagnosticSink`] (off): the
    /// capture stays in the local ring only.
    diagnostic_sink: Arc<dyn DiagnosticSink>,
    /// Signs/verifies scroll & PIT affinity envelopes (`docs/03` §6). `None` =
    /// affinity **off** (the opt-in default): cursor requests fail closed with a
    /// `CursorUnresolvable` error rather than route blindly.
    pub(crate) cursor_signer: Option<Arc<dyn CursorSigner>>,
    /// The admin pass-through policy (`docs/03` §6): which cluster answers
    /// allow-listed `_cat`/`_cluster`/`_nodes` requests, and which path prefixes
    /// are permitted. `None` = reject all admin requests (the default).
    pub(crate) admin_policy: Option<crate::admin::AdminPolicy>,
    /// Tenant-agnostic passthrough (`None` = pure tenancy mode, the default).
    /// When set, requests the policy matches (by logical index) are forwarded
    /// verbatim with no rewrite; unmatched requests stay tenant-isolated. A
    /// prefix-free policy passes everything through (transparent/capture proxy).
    pub(crate) passthrough: Option<crate::passthrough::PassthroughPolicy>,
    /// The write mode applied when a request does not select one with the
    /// `X-Write-Mode` header. Default [`crate::WriteMode::Sync`], async fan-out is
    /// opt-in (`docs/04` §9).
    pub(crate) baseline_write_mode: crate::asyncwrite::WriteMode,
    /// The durable queue async writes are enqueued onto. Default
    /// [`crate::asyncwrite::NoQueue`]: async requests are refused (`422`) until a
    /// real queue is wired in.
    pub(crate) write_queue: Arc<dyn crate::asyncwrite::WriteQueue>,
    /// Whether `_delete_by_query` may be expanded into per-match deletes in async
    /// mode (`docs/04` §9). Default `false`: DBQ is rejected until opted in, since
    /// it reads the match set and enqueues a delete each.
    pub(crate) delete_by_query_expansion: bool,
}

/// The diagnostics decision for one request: how much to record/export, whether
/// to capture it into the break-glass tape, and whether to tee it to the fleet
/// traffic-capture sink.
#[derive(Clone, Copy)]
struct Diagnostics {
    level: DiagLevel,
    capture: bool,
    traffic_capture: bool,
}

impl<R, S> std::fmt::Debug for Pipeline<R, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected exporter/clock are not `Debug`; show the rest of the shape.
        f.debug_struct("Pipeline")
            .field("retry", &self.retry)
            .field("service_name", &self.service_name)
            .field("exporting", &self.exporter.enabled())
            .finish_non_exhaustive()
    }
}

impl<R: Router, S: Sink + Reader> Pipeline<R, S> {
    /// Builds a pipeline from a router and a sink (default backend-retry policy,
    /// no span export).
    pub fn new(router: R, sink: S) -> Self {
        Self {
            router,
            sink,
            retry: crate::RetryPolicy::default(),
            explain: Arc::new(ExplainStore::new(EXPLAIN_CAPACITY)),
            exporter: Arc::new(NoopExporter),
            clock: Arc::new(SystemClock),
            service_name: "osproxy".to_owned(),
            baseline: DiagLevel::Shape,
            baseline_capture: false,
            // An empty static set as the default store: `Arc<DirectiveSet>` is
            // itself a `DirectiveStore`, so this is `Arc<dyn DirectiveStore>` over
            // a constant snapshot. Swap it for a fleet store via builder.
            directive_store: Arc::new(Arc::new(DirectiveSet::new())),
            verifier: Arc::new(NoVerifier),
            break_glass: Arc::new(BreakGlassBuffer::new(BREAK_GLASS_CAPACITY)),
            diagnostic_sink: Arc::new(NoopDiagnosticSink),
            cursor_signer: None,
            admin_policy: None,
            passthrough: None,
            baseline_write_mode: crate::asyncwrite::WriteMode::Sync,
            write_queue: Arc::new(crate::asyncwrite::NoQueue),
            delete_by_query_expansion: false,
        }
    }

    /// Enables the `_delete_by_query` async expansion (builder style). Without it,
    /// DBQ is rejected even in async mode (`docs/04` §9).
    #[must_use]
    pub fn with_delete_by_query_expansion(mut self, on: bool) -> Self {
        self.delete_by_query_expansion = on;
        self
    }

    /// Sets the baseline write mode applied when a request does not carry an
    /// `X-Write-Mode` header (builder style). Default [`crate::WriteMode::Sync`]; set
    /// [`crate::WriteMode::Async`] to make durable fan-out the deployment default
    /// (`docs/04` §9).
    #[must_use]
    pub fn with_baseline_write_mode(mut self, mode: crate::asyncwrite::WriteMode) -> Self {
        self.baseline_write_mode = mode;
        self
    }

    /// Sets the durable queue async writes are enqueued onto (builder style).
    /// Without it, async requests are refused with `422` rather than dropped.
    #[must_use]
    pub fn with_write_queue(mut self, queue: Arc<dyn crate::asyncwrite::WriteQueue>) -> Self {
        self.write_queue = queue;
        self
    }

    /// The write mode for `ctx`: the validated `X-Write-Mode` header if present,
    /// else the deployment baseline. An unparseable header falls back to the
    /// baseline rather than erroring, an unknown mode is not a hard failure.
    pub(crate) fn write_mode(&self, ctx: &RequestCtx<'_>) -> crate::asyncwrite::WriteMode {
        self.resolve_write_mode(ctx.headers().get("x-write-mode"))
    }

    /// The write mode from a raw `X-Write-Mode` header value (or its absence),
    /// the precedence shared by [`write_mode`](Self::write_mode) and
    /// [`is_sync_write`](Self::is_sync_write): a valid header wins, else the baseline.
    fn resolve_write_mode(&self, header: Option<&str>) -> crate::asyncwrite::WriteMode {
        header
            .and_then(crate::asyncwrite::WriteMode::parse)
            .unwrap_or(self.baseline_write_mode)
    }

    /// Enables tenant-agnostic passthrough: every request is forwarded verbatim to
    /// `policy`'s cluster with no tenancy rewrite. Use this for a transparent or
    /// capture/migration proxy. Without it, the pipeline routes by tenancy (the
    /// default).
    #[must_use]
    pub fn with_passthrough(mut self, policy: crate::passthrough::PassthroughPolicy) -> Self {
        self.passthrough = Some(policy);
        self
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

    /// Sets whether traffic capture is on for every request before directives
    /// (builder style). Default `false` (capture on demand via a published
    /// directive); set `true` for an always-capture deployment.
    #[must_use]
    pub fn with_baseline_capture(mut self, on: bool) -> Self {
        self.baseline_capture = on;
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

    /// Sets the fleet-coherent diagnostic sink (builder style): a directive-selected
    /// capture is pushed here (keyed by `trace_id`) in addition to the local
    /// break-glass ring, so an aggregator can serve it fleet-wide (`docs/05` §5).
    #[must_use]
    pub fn with_diagnostic_sink(mut self, sink: Arc<dyn DiagnosticSink>) -> Self {
        self.diagnostic_sink = sink;
        self
    }

    /// The assembled `/debug/explain` document for a past request, if retained.
    #[must_use]
    pub fn explain(&self, request_id: &RequestId) -> Option<Value> {
        self.explain.get(request_id)
    }

    /// The break-glass tape, the explanations captured while a `ring_buffer`
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

    /// The trace context to inject onto an upstream request, or `None` when the
    /// proxy is not adding a span of its own (span export off). With export off the
    /// proxy stays transparent to tracing: the client's own trace headers (W3C, B3,
    /// anything) ride through verbatim in the forwarded header set, and the proxy
    /// inserts no `traceparent` of its own (`docs/05`). With export on it injects
    /// its hop's `traceparent`, overriding the client's so the upstream span nests
    /// under the proxy's.
    pub(crate) fn upstream_trace(
        &self,
        ctx: &RequestCtx<'_>,
    ) -> Option<osproxy_core::TraceContext> {
        self.exporter
            .enabled()
            .then(|| crate::endpoints::wire_trace(ctx))
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
        self.handle_with_capture(ctx).await.0
    }

    /// Like [`Self::handle`], but also returns whether this request should be teed
    /// to the fleet traffic-capture sink, the live per-request capture decision,
    /// applied by the ingress to both success and error responses.
    pub async fn handle_with_capture(
        &self,
        ctx: &RequestCtx<'_>,
    ) -> (Result<PipelineResponse, RequestError>, bool) {
        // Only pay for span timing/encoding when an exporter is actually active,
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

        // Break-glass: capture the explanation when a `ring_buffer`/`capture`
        // directive selected this request (`docs/05` §5). Off by default, so this
        // stays empty until an operator flips it on. The doc is built once and both
        // retained in the local ring and pushed to the fleet diagnostic sink
        // (keyed by `trace_id`) so it is reachable on any instance.
        if diag.capture {
            let doc = explain_json(ctx.request_id(), &trace);
            if self.diagnostic_sink.enabled() {
                self.diagnostic_sink.emit(doc.clone());
            }
            self.break_glass.capture(doc);
        }

        // Export the span when an exporter is active AND the diagnostics level for
        // this request reaches at least `Shape`, so directives can restrict export
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
        (result, diag.traffic_capture)
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
        // Traffic capture is on when the deployment baseline says always-on or any
        // published `capture` directive selects this request (capture on demand).
        let mut traffic_capture =
            self.baseline_capture || snapshot.wants_capture(&attrs, now, request);
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
                traffic_capture |= directive.capture;
            }
        }
        Diagnostics {
            level,
            capture,
            traffic_capture,
        }
    }

    /// Whether the effective write mode for a request with these headers is sync,
    /// the `X-Write-Mode` header if present and valid, else the deployment
    /// baseline. Lets the transport decide to stream-demux a `_bulk` (sync only;
    /// async fan-out keeps the buffered path) from the head alone (ADR-014 stage 4).
    #[must_use]
    pub fn is_sync_write(&self, headers: &[(String, String)]) -> bool {
        let header = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("x-write-mode"))
            .map(|(_, v)| v.as_str());
        self.resolve_write_mode(header) == crate::asyncwrite::WriteMode::Sync
    }

    /// Whether a request for `logical_index` is a tenant-agnostic passthrough that
    /// can be **streamed** verbatim (ADR-014 stage 2). Body-free so the transport
    /// can decide before buffering. `false` when no passthrough policy is set or
    /// the index is not matched (the request then takes the buffered tenancy path).
    #[must_use]
    pub fn is_passthrough(&self, logical_index: &str) -> bool {
        self.passthrough
            .as_ref()
            .is_some_and(|p| p.matches_index(logical_index))
    }

    /// Handles a verbatim passthrough request whose body is supplied as a
    /// **stream** (ADR-014 stage 2): forward it to the passthrough cluster without
    /// buffering. Mirrors [`handle_with_capture`](Self::handle_with_capture)'s
    /// trace lifecycle (classify → dispatch → egress, recorded into the explain
    /// store), minus the buffered-body diagnostics: traffic capture is never
    /// available here because the body is not retained, so the returned flag is
    /// always `false`.
    pub async fn forward_streamed(
        &self,
        ctx: &RequestCtx<'_>,
        body: ByteBody,
    ) -> (Result<StreamingForward, RequestError>, bool) {
        let mut trace = Self::begin_streamed_trace(ctx);
        let result = match self.passthrough.as_ref() {
            Some(policy) => self.forward_stream(ctx, policy, body, &mut trace).await,
            // Only reachable if a caller streams a request `is_passthrough` rejects.
            None => Err(RequestError::Spi(SpiError::UnsupportedEndpoint {
                endpoint: ctx.endpoint(),
            })),
        };
        // The response body is a live stream of unknown length, so egress records
        // the status with zero bytes (the size is not known until it has flowed).
        match &result {
            Ok(f) => trace.record_egress(EgressInfo {
                status: f.status,
                response_bytes: 0,
            }),
            Err(err) => trace.record_error(error_context(err)),
        }
        self.explain.record(ctx.request_id().clone(), &trace);
        (result, false)
    }

    /// Handles a `_search` whose response is streamed back through the hit
    /// transform (ADR-014, final stage): the upstream body is never buffered, each
    /// hit is shaped incrementally and every sibling (notably `aggregations`) is
    /// forwarded verbatim. Same trace lifecycle as
    /// [`forward_streamed`](Self::forward_streamed): the body length is unknown
    /// until it flows, so egress records the status with zero bytes. The request
    /// query body is small and already buffered in `ctx`; only the response
    /// streams. Returns the result plus `false`, capture is never available on a
    /// streamed path (and the caller only streams when capture is off).
    pub async fn search_streamed(
        &self,
        ctx: &RequestCtx<'_>,
    ) -> (Result<StreamSearch, RequestError>, bool) {
        let mut trace = Self::begin_streamed_trace(ctx);
        let result = self.run_search_stream(ctx, &mut trace).await;
        match &result {
            Ok(s) => trace.record_egress(EgressInfo {
                status: s.status,
                response_bytes: 0,
            }),
            Err(err) => trace.record_error(error_context(err)),
        }
        self.explain.record(ctx.request_id().clone(), &trace);
        (result, false)
    }

    /// Handles a `_bulk` request whose body is supplied as a **stream** (ADR-014
    /// stage 4): frame and demux the NDJSON incrementally so the whole batch is
    /// never buffered. Same trace lifecycle as [`forward_streamed`](Self::forward_streamed)
    /// (classify → egress, into the explain store); per-op outcomes live
    /// positionally in the response body, as in the buffered bulk path. Sync write
    /// mode only, the streaming decision is made by the caller; async fan-out
    /// keeps the buffered path.
    pub async fn handle_bulk_streamed(
        &self,
        ctx: &RequestCtx<'_>,
        body: ByteBody,
    ) -> (Result<PipelineResponse, RequestError>, bool) {
        // Bulk records its outcome positionally in the response, not per-stage, so
        // the trace passes straight from open to close with no mid-stage spans.
        let trace = Self::begin_streamed_trace(ctx);
        let result =
            crate::bulk::ingest_bulk_streamed(&self.router, &self.sink, ctx, body, self.retry)
                .await;
        self.finish_streamed_trace(ctx, trace, result)
    }

    /// Opens the shape-only trace for a streamed request: context + classify, the
    /// stages known before dispatch. Shared by the streamed forward and bulk paths.
    fn begin_streamed_trace(ctx: &RequestCtx<'_>) -> RequestTrace {
        let mut trace = RequestTrace::new();
        trace.record_context(crate::endpoints::wire_trace(ctx));
        trace.record_classify(ClassifyInfo {
            endpoint: ctx.endpoint(),
            logical_index: logical_index(ctx.logical_index()),
        });
        trace
    }

    /// Closes a streamed request's trace (egress or error) and records it into the
    /// explain store. Returns the result plus `false`, traffic capture is never
    /// available on a streamed path (the body is not retained to tee).
    fn finish_streamed_trace(
        &self,
        ctx: &RequestCtx<'_>,
        mut trace: RequestTrace,
        result: Result<PipelineResponse, RequestError>,
    ) -> (Result<PipelineResponse, RequestError>, bool) {
        match &result {
            Ok(resp) => trace.record_egress(EgressInfo {
                status: resp.status,
                response_bytes: resp.body.len(),
            }),
            Err(err) => trace.record_error(error_context(err)),
        }
        self.explain.record(ctx.request_id().clone(), &trace);
        (result, false)
    }

    /// Dispatches on endpoint class, recording the per-stage spans into `trace`.
    async fn dispatch(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        // Tenant-agnostic passthrough short-circuits tenancy dispatch for the
        // requests it matches (by logical index); unmatched requests fall through
        // to tenancy below (fail-closed).
        if let Some(policy) = self.passthrough.as_ref().filter(|p| p.matches(ctx)) {
            return self.forward(ctx, policy, trace).await;
        }
        match ctx.endpoint() {
            EndpointKind::IngestDoc => self.ingest_doc(ctx, trace).await,
            EndpointKind::IngestBulk => self.ingest_bulk(ctx, trace).await,
            EndpointKind::GetById => self.get_by_id(ctx, trace).await,
            EndpointKind::MultiGet => self.multi_get(ctx, trace).await,
            EndpointKind::DeleteById => self.delete_by_id(ctx, trace).await,
            EndpointKind::DeleteByQuery => self.delete_by_query(ctx, trace).await,
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
