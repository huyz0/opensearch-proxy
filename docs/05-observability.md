# 05: Observability (LLM-debuggable, security-aware)

## 1. Goal restated

A failure must be diagnosable **by an LLM, from telemetry alone, without reading
source or asking a human to gather context** (NFR-T1). Observability is
**read-only**, the AI observes; it never mutates routing or cluster state.

Two constraints pull against each other and are both hard requirements:

- **Richness**: enough causal detail to explain *why* a request went where it did.
- **Security/cost**: never capture tenant values or secrets (NFR-S2); cheap when
  off; expensive detail only when explicitly, temporarily, narrowly enabled.

## 2. The span schema (per request)

One trace per request. Spans (all attributes are **shapes, ids, field names,
sizes, counts, never values**):

| Span | Attributes |
|------|-----------|
| `ingress` | protocol (h1/h2/grpc), tls.version, tls.suite, tls.session_reused, client.pool_id |
| `auth` | identity.source, principal.id, authz.decision, authz.policy_id (**no token**) |
| `classify` | endpoint.kind, index.logical |
| `spi.resolve` | partition.id, placement.kind, target.cluster, target.index, epoch, inject.field_names, docid.rule_id |
| `rewrite` | transform.kind, query.rewrite.kind, body.bytes, demux.target_count (bulk), strip.field_names |
| `dispatch` | target.cluster, pool.reuse (hit/miss), tls.handshake_reused, retries, upstream.latency_ms, upstream.status |
| `egress` | status, strip.field_count, response.bytes |

Errors attach `ErrorContext` (code, decision_chain, retryable, remediation) to
the failing span.

## 3. Diagnostics directive: runtime control without restart

Verbosity is **data**, not a code path, distributed to every instance:

```rust
pub struct DiagnosticsDirective {
    pub id: DirectiveId,
    pub match_: DirectiveMatch,   // tenant_id? index? principal? endpoint? header_marker?
    pub level: DiagLevel,         // Off | Shape | ShapeTiming | ShapeRewriteDiff
    pub sample_rate: f32,         // 0.0..=1.0
    pub ttl: Ttl,                 // auto-expire; a forgotten "on" cannot bleed cost
    pub ring_buffer: bool,        // single-instance local break-glass only
}
```

### Two delivery channels (both shipped)

1. **Signed request header** (`X-Debug-Directive`, HMAC-signed): surgical,
   single-request, follows the request to whatever instance handles it. Clients
   cannot self-enable (signature required, NFR-S3). Best for "explain this one
   call."
2. **Control-plane directive** in the watched store (`osproxy-control`):
   fleet-wide, "watch tenant X for 10 minutes," propagates in seconds, TTL
   auto-expires. Best for live targeted debugging across instances.

### Why targeted + TTL

Targeting (by tenant/index/principal/endpoint) is the **cost lever**, you pay
for detail only on the partition under investigation, not the fleet. TTL ensures
verbose mode can't be left on and silently burn money/latency, satisfying the
low-cost NFR.

## 4. In-process mechanism

- Built on `tracing` + `tracing-subscriber` with a `reload` layer; toggling
  never restarts the process.
- Spans are **created cheaply always**; the directive controls whether they are
  **recorded/exported**. "Off" cost is near-zero (NFR-T3, NFR-P).
- The directive evaluator is a small, hot, lock-light component in `osproxy-observe`.

## 5. Egress & introspection surfaces

All of these are **shipped** and per-instance by design; fleet rollup is the
external aggregator's job. They share the `trace_id` so an agent can correlate
across them.

**Upstream trace headers are gated on span export.** When OTLP export is on the
proxy is a span in the distributed trace: it injects its own `traceparent`
(child of the caller's, so the upstream span nests under the proxy's) and
forwards the caller's `tracestate` verbatim. It continues the caller's trace from
a W3C `traceparent`, or — when only **B3** (Zipkin/Istio, single `b3` or the
`X-B3-*` multi-header form) is present — from that, so a B3-native client's trace
stays connected (its `trace_id` is preserved) even though the proxy speaks W3C
downstream. When export is **off** (the default) the proxy adds no span, so it
injects nothing and stays transparent to tracing: the client's own trace headers
ride through in the forwarded header set (see [04 §11](04-request-pipeline.md))
untouched, B3 included. Either way a proxy that is not participating in tracing
never inserts a `traceparent` pointing at a span it never exported.

- **Structured JSON logs**, one shape-only line per request (the `/debug/explain`
  document, carrying `trace_id`). Off unless `OSPROXY_LOG_REQUESTS` is set
  (`RequestLog` seam: `NoLog` default / `StdoutJsonLog`).
- **OTLP export**, a shape-only `resource_spans` SERVER span per request via the
  `SpanExporter` seam. Off (near-zero cost) unless `OSPROXY_OTLP_ENDPOINT` is set;
  the `osproxy-otlp` crate POSTs to `{endpoint}/v1/traces`, fire-and-forget. The
  proxy span nests under the caller (`parentSpanId`) so the client→proxy→upstream
  tree reconstructs.
- **`GET /metrics`**, always-on shape-only counters (requests total/ok/error) and
  per-cluster pool-reuse snapshot, served **before auth**. This is the one
  introspection surface meant to stay on in production where `/debug/*` is off.
- **`/debug/explain` + `/debug/breakglass`**, see §6.
- **`GET`/`POST /admin/directives`**, token-gated, fail-closed. `POST` publishes a
  `DirectiveSet` to the fleet `DirectiveStore` (polled fresh per request → flips
  fleet-wide with no restart); `GET` introspects the active set as shape-only JSON
  that round-trips back to a publish. This is the store-agnostic control-plane
  seam: the proxy ships the seam + an in-memory reference impl, plus an **optional
  etcd-backed `DirectiveStore`** (`osproxy-etcd`, behind the `etcd` feature) that
  keeps a locally-cached snapshot fresh by an etcd watch, fleet-wide propagation
  with no shared in-process state and no restart (ADR-013). Under etcd the etcd key
  is the control plane, so the local `POST` publish path is disabled (operators
  publish to the key); the same fail-closed decoder validates both paths.
- **Break-glass ring buffer**, populated only when a matching `ring_buffer: true`
  directive is active: a short-lived in-memory ring of the last N request
  explanations on one instance, served at `/debug/breakglass`. Single-instance
  local debugging; explicitly marginal in a fleet and documented as such.
- **Diagnostic sink (fleet-coherent break-glass)**, the `DiagnosticSink` seam
  addresses that single-instance limit: when a `ring_buffer`/`capture` directive
  selects a request, the same shape-only explain doc is also handed to the sink,
  which pushes it **off the instance keyed by `trace_id`** so a fleet aggregator
  can serve it regardless of which instance served the request. Default
  `NoopDiagnosticSink` (off → local ring only); the reference `StdoutDiagnosticSink`
  emits a tagged JSON line (`"kind":"diagnostic_capture"`) the platform's log
  collector scrapes (`log_diagnostic_captures`). Distinct from the per-request log
  (all-or-none): only directive-selected captures are pushed.

## 6. `/debug/explain/{request_id}`

An endpoint that assembles the **full causal story** for one request id into a
single JSON document purpose-built for LLM consumption: the ordered decision
chain, each span's shape attributes, the final status, and, on failure, the
`ErrorContext` with remediation. This is the primary "no human gathers context"
affordance (NFR-T4).

Security: the endpoint returns only shape-level data; it cannot reveal tenant
values because they were never captured. It short-circuits before auth (like
`/metrics` and `/debug/breakglass`) and is gated by `OSPROXY_DEBUG_ENDPOINTS`
(default on; **set `false` in production** so operational metadata is not exposed
unauthenticated, `/metrics` stays on regardless). `/debug/breakglass` serves the
break-glass tape (§5) in the same shape-only form.

## 7. What is NEVER captured

- Document field values, query literal values, source bodies.
- Tokens, passwords, client/upstream credentials, TLS private material.
- Anything declared sensitive by `TenancySpi::sensitive_fields`.

This is enforced **by construction** (the trace API only accepts shape/id/name
types for value-bearing positions), not by after-the-fact redaction, so there
is no path by which a value reaches a log. Tested by a static check + a
runtime "no value leaks" test that fuzzes documents with canary secrets and
asserts they never appear in any emitted telemetry.
