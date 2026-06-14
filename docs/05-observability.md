# 05 — Observability (LLM-debuggable, security-aware)

## 1. Goal restated

A failure must be diagnosable **by an LLM, from telemetry alone, without reading
source or asking a human to gather context** (NFR-T1). Observability is
**read-only** — the AI observes; it never mutates routing or cluster state.

Two constraints pull against each other and are both hard requirements:

- **Richness**: enough causal detail to explain *why* a request went where it did.
- **Security/cost**: never capture tenant values or secrets (NFR-S2); cheap when
  off; expensive detail only when explicitly, temporarily, narrowly enabled.

## 2. The span schema (per request)

One trace per request. Spans (all attributes are **shapes, ids, field names,
sizes, counts — never values**):

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

## 3. Diagnostics directive — runtime control without restart

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
   cannot self-enable (signature required — NFR-S3). Best for "explain this one
   call."
2. **Control-plane directive** in the watched store (`osproxy-control`):
   fleet-wide, "watch tenant X for 10 minutes," propagates in seconds, TTL
   auto-expires. Best for live targeted debugging across instances.

### Why targeted + TTL

Targeting (by tenant/index/principal/endpoint) is the **cost lever** — you pay
for detail only on the partition under investigation, not the fleet. TTL ensures
verbose mode can't be left on and silently burn money/latency, satisfying the
low-cost NFR.

## 4. In-process mechanism

- Built on `tracing` + `tracing-subscriber` with a `reload` layer; toggling
  never restarts the process.
- Spans are **created cheaply always**; the directive controls whether they are
  **recorded/exported**. "Off" cost is near-zero (NFR-T3, NFR-P).
- The directive evaluator is a small, hot, lock-light component in `osproxy-observe`.

## 5. Egress & aggregation

- Default: structured JSON logs and/or OTLP traces (OpenTelemetry) tagged with a
  shared `request_id`/trace id, shipped to the user's aggregator. This is the
  fleet-scale story.
- **Ring buffer**: populated only when `ring_buffer: true` — a short-lived
  in-memory ring of the last N request explanations on a single instance. Useful
  only for single-instance local debugging; explicitly marginal in a fleet and
  documented as such.

## 6. `/debug/explain/{request_id}`

An endpoint that assembles the **full causal story** for one request id into a
single JSON document purpose-built for LLM consumption: the ordered decision
chain, each span's shape attributes, the final status, and — on failure — the
`ErrorContext` with remediation. This is the primary "no human gathers context"
affordance (NFR-T4).

Security: the endpoint is itself authenticated/authorized and returns only
shape-level data; it cannot reveal tenant values because they were never
captured.

## 7. What is NEVER captured

- Document field values, query literal values, source bodies.
- Tokens, passwords, client/upstream credentials, TLS private material.
- Anything declared sensitive by `TenancySpi::sensitive_fields`.

This is enforced **by construction** (the trace API only accepts shape/id/name
types for value-bearing positions), not by after-the-fact redaction — so there
is no path by which a value reaches a log. Tested by a static check + a
runtime "no value leaks" test that fuzzes documents with canary secrets and
asserts they never appear in any emitted telemetry.
