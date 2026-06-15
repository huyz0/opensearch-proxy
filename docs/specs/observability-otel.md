# OpenTelemetry Conventions

> Status: skeleton — target OTel semantic-conventions version: `[PIN]`.

How the proxy exports traces/logs so any OTel-aware tooling (and an LLM with an
OTel query tool) can consume them. The schema is defined in docs/05; this file
pins the wire conventions.

## 1. Export

- Traces via OTLP/HTTP (JSON binding): one `SERVER` span per request, POSTed to
  the collector's `/v1/traces`. The span id is the proxy hop's W3C span id, so
  upstream spans nest under it; attributes are the shape-only stage data.
- **Off by default, near-zero cost when off.** The exporter is wired only when
  `OSPROXY_OTLP_ENDPOINT` (collector base URL) is set; `OSPROXY_SERVICE_NAME`
  sets `service.name`. With no exporter the pipeline skips encoding entirely
  (`SpanExporter::enabled() == false`).
- **Directive-gated.** Export happens only when the request's effective
  `DiagLevel` (pipeline baseline raised by any matching directive, docs/05 §3)
  reaches `Shape`. The baseline defaults to `Shape`, so a configured exporter
  exports every request; lowering the baseline to `Off` makes export purely
  directive-driven — targeted, sampled, TTL-bounded.
- **Never on the request's critical path.** `osproxy-otlp`'s `OtlpHttpExporter`
  hands off to a background task and ignores the result — a slow or down
  collector adds no latency and cannot fail a request (ADR-005, read-only obs).
- The encoder is `osproxy_observe::resource_spans` (pure, I/O-free); the seam is
  `osproxy_observe::SpanExporter` (`NoopExporter` default).

### Structured logs (correlated by trace id)

One structured JSON log line per request — the shape-only `/debug/explain`
document, which carries the request's `trace_id` — so logs join the traces/spans
in any aggregator. Off by default (`OSPROXY_LOG_REQUESTS` enables stdout JSON);
the seam is `osproxy_server::log::RequestLog` (`NoLog` default, `StdoutJsonLog`
impl). Shape-only by construction, so a log line can never carry a tenant value.

## 1a. Context propagation (W3C Trace Context)

The proxy is a span in a larger distributed trace, so it **propagates** standard
trace headers rather than starting an island trace:

- **Inbound**: a client's `traceparent` (W3C Trace Context) is parsed at the
  engine. If present and well-formed, the request continues that trace (same
  `trace_id`); if absent or malformed, the proxy mints a sampled root.
- **This hop**: a fresh `span_id` is generated for the proxy's own span, derived
  from the request id (deterministic, dependency-free — no RNG in `core`).
- **Outbound**: every upstream call (write, read, query, and each demuxed
  `_bulk`/`_mget`/`_msearch` sub-request) carries a `traceparent` whose
  `trace_id` matches the inbound trace and whose parent is the proxy's span, so
  OpenSearch's spans nest under the proxy.

The primitive is `osproxy_core::TraceContext` (`TraceContext::propagate`); it is
injected once at the sink's single send choke point. It holds **only** trace/span
identity — never request values — so propagation cannot become a value-leak
channel (the shape-only rule, docs/05 §7). `tracestate` pass-through and emitting
the `trace_id` into `/debug/explain` for log↔trace correlation are follow-ups.

## 2. Attribute naming

Use stable, namespaced keys. Custom keys under the `osproxy.*` namespace; reuse
OTel standard keys (`http.*`, `tls.*`, `server.*`, `network.*`) where they exist.

| Concept | Key | Value type |
|---------|-----|-----------|
| partition | `osproxy.partition.id` | id (never the value behind it) |
| placement kind | `osproxy.placement.kind` | enum string |
| target cluster | `osproxy.target.cluster` | id |
| target index | `osproxy.target.index` | name |
| epoch | `osproxy.epoch` | int |
| injected fields | `osproxy.inject.field_names` | string[] (names only) |
| stripped fields | `osproxy.strip.field_names` | string[] (names only) |
| demux targets | `osproxy.bulk.target_count` | int |
| error code | `osproxy.error.code` | stable string |
| retryable | `osproxy.error.retryable` | bool |
| pool reuse | `osproxy.pool.reuse` | bool |
| tls reuse | `tls.session_reused` | bool |

**No attribute ever carries a tenant value, document field value, query literal,
token, or credential** (docs/05 §7). This is enforced by the trace API types.

## 3. Sampling & directives

Directive-driven recording (docs/05 §3) maps to OTel sampling decisions at the
export layer; "Off" produces no exported spans (near-zero cost) while spans are
still created cheaply in-process.

## 4. `/debug/explain`

Not OTel — a synchronous JSON assembly of a single request's decision chain for
direct LLM consumption (docs/05 §6). It reads from the same span data.
