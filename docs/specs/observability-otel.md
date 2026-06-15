# OpenTelemetry Conventions

> Status: skeleton — target OTel semantic-conventions version: `[PIN]`.

How the proxy exports traces/logs so any OTel-aware tooling (and an LLM with an
OTel query tool) can consume them. The schema is defined in docs/05; this file
pins the wire conventions.

## 1. Export

- Traces via OTLP (gRPC/HTTP). Logs as structured JSON correlated by trace id.
- One trace per request; spans per docs/05 §2.

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
