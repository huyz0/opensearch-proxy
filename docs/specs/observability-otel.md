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

### 3a. Signed `X-Debug-Directive` header channel

The surgical, single-request channel: an operator mints a token off-band with a
shared HMAC key and attaches it to one request; `HmacDirectiveVerifier`
(osproxy-server) authenticates it and raises that request's effective level. A
client cannot forge a token, so it cannot self-enable verbose diagnostics
(NFR-S3); enable it with `OSPROXY_DEBUG_DIRECTIVE_KEY` and pair it with
`OSPROXY_DIAG_BASELINE=off` so diagnostics stay dark until a signed token (or a
fleet directive) lights one request. `OSPROXY_DIAG_BASELINE` accepts
`off`/`shape`/`shape-timing`/`shape-rewrite-diff` (default `shape`: a configured
exporter ships every request).

Wire form `{payload_hex}.{sig_hex}`, `sig = HMAC-SHA256(key, payload_bytes)`,
verified by constant-time `hmac::verify`. Payload JSON: `level` (a `DiagLevel`
name) and `exp` (absolute unix-seconds expiry) are required; `tenant`/`index`/
`principal` narrow the target, `sample_per_mille` (default 1000) and
`ring_buffer` (default false) are optional. The HMAC runs on the build's
**validated** crypto module (ring under `non-fips`, aws-lc-rs under `fips`,
cfg-selected exactly like the TLS cert fingerprint), so a FIPS artifact never
authenticates with a non-validated primitive. The verifier fails closed: an
unknown level, an out-of-range sampling rate, a past expiry, or any signature
mismatch authorizes nothing.

### 3b. Fleet-wide directive store

The fleet counterpart to the surgical header channel: a controller publishes a
`DirectiveSet` into a `DirectiveStore` and every proxy instance reads it, so an
operator can raise verbosity across the fleet (a tenant, an endpoint, a sampled
slice) without a restart. The pipeline polls the store **fresh per request**
(`Pipeline::with_directive_store`) — a cheap `Arc`-clone of the current snapshot
— so a published flip takes effect on the next request fleet-wide.

Like the migration control plane, the proxy ships the **seam plus an in-process
reference** (`InMemoryDirectiveStore`: `publish` writes, `load` reads), not a
distributed store; a real etcd/Consul/OpenSearch-index backend implements the
same `DirectiveStore` trait unchanged, keeping a watched local snapshot so
`load` stays I/O-free on the hot path. TTL safety is intrinsic: directives carry
an absolute expiry, so even a published set that is never replaced self-expires
at evaluation — a forgotten fleet "on" turns itself off.

The reference binary exposes a **`POST /admin/directives`** channel (enabled by
`OSPROXY_DIRECTIVE_ADMIN_TOKEN`, presented as `Authorization: Bearer`) that
publishes a set into the shared store, so the fleet flips with no restart. Body:
`{"directives":[{"id","level","ttl_secs",<optional "tenant"/"index"/"principal",
"sample_per_mille","ring_buffer">}]}`. The decoder is **fail-closed**: a bad
token (401), wrong method (405), or any malformed/unknown/out-of-range field
(400) leaves the active set unchanged — a misspelled targeting key is rejected
rather than silently widening a directive to the whole fleet. `ttl_secs` is
relative and resolved to an absolute expiry on publish.

### 3c. Break-glass ring buffer

When a directive sets `ring_buffer: true`, every request it selects is captured —
in order — into a bounded in-memory tape (`BreakGlassBuffer`), independent of
OTLP export and of the diagnostics level. This is the forensic affordance for the
case where a *class* of request is failing and the ids aren't known up front:
flip a `ring_buffer` directive (fleet store or signed header) and read back the
last N matching explanations as a sequence.

Single-instance by design (the tape lives on the instance that handled the
requests) and bounded (capacity-evicted), so it costs nothing until a directive
turns it on and cannot grow without limit once on. Each entry is the same
shape-only explain document, so the tape carries no tenant value, body, or
credential. The binary serves it at **`GET /debug/breakglass`** (a JSON array,
oldest first) — the operator read, the same shape-only, would-be-auth-gated
surface as `/debug/explain`; the pipeline exposes it via `break_glass()`.

## 4. `/debug/explain`

Not OTel — a synchronous JSON assembly of a single request's decision chain for
direct LLM consumption (docs/05 §6). It reads from the same span data.
