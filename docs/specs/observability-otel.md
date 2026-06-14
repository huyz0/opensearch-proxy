# OpenTelemetry Conventions

> Status: skeleton — target OTel semantic-conventions version: `[PIN]`.

How the proxy exports traces/logs so any OTel-aware tooling (and an LLM with an
OTel query tool) can consume them. The schema is defined in docs/05; this file
pins the wire conventions.

## 1. Export

- Traces via OTLP (gRPC/HTTP). Logs as structured JSON correlated by trace id.
- One trace per request; spans per docs/05 §2.

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
