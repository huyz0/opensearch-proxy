# 2. Requirements & Non-Functional Requirements

Every requirement here is testable, and the build gates on it. This page is the
summary; the source of truth is [`docs/00-goals.md`](../00-goals.md) and the NFR tables
in [`docs/01-architecture.md`](../01-architecture.md) §5.

## Functional scope (v1)

- **Ingress**: HTTP/1.1, HTTP/2, and gRPC; cleartext and TLS; optional FIPS build.
- **Single-target routing** for **all** request types (read and write).
- **Ingest demux**: one mixed-partition `_bulk` body split into per-placement
  writes, response `items[]` re-interleaved in original order.
- **Query rewrite** (mandatory partition filter) and **response field-stripping**
  for shared-index tenancy.
- **Doc-id construction** and **partition-field injection** on ingest.
- **Connection pooling**: downstream keep-alive + upstream per-cluster pools with
  TLS session reuse.
- **Auth**: client authentication (mTLS + token) and upstream credential
  management; optional post-auth authorization.
- **Scroll/PIT affinity** pinning (opt-in).
- **Epoch-gated partition migration**.
- **Pluggable write sink** (OpenSearch now; the `Sink` trait makes Kafka-based
  redundancy a later drop-in).
- **Runtime-togglable, security-aware, LLM-consumable observability.**

### Supported endpoint matrix

Each request is classified into one endpoint kind and dispatched accordingly:

| Kind | Examples | Tenancy-aware |
|------|----------|---------------|
| `IngestDoc` | `PUT/POST /idx/_doc/{id}` | yes (inject + construct id) |
| `IngestBulk` | `POST /_bulk`, `/idx/_bulk` | yes (per-doc demux) |
| `GetById` | `GET /idx/_doc/{id}` | yes (id mapping) |
| `MultiGet` | `GET/POST /_mget` | yes (per-doc) |
| `DeleteById` | `DELETE /idx/_doc/{id}` | yes |
| `Search` | `POST /idx/_search` | yes (filter + strip) |
| `MultiSearch` | `POST /_msearch` | yes (per-query) |
| `Count` | `POST /idx/_count` | yes (filter) |
| `Cursor` | `_search/scroll`, `_pit` | affinity-pinned |
| `Admin` | `_cat`, `_cluster`, `_nodes` | pass-through (opt-in, allow-listed) |

See [`docs/specs/opensearch-endpoints.md`](../specs/opensearch-endpoints.md) for the
full surface.

## Non-functional requirements

The NFRs are grouped and each has a stable id (e.g. `NFR-P1`) referenced throughout
the codebase and traces. Performance numbers marked `CALIBRATE` are validated on
release hardware, not hard-coded.

### Performance (NFR-P)

| Id | Requirement |
|----|-------------|
| NFR-P1 | Added p50 latency over direct-to-cluster ≤ ~1–2 ms for a pass-through request. |
| NFR-P2 | Added p99 latency under budget; no tail amplification from pooling. |
| NFR-P3 | Zero heap allocation on the pass-through hot path beyond unavoidable buffers; bulk rewrite streams without buffering the whole body. |
| NFR-P4 | Upstream TLS session reuse rate above threshold (e.g. ≥ 99%) under steady load. |
| NFR-P5 | Downstream keep-alive honored; no per-request connection churn. |
| NFR-P6 | Idle memory footprint bounded by config; no unbounded buffers/queues. |
| NFR-P7 | Bulk demux is O(body size), single-pass, no full-document re-serialization where avoidable. |

These are gated continuously: `dhat` allocation budgets and `iai-callgrind`
instruction counts in CI, plus the `osproxy-bench` macro load harness against a real
OpenSearch (see [Components](04-components.md)).

### Reliability (NFR-R)

| Id | Requirement |
|----|-------------|
| NFR-R1 | **No panics** reachable from the request path (enforced by `deny(clippy::unwrap_used, expect_used, panic)` on request-path crates). |
| NFR-R2 | Every fallible operation returns a typed error from the taxonomy; no string errors on the request path. |
| NFR-R3 | Backpressure: bounded queues everywhere; overload returns `429` with retry guidance, never OOM. |
| NFR-R4 | Upstream failures classified retryable/terminal and surfaced with the decision chain. |
| NFR-R5 | Graceful shutdown drains in-flight requests within a deadline; new requests rejected during drain. |
| NFR-R6 | No data corruption across partition migration (epoch gating). |
| NFR-R7 | Survives the fault-injection suite (slow/dropped upstreams, malformed bodies, partial writes) without panic or stuck request. |

### Traceability / observability (NFR-T)

| Id | Requirement |
|----|-------------|
| NFR-T1 | Every request emits one causal trace whose spans reconstruct *why* it routed where it did, with **no source reading required** (the "blind diagnosis" test). |
| NFR-T2 | Default verbosity emits **shapes, ids, and field names only**, never tenant values, bodies, tokens, or credentials. |
| NFR-T3 | Verbosity is runtime-togglable fleet-wide **without restart**, targeted by tenant/index/principal/endpoint, with TTL auto-expiry. |
| NFR-T4 | `GET /debug/explain/{request_id}` returns the full decision chain as one LLM-consumable JSON document. |
| NFR-T5 | Every error carries: code, decision chain, `retryable`, and a remediation hint. |

### Security (NFR-S)

| Id | Requirement |
|----|-------------|
| NFR-S1 | TLS termination required for any **mutated** request (you cannot rewrite an encrypted stream); no body-mutating cleartext passthrough. |
| NFR-S2 | No secret/credential/token/tenant value in any log or trace at any verbosity. Diagnostic capture is shape-only by construction. |
| NFR-S3 | Header-delivered debug directives are HMAC-signed; clients cannot self-enable expensive tracing. |
| NFR-S4 | Partition isolation enforced on the read path; a client-supplied query cannot bypass the partition filter. |
| NFR-S5 | FIPS build negotiates only FIPS-approved TLS versions and cipher suites. |

### Maintainability / quality (NFR-Q)

| Id | Requirement |
|----|-------------|
| NFR-Q1 | No "god" file/module/type; size and cohesion budgets enforced in CI. |
| NFR-Q2 | ≥ 90% semantic test coverage overall; SPI + routing core held higher. |
| NFR-Q3 | Every public SPI item has doc comments with intent, invariants, and an example. |
| NFR-Q4 | Public SPI changes require a design-review note. |

## Release acceptance

A release is acceptable only when **all** of: functional matrix routes correctly and
round-trips symmetrically; performance budgets met; no request-path panics; blind
diagnosis passes; ≥ 90% coverage; no value leaks at default verbosity; and (for FIPS
builds) linkage against a CMVP-validated module on a tested platform.

→ [Architecture](03-architecture.md)
