# 00 — Project Goals, Scope, and Success Criteria

## 1. One-sentence goal

Build a high-performance, low-resource, low-latency OpenSearch routing proxy,
consumable as a Rust library, that routes each request to the correct physical
placement based on a pluggable partition-based placement policy — with
first-class observability designed for LLM-driven debugging and a FIPS-capable
crypto build.

## 2. Primary use cases

1. **Logical index / tenancy** — clients address *logical* indices; the proxy
   resolves the *physical* cluster + index from a partition key, injecting
   partition fields and constructing document ids on write and reversing both on
   read.
2. **Interception** — profiling, telemetry, and auth applied uniformly to all
   OpenSearch traffic at the proxy boundary.
3. **Operational agility** — partitions can be migrated between placements with
   the proxy guaranteeing write correctness across the cutover.

## 3. In scope (v1)

- Ingress: HTTP/1.1, HTTP/2, gRPC; cleartext and TLS; optional FIPS build.
- Single-target routing for **all** request types (read and write).
- Ingest demux: one mixed-partition `_bulk` body split into per-placement
  writes, response `items[]` re-interleaved in original order.
- Query rewrite (partition filter) and response field-stripping for shared-index
  tenancy.
- Doc-id construction and partition-field injection on ingest.
- Connection pooling (downstream keep-alive + upstream per-cluster pools, TLS
  session reuse).
- Auth: client authentication (mTLS + token) and upstream credential management.
- Scroll/PIT affinity pinning (opt-in).
- Epoch-gated partition migration support.
- Pluggable write **sink** (OpenSearch now; the trait makes Kafka redundancy a
  later drop-in).
- Runtime-togglable, security-aware, LLM-consumable observability.

## 4. Explicit non-goals (v1)

| Non-goal | Why / where it goes |
|----------|---------------------|
| Synchronous fan-out / scatter-gather **search** | Search is always single-cluster. A partition lives in one place. |
| Cross-cluster result merge, agg merge, cross-cluster scoring | Eliminated by single-target search. |
| Synchronous dual/triple-write redundancy | Excluded; redundancy is instead **async** — the fan-out write mode (ADR-010, docs/04 §9) durably enqueues a write to Kafka behind the `WriteQueue` seam and a downstream component applies it to 1..N destinations. Honest `202`/`op_id`, never a synchronous fan-out. |
| Copying partition data during migration | External reindex/snapshot tooling does the copy; the proxy only gates the routing flip. |
| Dynamic plugin loading (WASM/dylib) | SPI is compiled in statically. |
| The proxy mutating cluster state via AI | Observability is **read-only**; the AI observes, humans/automation act. |

## 5. Success criteria

A release is acceptable when **all** of these hold:

- **Functional**: every request type in the supported matrix routes to the
  correct single placement; ingest inject/construct and read filter/strip are
  provably symmetric (round-trip property tests pass).
- **Performance** (see [01-architecture.md](01-architecture.md) §NFR for exact
  targets): added p99 latency and per-request allocation under budget; pool
  reuse rates above threshold under steady load.
- **Reliability**: no panics on the request path; every failure is a typed,
  contextual error; chaos/fault-injection suite passes.
- **Traceability**: every request emits a causal trace sufficient to diagnose a
  failure **without reading source code**; verified by the "blind diagnosis"
  test (see [09-testing-and-quality.md](09-testing-and-quality.md)).
- **Coverage**: ≥90% semantic coverage overall; SPI and routing core held to a
  higher bar (see [09](09-testing-and-quality.md)).
- **Security**: no secret or tenant value ever appears in a trace/log at default
  verbosity; FIPS build negotiates only approved suites.
- **Compliance**: FIPS build linked against a CMVP-validated module on a tested
  platform configuration.

## 6. Audience for this design

This design is written to be executable by an LLM with minimal human
intervention. Every doc states *why*, not just *what*, so that decisions can be
re-derived rather than guessed at. See [10-review-process.md](10-review-process.md)
for how changes are reviewed.
