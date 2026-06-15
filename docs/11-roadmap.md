# 11 — Roadmap & Milestones

Built as thin vertical slices: each milestone is shippable, tested to the docs/09
bar, and exercises real architectural seams rather than building horizontal
layers that can't be validated until the end.

## M0 — Workspace & guardrails (foundation)

- Workspace + empty crates per docs/01 §2; strict dependency graph wired and
  enforced.
- CI skeleton: build (fips + non-fips), rustfmt, clippy `-D warnings`,
  cargo-deny, doc build, coverage, size/complexity budgets, `xtask ci`.
- `osproxy-core` error taxonomy skeleton + newtype ids.
- ADR backfill in `docs/decisions/`.
- **Exit**: empty pipeline compiles; CI gates active; a trivial PR must pass all
  gates.

## M1 — First vertical slice: single-doc ingest

The spine. Smallest path that touches every seam **except** bulk demux and query
rewrite.

- `RoutingSpi` + `TenancySpi` traits (docs/02) defined and documented with examples.
- Partition extraction (BodyField + Principal) + `placement_for` against an
  in-memory epoch-versioned placement table (docs/03).
- Inject fields + construct `_id` + set `_routing` for `SharedIndex`.
- `Sink` trait + `OpenSearchSink` (single write), epoch-stamped.
- `transport` ingress for HTTP/1.1 + TLS (aws-lc-rs, non-fips ok for now) +
  upstream pool (one cluster).
- Auth: mTLS termination + token authenticate/authorize (minimal).
- Observability: span schema (docs/05) + `/debug/explain` + directive evaluation
  (header channel); no-value-leak test.
- **Exit**: a `PUT /{logical}/_doc` round-trips to a real (testcontainer)
  OpenSearch in the right index with injected fields + constructed id; round-trip
  symmetry property test (write side) passes; blind-diagnosis passes for
  partition-unresolved + placement-missing + upstream-timeout.

## M2 — Read path & symmetry

- Query rewrite (partition filter wrapping) + response field strip (docs/04 §4).
- Get/delete/update by id with logical→physical id mapping.
- Full **round-trip symmetry** property test (write+read) green — the headline
  correctness property.
- Isolation property + adversarial bypass tests (docs/09 §2.7).
- **Exit**: `_search`, `_count`, `GET/_doc/{id}` symmetric and isolated; endpoint
  matrix subset proven.

## M3 — Bulk demux

- Streaming NDJSON parse, per-doc resolve (with per-request placement cache),
  demux by target, concurrent dispatch, re-interleave `items[]` (docs/04 §3).
- Partial-failure + bounded-memory + backpressure handling.
- Bulk order-preservation & id-collision-freedom property tests.
- `_mget`/`_msearch` demux.
- **Exit**: mixed-partition bulk routes correctly; memory bounded on large bulk;
  partial failures positioned correctly.

## M4 — Protocols & pooling completeness

- HTTP/2 + gRPC ingress; per-request upstream protocol selection.
- Sharded per-cluster upstream pools; TLS session reuse; downstream keep-alive;
  health-checked eviction.
- Performance harness + baselines for NFR-P; alloc profiling.
- **Exit**: NFR-P targets calibrated and met; pool reuse rates verified.

## M5 — Migration & affinity

- Epoch-gated migration state machine + control-plane state transitions (docs/06).
- Migration simulation tests (INV-M1..M4).
- Scroll/PIT affinity pinning (docs/03 §6).
- **Exit**: migration correctness invariants green under interleaving simulation.

## M6 — FIPS hardening & compliance

- FIPS build as release default; suite pinning; boundary doc complete.
- **Verify live CMVP cert + platform** (docs/07 §5) — release blocker.
- **Exit**: release artifacts FIPS-built; boundary doc signed off.

## M7 — Fleet observability & control plane

- **The proxy is store-agnostic.** It does not ship a specific control store
  (etcd/Consul/Redis/OS index); those are the operator's backend, bound through
  the existing seams — `TenancySpi`/placement lookup for reads, `MigrationStore`
  (`osproxy-control`) for migration transitions. The proxy provides the seams,
  the fleet-safe protocol (poll-fresh + drain barrier, `docs/06` §3a), and an
  in-memory reference impl; concrete bindings are consumer-provided. So M7 is
  *not* "implement etcd" — it is fleet-wide directive propagation, TTL expiry,
  and the observability below, on top of seams that already exist.
- OTLP export; aggregation integration; ring-buffer break-glass.
- **Exit**: directive flips fleet-wide without restart (against a reference
  store binding); blind-diagnosis across the full failure catalogue.

## Deferred (post-v1, behind existing seams)

- **Queue-based redundancy**: `QueueSink` (Kafka) + pull-ingester for dual/triple
  write. The `Sink` trait already accommodates it; no core change.
- Additional control-store backends; richer admin tooling.

## Cross-cutting, every milestone

Coverage ≥ thresholds, budgets/lints green, docs updated in-PR, blind-diagnosis
extended for new failure modes, ADR for any design-surface change.
