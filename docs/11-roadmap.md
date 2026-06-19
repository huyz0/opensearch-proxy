# 11 — Delivery History & Status

The project was built as thin vertical slices: each milestone shippable, tested to
the docs/09 bar, and exercising real architectural seams rather than horizontal
layers that can't be validated until the end. This doc is now a **record** of that
delivery, not a forward plan.

> **Status (2026-06-19): feature-complete and CI-green.** M0–M7 are all done
> (build/fips, clippy `-D warnings`, coverage ≥90%, deterministic perf,
> supply-chain, and a live-Docker integration lane), and the post-plan additions
> below have shipped on the seams the milestones established. **No code-side gaps
> remain.** The only outstanding items are *external* and require no engineering:
> the AWS-LC CMVP certificate award (docs/07 §5) and authoritative NFR-P thresholds
> measured on reference hardware. The milestone descriptions below are the original
> plan, kept for provenance.
>
> **Shipped beyond the original plan**, on the seams the milestones established:
> - **Async fan-out write mode** (ADR-010, docs/04 §9): per-request `X-Write-Mode`,
>   honest `202`/`op_id`, protobuf+CBOR op envelope, Kafka queue behind the
>   `WriteQueue` seam (`fanout` feature). This *is* the queue-based redundancy that
>   was a non-goal — delivered as durable async enqueue, not synchronous dual-write.
> - **Tenant-agnostic passthrough** (docs/04 §10), per-request by logical-index
>   prefix so one instance serves tenanted and legacy/agnostic traffic at once.
> - **Traffic capture** (docs/guide/08): full-fidelity tee to Kafka behind the
>   `Capture` seam (`capture` feature), runtime on-demand via diagnostics directives.
> - **Live scroll/PIT cursor affinity** end to end, with the PIT shape aligned to
>   OpenSearch (`_search/point_in_time`, `pit_id`), verified against a real cluster.
> - **Reference distributed directive store over etcd** (ADR-013, `osproxy-etcd`,
>   `etcd` feature): watch-and-cache `DirectiveStore` so directive flips reach a
>   fleet with no restart — the seam proven, not the infra mandated.
> - **Fleet-coherent diagnostic sink** (docs/05 §5): directive-selected break-glass
>   captures pushed off-instance keyed by `trace_id`, so an aggregator can serve
>   them across a fleet (`DiagnosticSink` seam + stdout reference).
> - **Modes-UX pass**: SPI collapsed to one `resolve_partition`; `capture`/`fanout`
>   features split; optional `[section]` config grouping; docs/guide/10 mode map.

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

- **The proxy is store-agnostic.** A specific control store is never *mandated*;
  it is the operator's backend, bound through the existing seams —
  `TenancySpi`/placement lookup for reads, `MigrationStore` (`osproxy-control`) for
  migration transitions, `DirectiveStore` (`osproxy-observe`) for directives. The
  proxy provides the seams, the fleet-safe protocol (poll-fresh + drain barrier,
  `docs/06` §3a), an in-memory reference impl, and an **opt-in reference etcd
  binding for the directive plane** (ADR-013). So M7 was *not* "implement etcd" — it
  is fleet-wide directive propagation, TTL expiry, and the observability below, on
  top of seams that already exist (with etcd as one shipped reference, not a core
  dependency).
- OTLP export; aggregation integration; ring-buffer break-glass.
- **Exit**: directive flips fleet-wide without restart (against a reference
  store binding); blind-diagnosis across the full failure catalogue.

## Intentionally out of scope (behind existing seams)

These are not gaps — they are deliberate boundaries. The seams exist; the
implementations are operator infrastructure or excluded by an ADR.

- **Synchronous fan-out / quorum writes** — excluded (ADR-002). The dual/triple-write
  intent is served instead by the **async fan-out write mode** (ADR-010, docs/04 §9):
  writes durably enqueued behind the `WriteQueue` seam, fanned out downstream as
  honest async enqueue (`202`/`op_id`).
- **Concrete distributed control stores beyond the etcd reference** (Consul/Redis/
  OS-index, and a `MigrationStore` binding) — operator-provided behind the
  `DirectiveStore`/`MigrationStore` seams. A reference etcd directive binding ships
  (ADR-013); migration-over-etcd awaits an async + fallible `MigrationStore` seam.
- **The external aggregator and AI agent** that consume the diagnostic plane — out
  of scope by design (docs/05); the proxy ships the emission seams, not the consumer.
- Richer admin tooling and `capture`/`fanout` packaging refinements.

## Cross-cutting, every milestone

Coverage ≥ thresholds, budgets/lints green, docs updated in-PR, blind-diagnosis
extended for new failure modes, ADR for any design-surface change.
