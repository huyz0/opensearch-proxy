# ADR-013 — Reference distributed DirectiveStore over etcd (watch-and-cache)

**Status:** Accepted

## Context

The diagnostics directive control plane (`docs/05` §3, NFR-T3) must flip
verbosity **fleet-wide with no restart**. The proxy runs as many instances behind
a load balancer, so a directive published to one instance must reach all of them.
The shipped `DirectiveStore` seam has an in-memory reference impl (fed by
`POST /admin/directives`) that is single-instance only — fine for a dev box, not a
fleet. We wanted a concrete distributed backing that proves the seam without
making the library depend on any one coordination service.

Two constraints shaped the design:
- `DirectiveStore::load()` is on the **request hot path** (polled fresh per
  request), so it must be a cheap cached read — never per-request network I/O.
- The proxy's discipline is **ship the seam, not the infra** — a distributed store
  is operator infrastructure.

## Decision

Ship **`osproxy-etcd`, a separate leaf crate** implementing `DirectiveStore` over
etcd v3, using the **watch-and-cache** model:

- `EtcdDirectiveStore::connect` does an initial read (fail-fast on an unreachable
  etcd) and spawns a background **watch** task that keeps a locally-cached
  `Arc<DirectiveSet>` snapshot fresh. `load()` is a cheap `Arc` clone.
- It backs **only** the directive (observability) control plane. The
  migration/placement store (`MigrationStore`) needs a linearizable compare-and-swap
  and a fallible, async seam; wiring it over etcd is deferred to that seam refactor.
- **One fail-closed decoder**: the JSON→`DirectiveSet` decoder moved down into
  `osproxy-observe` (`decode_directive_set`), so the admin endpoint and the etcd
  watcher validate identically — a typo'd key can never widen blast radius on
  either path.
- It is **opt-in behind the server's `etcd` feature**; the default binary links no
  etcd/tonic client. Under etcd, the etcd key is the control plane and the local
  `POST /admin/directives` publish path is disabled (no publish to a store the
  pipeline ignores). `etcd-client` is pulled with its `tls` feature **off**, so it
  adds no second crypto provider to a FIPS build.

## Why

- **etcd fits this shape exactly** (it is what Kubernetes uses): a watch stream
  feeds the local cache, MVCC revisions are a natural generation, and leases/TTL
  and linearizable txns are there when the migration side needs them.
- **Watch-and-cache, not poll-the-hot-path**: per-request reads to etcd would
  wreck NFR-P1. Correctness holds because the snapshot is eventually consistent and
  directives are TTL'd/sampled — staleness under-applies diagnostics, never
  mis-routes data.
- **Fail-fast at startup, fail-safe while running**: an unreachable etcd at boot is
  a loud error; a transient outage or a malformed publish keeps the **last good**
  snapshot rather than blanking fleet diagnostics, and the watch reconnects.

## Consequences

- A second concrete `DirectiveStore` exists; both are exercised (unit tests for the
  apply/last-good logic, a Docker-gated live round-trip against real etcd v3).
- The directive decoder is now owned by `osproxy-observe`; `osproxy-server` and
  `osproxy-etcd` both call it (no duplicated, driftable decoder).
- `MigrationStore`-over-etcd remains future work, explicitly gated on making that
  seam async + fallible (so a backend error fails closed at the write gate).
- The crate is a leaf adapter like `osproxy-otlp`: nothing depends upward on it,
  and the dependency graph stays downward-only.
