# ADR-003: Epoch-gated partition migration, no in-path dual-write

**Status:** Accepted

## Context

Partitions move between placements. Moving a partition that takes live writes is
a consistency problem. Options ranged from stop-the-world, to in-path dual-write
during migration, to "proxy only flips the pointer."

## Decision

The proxy **does not copy data** (external reindex/snapshot tooling does). It
guarantees write correctness across the flip via an **epoch-gated routing
pointer**: every `RouteDecision` is stamped with the placement-table generation
it was resolved against; the `Sink` rejects (retryably) any write committed
against a stale epoch for a `Migrating` partition. The only write-rejection
window is the brief `Cutover` phase.

No in-synchronous-path dual-write.

## Why

- In-path dual-write would replicate the exact mechanism deliberately deferred to
  the queue-based redundancy mode (ADR-008), avoid building it twice.
- Epoch gating reduces correctness to a single invariant: *no write commits
  against a stale epoch for a migrating partition*, small, testable (INV-M1..M4).
- Preserves ADR-002's "one partition, one place at any instant."

## Consequences

- Brief retryable write rejection at cutover (clients/SDKs retry; surfaced as a
  normal observable event, not an outage).
- Migration tooling drives state transitions via the control-plane API
  (operator/automation, not AI, ADR-005).
- Requires a versioned (generation-stamped) placement table (docs/03).
