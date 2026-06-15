# 06 — Partition Migration

## 1. Contract (decided)

The proxy **does not copy partition data**. An external reindex/snapshot-restore
tool performs the copy. The proxy's job is to guarantee **write correctness
across the routing flip** with at most a brief reject-and-retry window at the
instant of cutover — never a dual-write in the synchronous path (that is the
deferred Kafka-based redundancy mode, docs/00 §non-goals).

This is the "(c) + thin (a) guard" decision from the design conversation.

## 2. Epoch gating — the correctness mechanism

Every `RouteDecision` is stamped with the `PlacementEpoch` (the placement table
generation) it was resolved against. The `Sink` compares the stamped epoch to
the current table state for that partition before committing the write:

- **Epoch current** → write proceeds.
- **Epoch stale AND partition is `Migrating`** → reject with a typed, retryable
  error (`SinkError::StaleEpoch`). The client/SDK retries; the retry re-resolves
  against the new placement.
- **Epoch stale but partition `Active`** (table advanced for an unrelated
  reason) → write may still proceed if the placement for *this* partition is
  unchanged; the check is per-partition, not global, to avoid spurious rejects.

## 3. Migration phases

```
Active(A)
   │  operator/migration tool marks partition Migrating { to: B }
   ▼
Migrating { from: A, to: B, phase: Draining }   // new writes still go to A; epoch bumped
   │  external tool copies A -> B (reindex/snapshot)
   ▼
Migrating { from: A, to: B, phase: Cutover }    // brief: writes to A rejected (StaleEpoch->retry)
   │  table flips current placement to B, generation++
   ▼
Active(B)                                        // writes/reads now resolve to B
```

- The **only** window with write rejection is `Cutover`, kept short. During
  `Draining`, writes continue to A normally; the copy tool is responsible for
  catching up the delta (or the operator quiesces the partition — operator's
  choice, documented).
- Reads follow current placement: A during Draining, B after the flip.

## 3a. Fleet safety: no client-side cache + a drain barrier

The proxy runs as **many instances**. Each resolves placement (and re-checks the
write gate) by polling the shared backend **fresh on every request** — no
migration decision is cached in an instance — so the backend is the single
synchronized source of truth. The backend is **operator-provided behind the SPI**
(the in-memory `PlacementTable` is the reference impl; a watched store such as
etcd/Consul is bound through the same seam — the proxy does not ship one).

When a placement read **fails because the backend is momentarily unavailable**,
the proxy **retries with bounded exponential backoff** rather than failing the
request outright; only after the attempts are exhausted does it surface a
retryable error. A definitive answer of *reject* (cutover/stale epoch) is never
retried in-proxy — it is correct, and the client re-resolves.

That leaves one residual window: a write whose gate passed an instant *before*
`Cutover` was published may still be committing upstream. So the controller does
not flip the pointer immediately. After publishing `Cutover` it holds a **drain
barrier** — at least the upstream write timeout (NFR-R7) — before
`complete_migration` is allowed. By then every pre-cutover write has committed or
hit its deadline, so none can land in the old placement after the flip. The
barrier is enforced by `osproxy-control`'s `ControlPlane` against an injected
clock (deterministic in tests); one controller drives a given partition's
migration.

## 4. Why no in-path dual-write

Dual-write during migration would replicate the very thing we deliberately
deferred to the queue-based redundancy mode. Keeping migration to a pointer flip
+ epoch gate keeps the synchronous path simple and the correctness argument
small (one invariant: *no write commits against a stale epoch for a migrating
partition*).

## 5. Proxy responsibilities (what "the proxy helps with migration" means)

- Expose partition state (Active/Migrating + phase) in observability so an
  operator/LLM can see exactly where a migration is.
- Enforce the epoch gate so no write lands in the wrong place across the flip.
- Provide a **control-plane API** (read + the migration state transitions) that
  the external migration tooling drives — this is operational, human/automation
  controlled, NOT AI-mutated (docs/05 §read-only).
- Surface stale-epoch retries as a normal, observable event (not an error spike
  that looks like an outage).

## 6. Invariants (tested)

- INV-M1: No write commits against a stale epoch for a `Migrating` partition.
- INV-M2: After `Cutover` completes, no in-flight request resolves to the old
  placement (epoch monotonicity guarantees this).
- INV-M3: A migration that aborts mid-flight returns the partition to `Active(A)`
  with no committed writes to B for that partition after the abort point.
- INV-M4: Reads never see a partially-migrated split view (single placement at
  any instant).

These are verified with deterministic, time-controlled simulation tests
(docs/09 §property/simulation testing).
