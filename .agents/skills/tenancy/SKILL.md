---
name: tenancy
description: "WHAT: Partition model, placement kinds, epoch-gated migration, and read isolation. USE WHEN: working on partition resolution, placement lookup, doc-id construction, the placement table, or migration in osproxy-tenancy/osproxy-control."
---

# Tenancy, placement & migration

The **partition** is the central concept: the unit of tenancy and of placement.
Everything routes by partition.

## Rules

- **One partition lives in exactly one placement at any instant.** This is what
  makes single-cluster search possible (ADR-002). A mixed-partition bulk is
  demuxed *by partition*; a partition is never split.
- **Placement kinds**: `DedicatedCluster`, `DedicatedIndex`, `SharedIndex`. In
  shared mode the partition id is **mandatory** in the doc-id template (collision
  safety) and a `bool.filter term(partition_field=P)` wraps every read query.
- **SPI = rules; placement table = mutable epoch-versioned state.** The table is
  owned by `osproxy-control`, not computed purely from the partition id
  (migration mutates it). Every routed write is stamped with the epoch it
  resolved against.
- **Migration is an epoch-gated pointer flip** (ADR-003): the proxy does not copy
  data; it rejects (retryably) any write committed against a stale epoch for a
  migrating partition. Only the brief `Cutover` phase rejects writes.
- **Read isolation is filtered-or-rejected** (ADR-006): a request is either
  provably partition-filtered or rejected — never best-effort.

## Enforced by

- Property tests: round-trip symmetry, isolation, id-collision-freedom (docs/09).
- Migration simulation tests: INV-M1..M4 (docs/06).
- Adversarial bypass tests for isolation.

## Deep dive

[docs/03-tenancy-and-placement.md](../../../docs/03-tenancy-and-placement.md),
[docs/06-partition-migration.md](../../../docs/06-partition-migration.md),
ADR-002/003/006.
