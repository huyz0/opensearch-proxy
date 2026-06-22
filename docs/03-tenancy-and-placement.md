# 03: Tenancy & Placement Model

## 1. The partition: the central concept

A **partition** is the unit of tenancy and the unit of placement. Everything
routes by partition id. Invariants:

- **One partition lives in exactly one placement at a time.** This is what makes
  single-cluster search possible (no fan-out). A mixed-partition `_bulk` is
  demuxed *by partition*; it never splits one partition across placements.
- A partition can be **moved** between placements ([06-partition-migration.md](06-partition-migration.md)),
  but at any instant it has one current placement and one epoch.

## 2. Placement kinds

| Kind | Meaning | Write behavior | Read behavior |
|------|---------|----------------|---------------|
| `DedicatedCluster` | Partition owns a whole cluster | route to cluster; index as addressed | route to cluster |
| `DedicatedIndex` | Partition owns a named index on a cluster | route to `cluster/index` | route to `cluster/index`; no partition filter needed |
| `SharedIndex` | Partition shares an index with others | inject partition field(s), construct partition-prefixed `_id`, set `_routing` | inject `bool.filter term(partition_field=P)`, strip injected fields from hits |

The injected field name(s) in `SharedIndex` are decided by the SPI
(`injected_fields()`), per the original requirement.

## 3. Placement table: mutable operational state

Placement is **not** a pure function of the partition id. It is a mutable,
versioned table:

```
PlacementTable {
    generation: Epoch,                    // monotonically increasing
    entries: Map<PartitionId, PlacementEntry>,
}
PlacementEntry {
    placement: Placement,
    epoch: Epoch,                         // the generation this entry was last changed at
    state: Active | Migrating { to: Placement, phase: MigrationPhase },
}
```

- Owned by `osproxy-control`, distributed to every proxy instance through a
  watched store (etcd/Consul/Redis/an OpenSearch index, pluggable backend).
- The **SPI provides the rules; the table provides the current mapping.** This
  split (docs/02) is what lets migration tooling edit placement without touching
  SPI code.
- Every `RouteDecision` is stamped with the table generation it was read at
  (`PlacementEpoch`). The sink rejects a write whose epoch is stale relative to
  the current table during a migration window → client retries → re-resolves
  against the new placement. This is the correctness mechanism; see
  [06](06-partition-migration.md).

## 4. Doc-id construction & collision safety

In `SharedIndex` mode, two partitions in the same index could collide on `_id`.
Therefore:

- **The partition id is mandatory in the `DocIdRule` template** in shared mode
  (validated at config load; a shared-index tenancy with a partition-free id
  template is rejected). Example: `"{partition}:{body.$.natural_key}"`.
- `_routing` is set to the partition id so all of a partition's docs land on the
  same shard set (shard-locality, cheaper search).

On **read by logical id**, the proxy applies the same template to map
logical→physical id (`GetById`, `DeleteById`, `_update`, `_mget`). The client
only ever knows the logical id.

## 5. Read-path isolation guarantee

For `SharedIndex`, partition isolation is enforced by the proxy injecting a
mandatory `bool.filter` on the partition field that the client **cannot remove
or override**:

- The rewrite wraps the *entire* client query in a `bool { must: [client_query],
  filter: [term(partition_field = P)] }`. The client query cannot escape the
  filter because it is nested inside.
- Endpoints that cannot be safely filtered (e.g. raw `_sql` passthrough,
  scripted queries that could reference other partitions) are **not** in the
  tenancy-aware set; they are rejected in shared mode (`SpiError::UnsupportedEndpoint`)
  unless explicitly allow-listed by the operator who accepts the risk.

**Stated guarantee level:** isolation is a *security boundary* for the supported
endpoint set, and *not offered at all* (request rejected) for unsupported
endpoints. There is no "best effort" middle ground, a request is either
provably filtered or rejected. This is recorded as a decision so reviewers know
the bar. See [10](10-review-process.md).

## 6. Affinity (scroll / PIT)

Cursors (scroll ids, PITs) are bound to the physical cluster that created them.
When `Affinity::Pin` is set:

- The proxy records `cursor_id -> cluster` (bounded, TTL'd map in `control`).
- Subsequent cursor requests resolve to the pinned cluster regardless of the
  partition resolution path, and the binding expires with the cursor TTL.
- Affinity is **opt-in** per the library config; off by default to avoid the
  state cost when not needed.

## 7. Validation rules (fail at config/startup)

- Shared-index tenancy without partition in `DocIdRule` → reject.
- `injected_fields` overlapping reserved/meta fields → reject.
- A partition key spec that cannot apply to a configured endpoint class → reject
  with the offending endpoint named.
- Placement table backend unreachable at startup → fail fast (no silent empty
  table that would mis-route everything).
