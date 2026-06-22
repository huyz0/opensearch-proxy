# ADR-006: Read isolation: provably filtered or rejected

**Status:** Accepted

## Context

In `SharedIndex` tenancy, multiple partitions share an index. Read isolation
could be a security boundary (unbypassable) or best-effort (logical convenience).
Best-effort is dangerous: a client query that escapes the partition filter leaks
another tenant's data.

## Decision

Isolation is a **security boundary** for the supported endpoint set, and **not
offered (request rejected)** for endpoints that cannot be safely filtered. There
is no best-effort middle ground, a request is either provably filtered or
rejected.

Mechanism: the proxy wraps the entire client query in
`bool { must: [client_query], filter: [term(partition_field = P)] }`. The client
query is nested and cannot remove or override the filter. Endpoints that could
reference other partitions in ways the wrapper can't constrain (e.g. raw `_sql`,
arbitrary scripts) are not tenancy-aware and are rejected in shared mode unless an
operator explicitly allow-lists them and accepts the risk.

## Why

- A leak is a critical security failure; "best effort" isolation is effectively no
  isolation under adversarial queries.
- "Filtered or rejected" gives a provable guarantee testable adversarially
  (docs/09 §2.7).

## Consequences

- The supported endpoint matrix is explicit (docs/specs/opensearch-endpoints.md);
  unsupported endpoints reject in shared mode.
- Adversarial bypass tests (nested bool, `should`, scripts, `_sql`) are a
  permanent part of the suite.
- `DedicatedIndex`/`DedicatedCluster` need no filter (physical isolation).
