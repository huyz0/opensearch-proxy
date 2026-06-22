# ADR-002: No synchronous fan-out; single-target search

**Status:** Accepted

## Context

Early discussion considered fan-out search (a logical index spanning multiple
clusters, requiring scatter-gather and result merge). That implies a federation
engine: cross-cluster sort/pagination over-fetch, lossy `terms` agg merge,
non-mergeable approximate aggs (percentiles/cardinality), and incomparable
relevance scores across clusters.

## Decision

**Every search/read resolves to exactly one physical cluster.** A partition's
data normally lives in one place. There is no synchronous fan-out anywhere.

"Ingest fan-out" survives only as **demux**: a mixed-partition `_bulk` is split
*by partition*, each partition's slice going to its single placement. One
partition is never split across placements.

## Why

- Deletes the entire scatter-gather/merge engine, cross-cluster scoring, and
  agg-merge correctness problems, enormous scope and correctness reduction.
- Makes single-cluster search well-defined: it requires that a partition lives in
  one place (ADR-003 preserves this across migration).
- OpenSearch CCS exists for genuine cross-cluster needs; out of scope here.

## Consequences

- A query that cannot resolve to one partition/placement (e.g. cross-partition
  wildcard) is **rejected** (docs/specs/opensearch-endpoints.md), not fanned out.
- Multi-write redundancy cannot be synchronous either; deferred to a queue-based
  mode (ADR-008).
