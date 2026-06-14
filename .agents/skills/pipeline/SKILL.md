---
name: pipeline
description: "WHAT: The request pipeline — bulk NDJSON demux, query rewrite, response field strip, pooling, affinity. USE WHEN: editing osproxy-engine, osproxy-rewrite, or osproxy-transport, or handling a new OpenSearch endpoint."
---

# Request pipeline

One pipeline handles both directions:
`ingress → authenticate → authorize → classify → resolve → transform(req) →
dispatch → transform(resp) → egress`. Each stage is a small unit with typed
in/out; no stage reaches into another's internals.

## Rules

- **Ingest demux is single-pass and streaming** (NFR-P7): never buffer a whole
  bulk body. Demux by target, remember original ordinals, dispatch concurrently
  (bounded), re-interleave `items[]` in original order with positional per-item
  status. Partial failure mirrors OpenSearch semantics (200 + `errors:true`).
- **Search is single-target.** Wrap the client query in
  `bool{must:[client],filter:[term(partition=P)]}` (shared mode) and strip
  injected fields from the response. A query that can't resolve to one partition
  is rejected, not fanned out.
- **Id paths** map logical→physical id via the doc-id template (GET/DELETE/
  update/_mget).
- **Pooling**: downstream keep-alive honored; per-cluster upstream pools sharded
  (no god-lock); reuse TLS sessions. Broken connections evicted without failing
  unrelated in-flight requests.
- **Bounded everything**: bounded queues, backpressure → 429, never OOM.

## Enforced by

- Property tests: bulk order preservation, round-trip symmetry (docs/09).
- Fault-injection suite: slow/dropped upstreams, malformed bodies, pool
  exhaustion → no panic, no stuck request (NFR-R7).
- Alloc/latency benches for NFR-P (`criterion` + `dhat`).

## Deep dive

[docs/04-request-pipeline.md](../../../docs/04-request-pipeline.md),
endpoint matrix in [docs/specs/opensearch-endpoints.md](../../../docs/specs/opensearch-endpoints.md).
