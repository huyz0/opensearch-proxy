# 04 — Request Pipeline

The single pipeline handles both directions. Each stage is a small, testable
unit with a typed input and output; no stage reaches across into another's
internals (NFR-Q1).

## 1. Stages

```
ingress -> decode -> authenticate -> authorize -> classify -> resolve
        -> transform(request) -> dispatch -> transform(response) -> egress
```

| Stage | Crate | Input | Output | Notes |
|-------|-------|-------|--------|-------|
| ingress | transport | bytes | framed request | h1/h2/grpc; TLS terminated |
| decode | transport | framed | normalized `Request` | protocol-neutral model |
| authenticate | engine/spi | creds | `Principal` | mTLS + token |
| authorize | engine/spi | principal+action | `Authorized` | typed deny |
| classify | core | path/method | `EndpointKind` | typed endpoint matrix |
| resolve | tenancy/spi | `RequestCtx` | `RouteDecision` | epoch-stamped |
| transform(req) | rewrite | body/query + decision | rewritten request(s) | inject/construct/filter/demux |
| dispatch | sink/transport | request(s) + target | upstream response(s) | pooled |
| transform(resp) | rewrite | response(s) + decision | client response | strip / re-interleave |
| egress | transport | client response | bytes | original protocol |

## 2. Ingest — single document

`PUT/POST /{logical_index}/_doc[/{id}]`:

1. Extract partition key (streaming, per `PartitionKeySpec`).
2. `placement_for(partition)` → `PlacementAt { placement, epoch }`.
3. Per placement kind:
   - `SharedIndex`: inject partition field(s); construct `_id` from `DocIdRule`
     (partition-prefixed); set `_routing`.
   - `DedicatedIndex`/`DedicatedCluster`: rewrite target; inject only if the SPI
     asks; construct id if a rule is present.
4. Write via `Sink` to `Target`, epoch-stamped.
5. On stale-epoch reject → return retryable error (client/SDK retries; the proxy
   may also auto-retry once after re-resolving, configurable).

## 3. Ingest — bulk demux (the hard path)

`_bulk` is NDJSON: alternating action + (optional) source lines. A single body
may contain documents for **different partitions → different placements**.

Algorithm (single pass, streaming — NFR-P7):

1. Stream-parse NDJSON line pairs without buffering the whole body.
2. For each action+doc, extract partition → resolve placement (with a
   per-request placement cache so repeated partitions resolve once).
3. Apply per-doc transforms (inject/construct/routing).
4. **Demux** into per-`Target` sub-batches, **remembering original ordinal
   index** of each item.
5. Dispatch sub-batches **concurrently** via the `Sink`, bounded by a
   concurrency limit.
6. **Re-interleave**: assemble the OpenSearch-shaped `items[]` response in the
   original ordinal order, merging per-target results. Per-item errors are
   preserved positionally so the client sees a normal bulk response.
7. Partial failure: items that failed (including stale-epoch) are marked in their
   position with a typed, retryable status; the bulk as a whole still returns
   200 with `errors: true`, matching OpenSearch semantics.

Edge cases that MUST be tested:

- Action line referencing an explicit `_index`/`_id` vs. relying on the URL.
- Mixed operation types (index/create/update/delete) in one bulk.
- A doc whose partition cannot be resolved → that item errors, others proceed.
- Very large bulk → memory stays bounded (streaming, bounded sub-batch buffers).
- Backpressure when one target is slow → does not stall the whole bulk
  unboundedly; bounded queues, then 429 on the affected items.

## 4. Search / read

`_search`, `_count`, `_msearch`:

1. Resolve partition → single placement (single-cluster; never fan-out).
2. `SharedIndex`: wrap client query in `bool { must:[client], filter:[term(field=P)] }`;
   record `ResponseTransform::StripFields(injected)`.
3. Dispatch to the one target.
4. Response: strip injected fields from each hit (and from `fields`/`_source`
   projections) so the tenant sees the logical document.
5. `_msearch`: each sub-query resolves independently but each must still be
   single-target; a sub-query whose partition resolves elsewhere is fine
   (different sub-request, different target) — but a *single* sub-query never
   fans out.

## 5. Get / delete / update by id

Logical id → physical id via the same `DocIdRule` template, then route to the
single placement. `_mget` demuxes like bulk (per-doc) and re-interleaves.

## 6. Cursors (scroll / PIT)

- Create: route by partition, record `cursor->cluster` if affinity on.
- Use: resolve to pinned cluster; strip injected fields from hits as in search.
- Expire: drop the binding at cursor TTL.

## 7. Connection pooling

- **Downstream**: honor HTTP keep-alive / h2 / grpc multiplexing; no per-request
  connection churn (NFR-P5).
- **Upstream**: per-cluster pools, sharded to avoid a central lock (docs/01 §7);
  reuse TLS sessions (NFR-P4). Health-checked; broken connections evicted and
  replaced without failing unrelated in-flight requests.
- Pool sizing is config + hot-reloadable via `control`.

## 8. Observability hooks

Each stage attaches typed span attributes (shape/ids/names only) to the request
trace. The decision chain is accumulated stage-by-stage so a failure at any
stage carries the full upstream context. See [05](05-observability.md).
