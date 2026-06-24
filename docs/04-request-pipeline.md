# 04: Request Pipeline

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

## 2. Ingest: single document

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

## 3. Ingest: bulk demux (the hard path)

`_bulk` is NDJSON: alternating action + (optional) source lines. A single body
may contain documents for **different partitions → different placements**.

Algorithm (single pass, streaming, NFR-P7):

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
2. `SharedIndex`: wrap client query in `bool { must:[client], filter:[term(field=P)] }`.
   The response field-strip is derived from the decision's `body_transform` (the
   injected names), not a separate decision field, see `read::read_shape` /
   `filter_terms` (`docs/02` §1).
3. Dispatch to the one target.
4. Response: strip injected fields from each hit (and from `fields`/`_source`
   projections) so the tenant sees the logical document.
5. `_msearch`: each sub-query resolves independently but each must still be
   single-target; a sub-query whose partition resolves elsewhere is fine
   (different sub-request, different target), but a *single* sub-query never
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

## 9. Asynchronous fan-out write mode

A mutation can be dispatched in one of two modes:

- **Sync** (the default): forward to the upstream and return its real result,
  the path described in §2, §3, §5.
- **Async**: durably enqueue the fully-resolved, epoch-stamped op onto a
  `WriteQueue` and return `202 Accepted` with an `op_id`. A separate downstream
  component consumes the queue and applies each op to one or more destinations
  (fan-out). The proxy's only promise is **durable acceptance into the
  pipeline**, never application, ordering across destinations, or a per-doc
  result.

### Mode negotiation

| Source | Effect |
| --- | --- |
| `with_baseline_write_mode(..)` | The deployment default (config). `Sync` unless set. |
| `X-Write-Mode: sync\|async` header | Per-request override of the baseline. Unknown value → baseline (not an error). |

Async is therefore opt-in twice over: a deployment stays fully sync unless an
operator sets the baseline or a client sends the header.

### The async contract (single-doc / bulk / delete-by-id)

1. Resolve + transform exactly as sync (same partition routing, same
   epoch-stamped op), async changes *delivery*, not *correctness*.
2. If no queue is wired, refuse with **`422`** (`status:"rejected"`). An async
   request is **never accepted-and-dropped**.
3. Enqueue. Return **`202`** only **after** the queue acknowledges durable
   acceptance (WAL/broker ack). A queue refusal is reported as **`503`** (the
   same `op_id` makes a retry idempotent downstream).
4. No live epoch gate runs: the op carries its epoch and the downstream applier
   owns staleness, there is no synchronous upstream to hold.

The `202` body is a generic async envelope, **not** a synthetic OpenSearch
result:

```json
{ "op_id": "client-key-1", "status": "accepted", "result": "queued", "_index": "orders" }
```

### `op_id`: correlation + idempotency

- Client-supplied via the **`X-Op-Id`** header when present and valid
  (non-empty, ≤128 bytes, charset `A-Za-z0-9-_.:`); otherwise the proxy mints
  one from the request id. Always present, always echoed in the `202`.
- It is the **idempotency key** the downstream applier dedups on (delivery is
  at-least-once), and the handle a client uses to correlate any
  downstream-emitted outcome.
- **Bulk** (`_bulk`) is supported in async mode: each item is resolved and
  transformed exactly as the sync demux, then enqueued individually with a
  per-item `op_id` of `{batch_id}:{ordinal}` (the `X-Op-Id`/request-id batch id
  plus the line index). The response is the normal positional `items[]`, each a
  `202 queued` line carrying its `op_id`; a per-item `update` is rejected in place
  (`400`, scripted/partial update is not honorable async), and a queue refusal is
  a per-item `503`. A whole-request refusal (no queue, or a query-level CAS param)
  returns the generic envelope rather than a partially-applied bulk.

### Queue wire format (op envelope)

Each enqueued op is a **protobuf `OpEnvelope`** (`osproxy.fanout.v1`): typed
metadata (`op_id`, `partition`, `cluster`, `index`, `epoch`, `op_type`, `id`,
`routing`) plus a `content_type` and an opaque `body`. The downstream applier
reads the metadata and forwards `body` to OpenSearch verbatim with that
`Content-Type`, it never parses the document, so the document shape never enters
the contract.

- **Body encoding**: **CBOR** (RFC 8949) by default, compact binary, ingested
  natively by OpenSearch, with JSON selectable for debuggability. This applies
  uniformly: a bulk request is demuxed into individual ops, so each bulk item is
  its own CBOR-bodied envelope, the same as a single-doc write (there is no
  binary-NDJSON framing to worry about).
- **Key**: the Kafka record is keyed by `partition`, so all ops for one logical
  partition keep their order within a partition through the fan-out.
- **Durability**: the producer is broker-acknowledged, the `202` is returned
  only after the op is acked, never fire-and-forget.

### What async does *not* cover

- **Optimistic concurrency** (`if_seq_no`/`if_primary_term`, `version`),
  **scripted/partial `_update`**, and **`_update_by_query`** cannot be honored
  async and are rejected (`400`), they need read-modify-write against current
  state the proxy cannot evaluate at enqueue time. `_update_by_query` is not even
  classified (it falls through to `Unknown` and is rejected).
- **`_delete_by_query`** is rejected by default, with an **opt-in expansion**
  (`fanout_expand_delete_by_query`): in async mode the proxy runs the
  **partition-scoped** query itself (the same mandatory isolation filter as a
  normal search), caps the match set (refusing over the cap rather than partially
  deleting), and enqueues a concrete delete per matched id, keeping the op stream
  self-contained and idempotent. It returns a delete-by-query-shaped count where
  `deleted` is what was durably enqueued (not yet applied). Sync mode, expansion
  off, or no queue all reject (`400`/`422`).
- **No status surface on the proxy.** Whether and how a failed apply is reported
  back is the downstream's responsibility (an outcome topic, an alert, a
  reconciler), out of scope here. See [client handling](guide/09-async-clients.md).
- **Read-after-write is not guaranteed**: a `202`'d doc is not queryable until
  the downstream applies it; reads still hit the upstream synchronously.

## 10. Tenant-agnostic passthrough

A deployment can forward requests **verbatim** to one cluster with no partition
resolution, body rewrite, or isolation, a transparent / capture / migration
proxy that still gets osproxy's auth, TLS, pooling, and observability. It is a
short-circuit *before* the endpoint demux above: a matching request is forwarded
raw (reusing the same verbatim-forward primitive as the cursor/admin paths) and
the upstream response is returned untouched.

The match is **per request, by logical index**, and **fail-closed**, so one
instance can serve both modes at once (the migration shape):

- `passthrough_cluster` + `passthrough_endpoint` with **no** `passthrough_indices`
  → *every* request passes through (whole-instance transparent proxy).
- `passthrough_indices` = a comma-separated **logical-index prefix list** → only
  those indices pass through; every other index keeps full tenancy. A not-yet-
  onboarded legacy index flows through untouched while onboarded indices are
  isolated, on the same instance.

Matching is on the operator-configured index list **only, never a client
header**, so a client cannot opt itself out of isolation, and a non-match keeps
tenancy (the safe direction). Unset ⇒ pure tenancy mode (the default). See
[choosing a mode](guide/10-choosing-a-mode.md).

## 11. Client header forwarding

The proxy rebuilds the upstream request from scratch, so by default the cluster
sees only the headers the proxy manages (content type, and, when span export is
on, `traceparent`/`tracestate`). For a sidecar / transparent deployment that is
too lossy, so on **every** routing path — the verbatim forward (passthrough,
admin, cursor) **and** the tenancy-shaped ones (ingest, get/delete, search,
count, bulk, `_mget`/`_msearch`) — the proxy relays the client's own headers too,
applied at the sink's single upstream-send choke point:

- **Default pass-all** (sidecar trust, `forward_client_headers=true`): every
  client header rides through, **minus** a mandatory set that is never safe to
  relay verbatim — hop-by-hop (`connection`, `keep-alive`, `proxy-*`, `te`,
  `trailer`, `transfer-encoding`, `upgrade`), `host`/`content-length` (the proxy
  targets a different host and may re-frame the body), and `accept-encoding` (the
  proxy is not a compression-transparent hop, so it never lets the client
  negotiate a transfer-coding it would relay back without round-tripping
  `content-encoding`; full compression passthrough is a separate opt-in).
- **Configurable deny** (`forward_header_deny`): drop named headers on top of the
  mandatory set, e.g. `authorization` to keep the client credential off the
  cluster. The client's `Authorization` is otherwise forwarded by default (the
  proxy still authenticates the client itself; it is consumed *and* relayed).

This is computed from the **raw** request headers (so the client `Authorization`
is available), independent of the auth-stripped view the pipeline routes and
traces on. Trace headers ride through here like any other client header; whether
the proxy overrides them with its own span is the export-gated decision in
[05 §5](05-observability.md), so with export off a client's W3C **or** B3 trace
context passes through untouched.
