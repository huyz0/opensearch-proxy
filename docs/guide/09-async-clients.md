# Async fan-out clients

By default osproxy is a synchronous OpenSearch proxy: a write returns the
upstream's real result. A deployment can also offer an **async fan-out** mode
where a write is durably enqueued and a downstream component applies it to one or
more destinations. In async mode the proxy returns `202 Accepted` with an
`op_id` instead of an OpenSearch result. See [request pipeline §9](../04-request-pipeline.md#9-asynchronous-fan-out-write-mode)
for the contract; this page is for client authors.

## What changes for the client

| | Sync | Async |
|---|---|---|
| Status | `200`/`201` | `202` (accepted), `422` (no queue), `503` (enqueue failed) |
| Body | OpenSearch result (`_version`, `result`, `_shards`) | `{ "op_id", "status", "result":"queued", "_index" }` |
| Meaning of success | Applied and queryable | **Durably enqueued, not yet applied** |
| Read-after-write | Guaranteed | **Not** guaranteed — the doc is queryable only once the downstream applies it |
| Errors from apply | In the response | Out-of-band, via the downstream's own channel (the proxy has no status endpoint) |

Unsupported in async mode (rejected with `400`): optimistic concurrency
(`if_seq_no`/`if_primary_term`, `version`), scripted/partial `_update`, and
`_update_by_query`. They need read-modify-write the proxy cannot do at enqueue
time.

## Selecting async

Per request, send `X-Write-Mode: async` (or `sync` to override a deployment whose
baseline is async). Optionally send `X-Op-Id: <key>` — a stable idempotency key
(≤128 bytes, `A-Za-z0-9-_.:`). Reuse the same `X-Op-Id` on a retry and the
downstream applier collapses the duplicate; omit it and the proxy mints one and
echoes it in the `202`.

## Handling it with the OpenSearch Java client

The typed `OpenSearchClient` deserializes responses into fixed types
(`IndexResponse`), so it cannot parse the `202` envelope. Two practical options:

**1. Header interceptor + raw transport for the envelope.** Inject the headers in
a transport interceptor (so connection pooling, auth, retry are reused), and read
the `202` body generically rather than as an `IndexResponse`:

```java
// Inject per-request headers without leaving the client's transport.
var options = RequestOptions.DEFAULT.toBuilder()
    .addHeader("X-Write-Mode", "async")
    .addHeader("X-Op-Id", idempotencyKey)   // optional; stable across retries
    .build();

// Send the same request shape, but parse the async envelope, not IndexResponse.
// (Use the generic/raw transport so the 202 body is read as { op_id, status, ... }.)
AsyncAck ack = asyncWrites.index("orders", "1", doc, options);
// ack.opId() is your correlation handle. Do NOT treat it as "indexed".
```

**2. A thin wrapper that reuses the request builders.** If you control the client
layer, wrap the request builders (reusing serialization, pooling, auth) and add
async methods that return your own `AsyncAck { opId, status }` type. This is the
"reuse the request shape, expect a different response shape" approach.

In both cases the rule is the same: **do not read `op_id` as an
`IndexResponse`** and **do not assume read-after-write**.

## Learning the outcome

The proxy does not track outcomes. If you need to know whether an op applied (or
hit a conflict at a destination), consume the downstream's outcome channel —
keyed by your `op_id` — and react there (retry, alert, reconcile). Treat `op_id`
as the join key between the `202` you received and any later outcome event.
