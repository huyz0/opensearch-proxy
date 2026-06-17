# ADR-010 — Async fan-out write mode: same endpoints, `202`/`op_id`, no proxy status surface

**Status:** Accepted

## Context

ADR-008 reserved a pull-based redundancy path: the proxy enqueues writes and
separate ingesters fan them out to 1..N OpenSearch destinations. This ADR settles
the **client-facing contract** for that mode — how a request selects it, what the
proxy returns, and where the boundary of the proxy's responsibility sits.

The tension: OpenSearch's write protocol is synchronous and per-document
(`_version`, `result`, per-item `_bulk` status, optimistic concurrency). A
fan-out enqueue cannot honor those — it responds before the write is applied, and
"applied" may mean N destinations that converge only eventually.

## Decision

1. **Same endpoints, not a new URL namespace.** Async reuses the existing
   OpenSearch paths (`_doc`, `_bulk`, delete-by-id). Being a drop-in OpenSearch
   front is the product's reason to exist; a parallel `/_async/...` tree would
   force every client and tool off the stock SDK transport.
2. **Per-request negotiation over a deployment baseline.** `X-Write-Mode:
   sync|async` selects per request; `with_baseline_write_mode` sets the default.
   Default is `Sync`, so a deployment is fully synchronous unless opted in.
3. **`202 Accepted` + a generic envelope, not a synthetic OpenSearch result.**
   The body is `{ op_id, status:"accepted", result:"queued", _index }`. Honest
   about "enqueued, not applied" rather than faking `result:"created"` with a
   meaningless `_version`.
4. **Refuse, never lie.** Async requested but no queue → `422`. Queue refuses the
   op → `503`. The `202` is returned **only after** the queue acknowledges
   durable acceptance. There is no "synthesize success and skip the work" path.
5. **`op_id` is the correlation handle and idempotency key.** Client-supplied via
   `X-Op-Id` (validated, ≤128 bytes, safe charset) or proxy-minted; always
   echoed. The downstream applier dedups on it (delivery is at-least-once).
6. **No status surface on the proxy.** Outcome notification (apply success,
   conflict, failure) is the downstream's responsibility — an outcome topic, an
   alert, a reconciler. The proxy does not poll or store outcomes.
7. **Unsupported-async ops are rejected (`400`), not enqueued.** Optimistic
   concurrency, scripted/partial `_update`, and `_update_by_query` need
   read-modify-write the proxy cannot do at enqueue time. **`_delete_by_query`**
   is reject-by-default with an **opt-in bounded expansion**
   (`fanout_expand_delete_by_query`): the proxy runs the partition-scoped query
   itself, caps the match set, and enqueues a concrete delete per matched id —
   never a partial delete, never a query the fan-out can't carry.

## Why

- **Reuse where OpenSearch already speaks; extend minimally where it does not.**
  Headers (`X-Write-Mode`, `X-Op-Id`) inject through the stock client transport
  without leaving the typed API; a separate path or query param would not. For
  by-query mutations the native Tasks API (`wait_for_completion=false`) is the
  reuse point if a poll model is ever wanted — but with no proxy status surface
  (decision 6) it stays reject-by-default for now.
- **The dangerous failure is the silent lie**, not the honest refusal. A `422`/
  `503`/`400` can't be misread; a synthetic `result:"created"` over an op that
  was never enqueued deletes the only evidence nothing happened.
- **The proxy ships the seam, not the infra** (consistent with ADR-005's
  "observe, don't own" posture): a `WriteQueue` trait + the `op_id`/envelope
  contract. The queue implementation (Kafka + WAL) and the fan-out applier are
  separate components.

## Consequences

- `WriteMode` + `WriteQueue` live in `osproxy-engine` (`asyncwrite.rs`); the
  shipped binary wires a durable Kafka/WAL implementation behind the seam (reuses
  the capture-arc producer stack). Default `NoQueue` keeps async off.
- **Wire format: a protobuf `OpEnvelope`** (`osproxy.fanout.v1`, in
  `osproxy-server`) — typed metadata wrapper + `content_type` + opaque `body`.
  The body is **CBOR** by default (RFC 8949: compact, OpenSearch-native via
  XContent, portable across applier languages), JSON selectable for
  debuggability; bulk stays JSON for now. Protobuf was chosen for the wrapper (IDL
  contract, already in the build via `protoc`); the opaque-body split keeps the
  document shape out of the contract and avoids a transcode-back step (the applier
  forwards `body` with `content_type`). Keyed by `partition` for per-partition
  ordering; produced with broker-ack durability so the `202` is truthful.
- **Read-after-write is not guaranteed** in async mode: a `202`'d doc is
  queryable only once the downstream applies it; reads still hit the upstream
  synchronously. Client-visible; documented in `guide/09-async-clients.md`.
- Clients on the typed OpenSearch SDK must parse the `202` envelope themselves
  (it is not an `IndexResponse`) — handled by a header interceptor + generic
  body read, or a thin wrapper reusing the request builders.
- Bulk per-item correlation (per-item `op_id` vs. `batch_id` + line index) is
  owned by the bulk demux and settled when async bulk lands.

Supersedes nothing; extends [ADR-008](008-sink-trait-deferred-redundancy.md).
