# ADR-014 — Streaming body pipeline: extract-don't-buffer, splice-don't-reserialize, SPI uses shared utils

**Status:** Accepted

## Context

Today the request body is handled by full materialization: the engine reads the
whole body into memory (`ctx.body() -> &[u8]`), parses each source document into a
`serde_json::Value` tree, mutates the tree (inject the tenant field, construct the
`_id`), and re-serializes with `serde_json::to_vec`. The bulk source benchmark
(`crates/osproxy-rewrite/benches/hot_paths.rs`, commit `e89d72c`) measured this
parse+reserialize round-trip at **~27–30× a raw byte copy** (≈26 µs for a single
64 KB document), and the working set scales with document/batch size. NFR-P3
already states the goal — *"bulk rewrite streams without buffering the whole
body"* — and it is unmet.

The body is read for at most three reasons, and none of them fundamentally
requires a tree or the whole body:

1. **Routing** — extract the partition key (e.g. `tenant_id`) when
   `PartitionKeySpec::BodyField` is declared.
2. **Id construction** — read `body.id` for a `DocIdRule` template like
   `{partition}:{body.id}`.
3. **Injection** — stamp the authoritative tenant field (e.g. `_tenant`) into the
   source document.

The declarative surface for (1) already exists (`PartitionKeySpec` /
`JsonPath` / `resolve_partition_spec` in `osproxy-tenancy`), but it consumes an
already-parsed `&serde_json::Value`, so the engine must build the tree first.

Two rejected alternatives framed the decision:

- **Raw bytes to the SPI** (hand `resolve_partition` the body as `&[u8]`). Rejected
  by the user: *"raw byte is bad as passing problem to spi."* It pushes JSON
  parsing, chunk-boundary handling, and — critically — the isolation-sensitive
  field-name/charset validation onto every SPI author. Each reimplementation is a
  fresh chance for a cross-tenant hole.
- **Status quo (full buffer + `Value` tree).** Rejected: it is the 27× cost and the
  unbounded working set NFR-P3 calls out.

## Decision

Re-architect the request body path around three rules, and give the SPI a shared,
tested toolkit so it never touches raw bytes.

### 1. Always stream — the only buffer is route-before-forward

The body flows downstream→upstream as a stream, never collected into one buffer.
The single unavoidable buffer is causal: **you cannot forward to a destination you
have not chosen**, so when the routing key lives in the body, the engine must read
the byte prefix up to that key before it can pick the upstream connection and start
forwarding. Three regimes:

- **No read, no mutate** → zero-copy stream pass-through (frames flow untouched).
- **Mutate (inject) only** → O(1) buffering: splice the injected field in as bytes
  flow (see rule 2).
- **Read for routing/id** → buffer the prefix until the declared key is found, then
  stream the rest.

A routing key carried in a **header** (`PartitionKeySpec::Header`) needs no body
buffering at all — an explicit incentive for large-body workloads to route by
header.

### 2. Never materialize — event scan + byte splice, no `Value`

On the request body path **no `serde_json::Value` is constructed.**

- **Extraction** is an event-driven pull scan over the byte stream that stops the
  moment the declared fields are in hand. Routing/id need only **top-level scalar
  fields**, which is a cheap bounded scan. Retained memory = (prefix-until-key) +
  the extracted scalar(s), never a node-per-element tree.
- **Injection** is a byte splice: on the source object's opening `{`, emit
  `"_tenant":"acme",` then forward the remaining bytes verbatim. Working set is the
  injected name+value (tens of bytes), independent of document size — the ~1×-copy
  path the benchmark measured against the 27× round-trip.

### 3. The SPI declares needs and composes shared utils — never raw bytes

Two layers keep parsing inside the proxy:

- **Declarative (common case).** The SPI states *what* it needs —
  `partition_source() -> PartitionKeySpec`, `doc_id_rule()`, `injected_fields()` —
  and the engine runs the streaming scan itself. The reference tenancy writes zero
  parsing code. (`resolve_partition`'s `Option<&Value>` argument is replaced; see
  Consequences.)
- **Util toolkit (the SPI that computes).** For real logic (hash of two fields, a
  pattern on `_index`), expose tested primitives in `osproxy-spi` that operate over
  the engine's already-extracted, typed values — never raw JSON:
  - `ExtractedFields` — scan-populated, bounded, borrow-or-small-owned; typed
    accessors (`str(path)`, `i64(path)`). It carries only the declared fields'
    scalars. It deliberately offers **no** "whole body as a tree" accessor, so the
    memory bound is enforced by the type, not by discipline. A field the proxy did
    not extract must be *declared* (added to the spec / an `extra_fields()` list) so
    the single scan picks it up — never "give me the body to go find it."
  - Pure matchers/validators/builders: `index_matches(pattern, index)`,
    `partition_from_template(tmpl, &fields)`, `hash_partition(parts, n)`,
    `validate_partition(id)` (charset/length, reserved-name rejection, fail-closed).

  These live in `osproxy-spi` (authors already depend on it; keeps the dep graph
  flat) unless the toolkit grows enough to warrant `osproxy-routing-util`.

### 4. INV-MEM — a gated invariant

> **INV-MEM**: on the request body path, peak heap is bounded by
> (prefix-until-routing-key + injected-field bytes + one bulk op), independent of
> body or batch size. No `serde_json::Value` is constructed from a request body.

Guarded by the existing dhat allocation budgets (`tests/memory.rs`), parameterized
by body size: a 64 KB and a 256 B document must show the **same** peak on the
verbatim/inject paths. This turns "no tree" from a promise into a CI gate.

## Why

- **It is NFR-P3, realized.** Streaming + no-tree removes both the 27× compute tax
  and the body-size-proportional working set in one design, most visibly for
  `_bulk` (process per line-pair, drop each op after forwarding — peak ≈ one op,
  not the whole batch).
- **Isolation stays centralized.** The streaming scanner, the field-name
  charset/length validation, and the reserved-field spoof check are
  isolation-critical. Owning them in the engine + util layer means the property
  tests (round-trip symmetry + spoof-rejection) guard *all* SPIs at once, instead
  of trusting each author to re-derive them. This is the concrete reason raw-bytes
  was rejected.
- **The SPI surface gets smaller and harder to misuse.** Implementers express a
  *decision* over typed values; they cannot ask for, and so cannot mishandle, the
  raw body.

## Consequences

- **Public SPI change (a design-review event per ADR-007).**
  `resolve_partition(ctx, Option<&Value>)` is replaced by a form that consumes
  `ExtractedFields` (engine-populated from the declared `partition_source`), plus a
  declarative `partition_source()` the engine drives the scan from. Migration is a
  breaking change to the tenancy trait; the reference tenancy and docs/05 SPI guide
  update with it.
- **Bulk fan-out is the hard case** and lands last: NDJSON with per-line routing
  means a streaming demux handling chunk boundaries that split lines and forwarding
  to multiple upstream connections concurrently. The retained state there is the
  per-target outgoing buffers (inherent to multi-target routing), not the batch.
- **Phased implementation**, each phase guarded by round-trip-symmetry + spoof +
  INV-MEM dhat tests:
  1. Pure pass-through streaming for no-transform placements (transport plumbing;
     biggest, safest win).
  2. Streaming inject (splice-on-`{`) for single-doc.
  3. Streaming partition/id extraction + the `partition_source` declaration +
     `ExtractedFields` util.
  4. Bulk streaming demux.
- **`wrap_query`'s `RawValue` approach is the precedent**: the search path already
  avoids reserializing the client query; this generalizes that posture to the body.
- The transport ingress→sink egress must stream bodies (hyper supports streaming
  request bodies both directions); the engine pipeline changes from
  "buffer→transform→forward" to "stream→incremental-transform→stream."

## Implementation status

**Landed (no-materialization foundation — green):** `core::json` byte scanner;
`inject_fields_bytes`/`construct_id_bytes`/`validate_json`; single-doc and bulk
index/create/delete de-materialized (no `Value` tree, splice not reserialize);
SPI `resolve_partition(ctx, BodyDoc)` with a `scalar(path)` extraction util and no
raw-byte accessor; INV-MEM dhat gate + serde-oracle + spoof property tests.

**Zero-buffer streaming (core-model rewrite) — verbatim forward + bulk done;
single-doc write streaming deliberately not done (unsound).** Staged:

1. **Sink streaming-capable body** — DONE (green, behavior-preserving): the
   upstream pooled clients carry a boxed body (`UpstreamBody = BoxBody<Bytes, _>`)
   instead of `Full<Bytes>`, with a `buffered()` helper; `inject_trace` is generic
   over the body. A request body may now be a buffered head, a stream, or a head +
   stream tail. No path streams yet; this is the type foundation.
2. **Streaming verbatim forward** — DONE: a tenant-agnostic passthrough request
   is streamed end to end. The sink gained `Reader::forward_stream`/`ForwardOp`
   (shared `forward_raw` with the buffered cursor op); the engine gained
   `Pipeline::is_passthrough` (body-free match) and `forward_streamed` (trace
   lifecycle minus buffered-body diagnostics); the transport `IngressHandler`
   gained `forward_plan` + `handle_forward(req, Incoming)` and `http_io` branches
   before buffering. The body stream travels *beside* the `Copy` `RequestCtx`, not
   inside it. Streaming is disabled when capture is wired (capture must tee the
   buffered body) and never applies to the proxy-internal surfaces. Response is
   still read buffered (response-body streaming is a later refinement).
3. **Streaming inject / prefix-until-key routing** for single-doc — WON'T DO
   (found unsound/infeasible): routing needs the partition key from the body, and
   the spoof check needs *every* top-level key (a client could place `_tenant`
   last), so a flat doc is fully read before it can be forwarded — there is no
   safe tail to stream. The buffered single-doc path is already CPU-optimal
   (no tree, splice) and bounded by the 413 cap; converting it would weaken the
   isolation invariant for no real gain.
4. **Bulk streaming demux** — DONE: the `_bulk` NDJSON is framed incrementally
   from the inbound stream (`NdjsonReader` over the body's frames) and each op is
   demuxed/dispatched as it is read, reusing the existing flush/gate/re-interleave
   — so the whole batch is never held (only the bounded per-target flush buffers +
   the response lines). Each op's object is still fully scanned (spoof check
   intact), one at a time. Sync write mode only (the transport decides from the
   endpoint + write-mode header; async fan-out and capture keep the buffered
   path). rewrite gained `parse_bulk_action`/`parse_bulk_op`; the engine gained
   `ingest_bulk_streamed` + `Pipeline::handle_bulk_streamed`/`is_sync_write`; the
   transport gained `wants_bulk_stream` + `handle_bulk_stream`. A per-op size cap
   bounds one giant line. Verified: streamed response == buffered response
   (same items, same order); per-item failures positioned in place.
