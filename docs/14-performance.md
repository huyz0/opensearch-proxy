# 14 — Performance Measurements

This is a **measurement record**, not a set of SLOs. Absolute numbers are
host-bound; what matters is the *shape* (does it scale with cores? is the hot path
allocation-bounded?) and the *deltas* from the optimizations below. Thresholds the
release must meet live in [docs/01 §5.1 (NFR-P)](01-architecture.md); the method is
fixed even where a number is still `[CALIBRATE]`.

Two places produce these numbers:

- **Local** — a developer box, run on demand. The figures in this doc were taken on
  a 20-core x86-64 Linux (WSL2) host over loopback. Reproduce with the commands in
  §5.
- **CI** — the **Live integration (Docker)** lane ([`.github/workflows/ci.yml`](../.github/workflows/ci.yml))
  runs the harnesses against a real OpenSearch container and renders the NFR-P
  briefs + the concurrency micro-benchmarks into the run's job summary. CI numbers
  are on shared, host-bound runners: **recorded, never gated**. Only host-independent
  invariants (correctness, completeness, pool reuse, throughput-scaling, bounded
  footprint, no dropped connections) are asserted.

## 1. Per-request hot path (CPU, single-thread)

Wall-clock micro-benchmarks of the rewrite transforms (`cargo bench -p osproxy-rewrite`,
divan, median):

| transform | median | transform | median |
|-----------|--------|-----------|--------|
| `strip_fields` | 31 ns | `construct_id` | 87 ns |
| `inject_fields` | 41 ns | `parse_mget` | 204 ns |
| `map_physical→logical` | 58 ns | `wrap_query` | 284 ns |
| `map_logical→physical` | 87 ns | `parse_bulk` | 335 ns |

Every transform is sub-microsecond. Against a ~0.8 ms request round-trip (§3) the
rewrite is **<0.1%** of request time. Allocation counts are budgeted in
`crates/osproxy-rewrite/tests/memory.rs` (dhat): `strip_fields` allocates **0**;
`wrap_query` is **~12** allocations, **down from 33** because the client query and
sibling subtrees are preserved as raw byte spans (`serde_json::RawValue`) instead of
being materialized into a `Value` tree.

## 2. Multicore contention — the two per-request shared-state ops

`cargo test -p osproxy-observe --test contention -- --ignored --nocapture --test-threads=1`.
Aggregate throughput (Mops/s) by thread count, 20-core host. A serializing lock
*stops scaling or drops*; a lock-free / cheaper path keeps climbing.

**`DirectiveStore::load()` (evaluated once per request)** — `Mutex<Arc>` → `ArcSwap`:

| threads | 1 | 2 | 4 | 8 | 16 |
|---------|---|---|---|---|----|
| Mutex (before) | 26.5 | 9.7 | 7.5 | 6.4 | **4.3** |
| ArcSwap (after) | 12.7 | 14.9 | 16.3 | 18.6 | **20.8** |

The mutex scales **negatively** (26→4.3 as cores rise — lock contention + cache-line
bouncing). `ArcSwap` scales **positively** (12.7→20.8), ~5× the mutex at 16 cores. It
is ~2× slower *uncontended* (38 ns→79 ns) — negligible on a path that then does
network I/O, and the right trade for multicore scaling.

**`ExplainStore::record()` (retained once per request)** — eager JSON → lazy:

| threads | 1 | 2 | 4 | 8 | 16 |
|---------|---|---|---|---|----|
| eager (before) | 0.08 | 0.07 | 0.08 | 0.10 | 0.12 |
| lazy (after) | **4.22** | 0.92 | 0.69 | 0.67 | 0.71 |

Building the `/debug/explain` JSON eagerly on *every* request cost ~12 µs of pure CPU
for a document almost never read. Retaining the (owned) trace and serializing lazily
on read is **~52× faster single-thread**. The lazy path is now bounded by the
ring-buffer mutex (slight negative scaling), but its ceiling (~0.7–0.9 Mops/s ≈
700–900k records/s) is far above realistic per-instance request rates, so the ring is
**not** sharded — that would be gold-plating.

Neither op was a practical bottleneck (millions of ops/s even contended); these are
CPU-efficiency and clean multicore scaling, not a throughput unblock.

## 3. Connection handling (no Docker, loopback)

`cargo test -p osproxy-server --test connection_load`. A real proxy (ingress →
pipeline → reference tenancy → sink) against an in-process mock upstream.

- **Capacity (gated, runs every CI build)**: 200 independent concurrent connections ×
  8 requests = **1,600 requests, 0 dropped, 0 errors**, and the upstream pool reuses
  connections (`opened ≪ dispatched`, NFR-P4/P5).
- **Connection-establishment microbench** (`--ignored`, sequential, isolated):
  - warm keep-alive round-trip: **p50 0.80 ms, p99 1.25 ms**
  - fresh connect + first request: **p50 1.12 ms, p99 1.56 ms** → establishment costs
    only **~0.3 ms**.
- Under a 200-connection *storm* the harness shows a long cold tail (p99 ≈ 1 s), which
  is a **co-located-load-generator + thundering-herd artifact** (the test process is
  client + proxy + mock on one box), **not** the proxy's path — proven by the
  sequential microbench above.

`TCP_NODELAY` is set on both the accepted downstream stream and the upstream
connector. It is flat on loopback (verified) but prevents Nagle/delayed-ACK tail
latency on a real network — standard for a latency-sensitive proxy.

## 4. End-to-end vs. a real OpenSearch (CI integration lane)

The Docker lane fills a real NFR-P profile (proxy vs. direct-to-cluster baseline) and
emits `nfr-*.md` briefs to the job summary + a `nfr-profiles` artifact. Representative
local figures from that harness:

- **Added latency** (proxy over direct): added **p50 ≈ 0.08 ms**, **p99 ≈ 1.7 ms** —
  well inside NFR-P1's ~1–2 ms target.
- **Upstream pool reuse** ≈ 1.0 under steady load (NFR-P4).
- **Scalability**: throughput scales ~44× (52 → 2310 rps as concurrency 1 → 64) while
  p50 stays flat (~18 → 24 ms) — the proxy scales by pool reuse, not latency inflation
  (NFR-P2).
- **Footprint**: idle ≈ 11 MiB RSS, bounded growth under a 50k-request soak (NFR-P6).

These are the authoritative end-to-end numbers; the local micro-figures above explain
*why* the per-request overhead is small.

## 5. Reproduce

```sh
# Per-request transform timing + allocation budgets (no Docker)
cargo bench -p osproxy-rewrite
cargo test -p osproxy-rewrite --test memory

# Multicore contention (no Docker)
cargo test -p osproxy-observe --test contention -- --ignored --nocapture --test-threads=1

# Connection capacity (gated) + establishment microbench (no Docker)
cargo test -p osproxy-server --test connection_load
cargo test -p osproxy-server --test connection_load \
  single_connection_request_latency_microbench -- --ignored --nocapture

# End-to-end vs. real OpenSearch (needs Docker)
cargo test -p osproxy-server --test perf_harness -- --ignored --nocapture --test-threads=1
```
