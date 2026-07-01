# 11. Performance

This page records **what osproxy actually does under load**: throughput and
latency by payload size, connection count, and write mode, plus the per-request
internals that explain the numbers. It is a measurement record, not a set of SLOs:
absolute numbers are **host-bound**, so what matters is the *shape* (how it scales)
and the *deltas*. The release targets (NFR-P) live in
[Requirements & NFRs](02-requirements-and-nfrs.md).

## Test environments

| | Local (this report) | CI (Live integration lane) |
|---|---|---|
| CPU | Intel i5-13600KF, 10C/20T | GitHub `ubuntu-latest`, 4 vCPU |
| RAM | 32 GB | 16 GB |
| OS | Linux 6.18 (WSL2) | Ubuntu (GitHub-hosted) |
| Network | loopback | loopback + containerized OpenSearch |

Two harness styles produce the numbers below:

- **No-Docker, in-process** (load matrix, hot-path, contention, connections): a mock
  upstream and the load generator share the process with the proxy, so absolute
  figures are inflated by co-located CPU contention. Good for *relative* comparisons
  (payload, mode, before/after).
- **No-Docker, differential** (proxy overhead, mode overhead): the same harness, but
  each cell is measured twice — direct client→upstream and proxied
  client→proxy→upstream — and only the **difference** is reported, at low concurrency.
  The generator, loopback, and upstream are in both legs and cancel, so what remains
  is the proxy's own per-request cost. This is how to read proxy overhead, not the
  inflated absolute numbers.
- **Docker, real OpenSearch** (NFR-P harness): the authoritative end-to-end numbers,
  run in CI and rendered into the run's job summary.

All figures are **recorded, never gated**. CI asserts only host-independent
invariants (correctness, pool reuse, throughput-scaling, bounded footprint, no
dropped connections).

## Load matrix: payload × connections × mode

End-to-end through the full pipeline (ingress → tenancy → rewrite → sink) against
the in-process mock upstream. **Sync** forwards each write to the upstream and
returns its result; **async** is the fan-out write mode (ADR-010), resolve +
rewrite + enqueue, returning `202` without an upstream round-trip. Local box;
`rps` is steady-state, `p50/p99` in milliseconds.

> **This table is *absolute* latency, not proxy overhead.** The generator, the
> proxy, and the mock upstream all share this one box, so these figures include the
> harness, and the tall p99s at 256 connections are **queueing at the box's
> throughput ceiling** (Little's law), not the proxy's cost. The proxy's *own* added
> latency — generator and upstream subtracted out — is ~0.3 ms at 64 KB; see
> [Proxy overhead, isolated](#proxy-overhead-isolated-differential) just below.

| payload | conns | sync rps | sync p50 | sync p99 | async rps | async p50 | async p99 |
|---------|------:|---------:|---------:|---------:|----------:|----------:|----------:|
| 256 B | 16 | 15,592 | 0.53 | 1.05 | 46,181 | 0.31 | 0.51 |
| 256 B | 64 | 36,450 | 0.85 | 1.56 | 106,948 | 0.50 | 1.06 |
| 256 B | 256 | 13,627 | 4.35 | 7.22 | 142,940 | 1.47 | 3.27 |
| 4 KB | 16 | 23,649 | 0.62 | 0.94 | 45,322 | 0.31 | 0.53 |
| 4 KB | 64 | 41,515 | 1.40 | 2.44 | 87,423 | 0.61 | 1.31 |
| 4 KB | 256 | 41,773 | 5.56 | 10.49 | 61,512 | 2.22 | 4.97 |
| 64 KB | 16 | 9,347 | 1.54 | 2.37 | 24,468 | 0.56 | 0.98 |
| 64 KB | 64 | 12,464 | 4.52 | 6.68 | 40,291 | 1.37 | 2.49 |
| 64 KB | 256 | 12,325 | 19.32 | 31.14 | 43,655 | 5.17 | 10.79 |

What it shows:

- **Payload dominates throughput.** ~14–42k rps at 256 B and 4 KB, dropping to
  ~9–12k at 64 KB. Large bodies are bound by socket I/O and memory bandwidth (most of
  it the co-located generator + upstream), not the routing logic. (The lone low
  256 B/256 cell is a concurrency-saturation dip, not a payload effect — the
  co-located generator floods the box at 256 connections regardless of size.)
- **Async fan-out is consistently faster** (higher rps, lower latency) than sync,
  because it skips the upstream round-trip, e.g. 256 B/64: 107k vs 36k rps. This is
  the cost of synchronous durability vs. accepting a `202` and applying downstream.
- **The p99 tail at 256 connections is queueing, not proxy cost.** Throughput
  plateaus past ~64 connections (64 KB: flat ~12k rps), so extra connections only add
  queue depth and the tail rises — `latency ≈ concurrency / throughput`. Proven by
  ablation in [the queueing section](#why-the-tail-grows-with-connections--queueing-not-the-proxy);
  more cores and a lock-free breaker both leave it unchanged.

Reproduce: `cargo test -p osproxy-server --test load_matrix -- --ignored --nocapture`.

## Proxy overhead, isolated (differential)

The load matrix above is *absolute* latency in a co-located harness, so it measures
the generator and upstream as much as the proxy. The differential bench isolates the
**proxy's own** added cost (direct vs. proxied, low concurrency, harness cancels):

| payload | proxy added p50 | of which |
|---------|----------------:|----------|
| 256 B | ~0.15 ms | fixed cost (parse, route, rewrite logic, dispatch) |
| 4 KB | ~0.21 ms | + body handling |
| 64 KB | ~0.29 ms | ~0.15 ms fixed + ~0.13 ms body-size-dependent |

The proxy adds **~0.15 ms fixed plus ~0.13 ms that scales with body size**. Of that
body cost at 64 KB, the avoidable *userspace* copy (the inject splice) is ~1 µs —
**under 1%** (cross-checked against the rewrite micro-bench: a 64 KB verbatim copy is
~1 µs). The rest is **kernel socket I/O** (reading the body in, writing it out),
inherent to any proxy that touches the body. There is no cheap copy left to remove.

Reproduce: `cargo test -p osproxy-server --test proxy_overhead -- --ignored --nocapture`.

### Why the tail grows with connections — queueing, not the proxy

The load matrix p99 climbs steeply at 256 connections (64 KB: ~159 ms). That tail is
**not** proxy cost — it is queueing at a throughput ceiling (Little's law:
`latency ≈ concurrency / throughput`). Two ablations
(`--test isolation_scaling`, plus a circuit-breaker lock-free A/B) prove it:

- Giving the proxy its **own** runtime (separate cores from the generator) halves the
  tail at 16–64 connections but **changes nothing at 256** — more cores don't help,
  so it is not core contention.
- Making the one per-request lock (the circuit breaker) lock-free **changed nothing**
  — so it is not lock contention.

Past the throughput knee, every extra connection just deepens the queue. The lever is
**horizontal scale** (cap connections per instance near the knee, add instances), not
a per-request micro-optimization.

## Choosing a mode: routing vs. body-rewrite cost

The four [proxy modes](10-choosing-a-mode.md) differ in whether they touch the body.
Their proxy-added latency (differential, p50, low concurrency):

| payload | passthrough (stream, no rewrite) | dedicated cluster / index (route, no rewrite) | shared (route + body rewrite) |
|---------|---------------------------------:|----------------------------------------------:|------------------------------:|
| 256 B | ~0.08 ms | ~0.08 ms | ~0.09 ms |
| 64 KB | ~0.29 ms | ~0.29 ms | ~0.30 ms |

**Mode choice is not a latency decision.** All four modes add ~0.1–0.3 ms and sit
within run-to-run noise of each other; the body rewrite (shared) costs ~nothing
measurable over no-rewrite routing (the inject splice is ~1 µs, swamped by socket
I/O). Streaming passthrough ≈ buffered dedicated *on latency* — its real advantage is
**memory footprint and time-to-first-byte** for large/streaming bodies, not p50.
Pick a mode for its **isolation model** (see [Choosing a Mode](10-choosing-a-mode.md)),
then scale horizontally for throughput.

Reproduce: `cargo test -p osproxy-server --test mode_overhead -- --ignored --nocapture`.

## Per-request hot path (CPU, single-thread)

Rewrite transform timing (`cargo bench -p osproxy-rewrite`, divan, median):

| transform | median | transform | median |
|-----------|--------|-----------|--------|
| `strip_fields` | 30 ns | `construct_id` | 87 ns |
| `inject_fields` | 35 ns | `parse_mget` | 212 ns |
| `map_physical→logical` | 63 ns | `wrap_query` | 288 ns |
| `map_logical→physical` | 77 ns | `parse_bulk` | 334 ns |

Every transform is sub-microsecond, <0.1% of a request. Allocations are budgeted
(dhat, `crates/osproxy-rewrite/tests/memory.rs`): `strip_fields` allocates 0, and
`wrap_query` is ~12 allocations (down from 33) because the client query is preserved
as raw bytes (`serde_json::RawValue`), never re-parsed.

## Multicore scaling of the per-request shared state

Aggregate throughput (Mops/s) by thread count
(`cargo test -p osproxy-observe --test contention -- --ignored --test-threads=1`).
These optimizations shipped after measuring a contention cliff:

**`DirectiveStore::load()` (per request)**, `Mutex<Arc>` → `ArcSwap`:

| threads | 1 | 2 | 4 | 8 | 16 |
|---------|---|---|---|---|----|
| Mutex | 26.5 | 9.7 | 7.5 | 6.4 | 4.3 |
| ArcSwap | 12.7 | 14.9 | 16.3 | 18.6 | 20.8 |

The mutex scaled **negatively** (contention cliff); `ArcSwap` scales **positively**
(~5× at 16 cores), at the cost of being ~2× slower uncontended (38→79 ns).

**`ExplainStore::record()` (per request)**, eager JSON → lazy:

| threads | 1 | 2 | 4 | 8 | 16 |
|---------|---|---|---|---|----|
| eager | 0.08 | 0.07 | 0.08 | 0.10 | 0.12 |
| lazy | 4.22 | 0.92 | 0.69 | 0.67 | 0.71 |

Building the explain JSON for *every* request cost ~12 µs of CPU for a document
almost never read; retaining the trace and serializing lazily is ~52× faster.

### The global allocator (mimalloc)

Both operations above plateau under contention because the remaining cost is the
per-request **clone** (the trace, the directive snapshot) — *allocation*, not the
lock. Measured confirmation: sharding `ExplainStore` matched a single mutex, and the
placement-table `RwLock` read shows no reader contention at 16 threads (`admit_write`
is flat ~19 ns from 8→16 threads). So the fleet-wide lever is the allocator, not lock
restructuring. The `osproxy` binary sets **mimalloc** as its `#[global_allocator]`;
its per-thread sharded heaps cut the cross-thread `malloc`/`free` contention this
allocation-heavy path incurs. Local A/B against a real OpenSearch (20-core): peak
throughput at 64 connections rose ~25% (≈2,600 → ≈3,300 rps), with no change at low
concurrency (nothing to relieve) and single-request latency unchanged
(upstream-dominated). It is engaged for default and FIPS builds alike.

## Connection handling

`cargo test -p osproxy-server --test connection_load` (no Docker):

- **Capacity (gated every CI build)**: 200 independent concurrent connections × 8
  requests = 1,600, **0 dropped**, upstream pool reuses connections (`opened ≪
  dispatched`).
- **Establishment (microbench, sequential)**: warm keep-alive round-trip p50 0.80 ms;
  fresh connect + first request p50 1.12 ms → establishment ≈ **0.3 ms**. A
  200-connection *storm* shows a ~1 s cold tail, but that is a co-located-load /
  thundering-herd artifact of the harness, not the proxy path.

`TCP_NODELAY` is set on both the accepted downstream stream and the upstream
connector (flat on loopback; prevents Nagle tail latency on a real network).

## End-to-end vs. a real OpenSearch (CI, authoritative)

The Docker integration lane fills an NFR-P profile (proxy vs. direct baseline) and
renders briefs to the job summary. Representative figures:

- **Added latency** (proxy over direct): p50 ≈ 0.08 ms, p99 ≈ 1.7 ms, inside
  NFR-P1's ~1–2 ms target.
- **Pool reuse** ≈ 1.0 under steady load (NFR-P4).
- **Scalability**: throughput scales ~44× (52 → 2,310 rps as concurrency 1 → 64) with
  p50 flat (~18 → 24 ms), scales by pool reuse, not latency inflation (NFR-P2).
- **Footprint**: idle ≈ 12 MiB RSS (the `mimalloc` allocator reserves ~1 MiB of
  per-thread arenas over the ~11 MiB system-allocator baseline); bounded growth under
  a 50k-request soak (NFR-P6).

## Reproduce everything

```sh
cargo test  -p osproxy-server --test load_matrix      -- --ignored --nocapture  # absolute end-to-end
cargo test  -p osproxy-server --test proxy_overhead   -- --ignored --nocapture  # proxy overhead (differential)
cargo test  -p osproxy-server --test mode_overhead    -- --ignored --nocapture  # routing vs body-rewrite by mode
cargo test  -p osproxy-server --test isolation_scaling -- --ignored --nocapture # co-located vs isolated (the tail is queueing)
cargo test  -p osproxy-observe --test contention      -- --ignored --nocapture --test-threads=1
cargo test  -p osproxy-server --test connection_load                          # capacity (gated)
cargo test  -p osproxy-server --test connection_load single_connection_request_latency_microbench -- --ignored --nocapture
cargo bench -p osproxy-rewrite                                                 # hot-path timing
cargo test  -p osproxy-rewrite --test memory                                   # allocation budgets
cargo test  -p osproxy-server --test perf_harness     -- --ignored --nocapture --test-threads=1  # needs Docker
```

To profile the per-request CPU breakdown with an external profiler (no kernel
support needed), the `profile_64k` test exposes 64 KB and 256 B single-connection
loops as callgrind targets; see that file's module docs for the `valgrind
--tool=callgrind` invocation.
