# ADR-015: mimalloc as the binary's global allocator

**Status:** Accepted

## Context

The request path is allocation-heavy: each request allocates a shape-only trace, a
stripped header copy, request/doc ids, and the response body; `_bulk` allocates per
document (id mapping, body splice, the write op, the response line). We drove the
per-document bulk cost down (ADR-014 byte splice, plus the resolution cache and the
byte-serialized response lines), but a floor of tens of allocations per request
remains and is intrinsic — the proxy's job is to reshape each request.

Two contention benchmarks then established *where* the multi-core ceiling actually
is. The per-request shared structures do **not** lock-contend:

- Sharding `ExplainStore` (16 deques keyed by `request_id`) was measured to match a
  single mutex — `record`'s high-thread cost is the trace **clone**'s allocations,
  not the lock.
- The placement-table `RwLock` read is flat under load: the alloc-free `admit_write`
  holds ~19 ns from 8→16 threads (no reader-count contention).

So the multi-core lever is the **allocator**, not lock restructuring. The system
allocator (glibc `malloc`) serializes concurrent `malloc`/`free` across worker
threads on shared arenas, which caps throughput exactly where this workload lives:
high fan-in, many small short-lived allocations, many cores.

## Options

1. **Keep the system allocator.** Zero build cost, but leaves the measured
   multi-core allocation contention on the table.
2. **Thread-per-core runtime (monoio/glommio) + shared-nothing.** Removes cross-core
   allocation entirely, but re-platforms off tokio (losing `tokio-rustls`/FIPS, tonic)
   for a syscall/thread-per-core win this proxy is not bottlenecked on (added p50
   ≈ 0.08 ms; the bottleneck is the upstream round-trip). Rejected.
3. **A modern sharded/arena allocator** (mimalloc / jemalloc / snmalloc) as the
   binary's global allocator. Per-thread heaps make concurrent allocation scale, for
   a one-line change and no runtime/architecture change.

## Decision

Set **mimalloc** (`mimalloc::MiMalloc`) as the `osproxy` binary's
`#[global_allocator]`. mimalloc over jemalloc because it builds cleanly (a small
vendored C library via `cc`, which the default `ring` crypto already requires — no
new prerequisite category) and is well-suited to many small allocations.

It is an unconditional dependency of the binary crate only — orthogonal to the
crypto provider, so default and FIPS builds both engage it with **no change to their
build commands**, and the library crates stay allocator-agnostic (their tests keep
using dhat for allocation budgets).

## Consequences

- **Throughput.** Local A/B against a real single-node OpenSearch (20-core,
  `perf_harness`): peak throughput at 64 connections rose ~25% (≈2,600 → ≈3,300 rps),
  ~15% at 32; no change at low concurrency (nothing to relieve). Single-request
  added-latency is unchanged — it is dominated by the upstream round-trip, not the
  allocator.
- **Footprint.** Idle RSS rises ~1 MiB (mimalloc reserves per-thread arenas); soak
  growth over 50k requests is unchanged (NFR-P6 still passes on either bound).
- **Build.** A C compiler is now required for *every* build (the README's tool table
  reflects this); `cmake`/`go` remain FIPS-only.
- **Boundary.** The allocator is not crypto and does not touch the FIPS module
  (ADR-004); the FIPS binary links mimalloc + aws-lc-fips without interaction.
