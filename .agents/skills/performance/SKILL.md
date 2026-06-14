---
name: performance
description: "WHAT: Deterministic performance, memory, I/O, and determinism gates. USE WHEN: writing hot-path code, adding a benchmark/dhat/proptest test, touching core::time, or investigating a perf/memory regression."
---

# Performance, memory & determinism (deterministic gates)

Non-functional quality is enforced **deterministically** — instruction counts and
allocation counts, never noisy wall-clock numbers (docs/12 Tier 1).

## Rules

- **Never read wall-clock time directly.** `Instant::now`/`SystemTime::now` are
  banned by clippy; take a `osproxy_core::time::Clock`. Tests use `ManualClock`
  and advance it, so timeouts/TTLs/affinity expiry are reproducible. The only
  sanctioned real-clock site is `SystemClock`.
- **No unseeded randomness** on any path whose behavior is observable; seed it
  and make it injectable.
- **Measure micro-perf in instruction counts** with `iai-callgrind` benches
  (`benches/`, `harness = false`). A refactor that doesn't change work shows zero
  delta; a real regression is visible. Wall-clock benches are not a gate.
- **Memory budgets are allocation counts** via `dhat` testing mode
  (`tests/memory.rs`, one profiler per test binary — it is process-global). Hot
  paths assert an exact alloc count; adding an allocation fails CI.
- **Hot path discipline**: stream, don't buffer whole bodies; reuse pooled
  conns/buffers; bounded queues; no blocking syscalls on async threads.

## Enforced by

- `cargo xtask clippy` — determinism bans (`disallowed-methods` in `clippy.toml`).
- `cargo xtask test` — proptest correctness + dhat memory budgets.
- `cargo xtask bench` — iai-callgrind instruction-count benches (CI, valgrind).

## Deep dive

[docs/12-quality-system.md](../../../docs/12-quality-system.md),
[docs/01-architecture.md](../../../docs/01-architecture.md) §5 (NFRs),
[docs/09-testing-and-quality.md](../../../docs/09-testing-and-quality.md).
