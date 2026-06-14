---
name: testing
description: "WHAT: Semantic ≥90% coverage, property/simulation tests, and test-quality standards. USE WHEN: writing any test, adding a feature (tests-first), or touching coverage config."
---

# Testing & quality

Coverage is a floor, not the goal — we want the behaviors, invariants, and
failure modes tested, not just lines executed.

## Rules

- **Tests-first for behavior.** Write the test that pins the invariant, watch it
  fail, then implement. A feature is not done until its error paths are tested.
- **Coverage**: ≥90% overall; `spi`/`tenancy`/routing-core ≥95%;
  `rewrite` ≥95% incl. branch; 100% of request-path error variants constructed
  and asserted.
- **Property tests for the correctness invariants** (`proptest`): round-trip
  symmetry `strip(read(write(doc)))==doc`, partition isolation, bulk order
  preservation, id-collision-freedom.
- **Deterministic always.** No sleeps, wall-clock, or network flakiness — inject
  clocks and stores. Migration uses time-controlled simulation (INV-M1..M4).
- **One behavior per test**, named for the behavior; diagnostic assert messages.
- **Mutation testing** (`cargo-mutants`) on `spi`/`tenancy`/`rewrite` periodically
  — the real defense against high-coverage/weak-assertion tests.

## Enforced by

- `cargo xtask test` (+ `--doc`); `cargo llvm-cov --fail-under-lines 90` in CI.
- Blind-diagnosis + no-value-leak tests (see the `observability` skill).

## Deep dive

[docs/09-testing-and-quality.md](../../../docs/09-testing-and-quality.md).
