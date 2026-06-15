# 08 — Engineering Standards

These are enforced in CI, not just recommended. The goal: high code quality, no
god modules, clear structure, high traceability — verifiable mechanically where
possible.

## 1. No god module / file / type — size & cohesion budgets

| Budget | Limit | Enforcement |
|--------|-------|-------------|
| File length | ≤ 400 lines (soft warn 300) | `cargo xtask lint` line counter, CI fail over hard limit |
| Function length | ≤ 60 lines | clippy `too_many_lines` configured |
| Function args | ≤ 7 (prefer a struct) | clippy `too_many_arguments` |
| Cyclomatic complexity | ≤ 15 per fn | complexity lint |
| Type field count | ≤ ~12 (prefer composition) | review checklist + lint where available |
| Module fan-in/out | a module with >1 clear responsibility is split | review checklist |
| Public API per module | cohesive; a module exporting unrelated groups is split | review checklist |

A limit may be exceeded **only** with an inline `// JUSTIFY(budget): reason`
comment that the reviewer accepts; CI greps for unjustified overflows.

Rationale: budgets are a forcing function for decomposition. They catch the
"one file slowly becomes everything" failure before it happens.

## 2. Folder & crate structure

- One workspace, many small crates (docs/01 §2). Strict downward dependency
  graph, enforced by `cargo-deny`/a dependency lint in CI.
- Within a crate: `src/lib.rs` is thin (re-exports + module tree only, no
  logic). Each public concept gets its own module file. Tests live next to code
  (`#[cfg(test)] mod tests`) for units; integration tests in `tests/`.
- No `util`/`misc`/`common` dumping-ground modules. If something is shared, it
  belongs to a named concept in `core`.

## 3. Error handling & traceability

- Request-path crates: `#![deny(clippy::unwrap_used, clippy::expect_used,
  clippy::panic, clippy::todo, clippy::unimplemented)]`. Panics are a reliability
  bug (NFR-R1).
- All request-path errors are typed enums (`thiserror`) carrying `ErrorContext`
  (code, decision chain, retryable, remediation) — docs/02 §4. **No `anyhow` or
  string errors on the request path.** `anyhow` is permitted only in `xtask` and
  test helpers.
- Every error code is **stable and documented** in a generated error reference
  so an LLM/operator can look it up.

## 4. Documentation coverage

- `#![deny(missing_docs)]` on `core`, `spi`, `tenancy` (the public/contract
  surface). Every public item documents intent, invariants, and panics (none).
- Every SPI trait/method carries a runnable doc example (NFR-Q3). `cargo test
  --doc` runs them.
- Module-level docs (`//!`) state the module's single responsibility.

## 5. Lints & formatting

- `rustfmt` enforced; `clippy` at `--deny warnings` with a curated, documented
  allow-list (each allow justified in `clippy.toml` or inline).
- `cargo-deny`: license/advisory/dup-dependency checks in CI.
- `unsafe` is **forbidden** by default (`#![forbid(unsafe_code)]`); any
  exception requires a `// SAFETY:` proof and reviewer sign-off, and is confined
  to a single audited module.

### 5a. Background-task discipline

A library crate must not call bare `tokio::spawn`: it panics when invoked outside
a running runtime, which a library cannot assume. Background work in a library
captures a `tokio::runtime::Handle` and spawns on it (so a missing runtime is
handled, not assumed) — e.g. `osproxy-otlp`'s fire-and-forget span export. The
binary (`osproxy-server`) owns the runtime, and `osproxy-transport` spawns only
from inside its `async` accept loops where a runtime is guaranteed; both are
exempt. Enforced by `cargo xtask spawn`; a deliberate exception carries an inline
`// JUSTIFY(spawn): reason`.

## 6. Dependency hygiene

- Minimal, audited dependency set; new deps require justification in the PR
  (license, maintenance, supply-chain). `cargo-deny` advisory DB gates known
  vulns.
- Pin crypto-relevant deps (docs/07).

## 7. Naming & API conventions

- Follow the Rust API Guidelines (naming, conversions, `must_use`, error types).
- Types in the public surface are `#[non_exhaustive]` where future growth is
  expected (error enums, endpoint kinds) so additions aren't breaking.
- Newtypes for ids (`PartitionId`, `ClusterId`, `Epoch`) — no bare `String`/`u64`
  identifiers crossing API boundaries (prevents mix-ups, aids traceability).

## 8. Commits & change discipline

- Small, focused commits; each compiles and passes tests.
- Public SPI changes carry a design-review note (docs/10).
- CI must be green (lint + test + coverage + budgets) before merge.

## 9. CI gates (summary)

A PR merges only if **all** pass: build (fips + non-fips), `rustfmt`, `clippy
-D warnings`, `cargo-deny`, doc build + doc tests, unit + integration + property
tests, coverage thresholds (docs/09), size/complexity budgets, background-task
discipline (§5a), and the "no value leaks" telemetry test (docs/05 §7).
