---
name: architecture
description: "WHAT: Crate layout, the strict downward dependency graph, and no-god-module budgets. USE WHEN: adding or moving a crate/module/file, changing Cargo dependencies, or deciding where code belongs."
---

# Architecture & structure

osproxy is a workspace of small, single-responsibility crates with a **strictly
downward dependency graph**. This is the structural defense against a god
module: a change in one crate cannot reach into an unrelated one because both
only see `osproxy-core` types and `osproxy-spi` traits.

## Rules

- **Dependency direction is downward only.** `core` depends on nothing in the
  workspace; `spi` only on `core`; `tenancy` on `core`+`spi`. No crate depends
  "upward" on `engine`/`server` except `server`. Siblings (`rewrite`, `sink`,
  `transport`, `control`) never depend on each other — only through `core`/`spi`.
- **`core` and `spi` have zero I/O deps.** No async runtime, socket, or
  wire-serialization in them.
- **One responsibility per crate/module.** No `util`/`misc`/`common` dumping
  ground; shared things become named concepts in `core`.
- **Budgets** (docs/08 §1): files ≤400 lines (warn 300), fns ≤60 lines, ≤7
  args, complexity ≤15. Exceeding needs an inline `// JUSTIFY(<budget>): reason`.

## Enforced by

- `cargo xtask budgets` — file-length budget + `// JUSTIFY` check.
- clippy `too_many_lines` / `too_many_arguments` / `cognitive_complexity`
  (thresholds in `clippy.toml`), denied via `cargo xtask clippy`.
- Dependency direction: review + the workspace `Cargo.toml` graph (a
  cargo-deny/dep lint is added as the graph grows).

## Deep dive

[docs/01-architecture.md](../../../docs/01-architecture.md) (crate table, NFRs),
[docs/08-engineering-standards.md](../../../docs/08-engineering-standards.md),
ADR-007 (static SPI).
