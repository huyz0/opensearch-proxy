# AGENTS.md — guidance for AI agents working in this repo

This project is built largely by LLMs. Read this before changing anything.

## What this is

A high-performance OpenSearch routing proxy (Rust library + binary). Routes each
request to one physical placement based on a partition-based placement policy.
Full design is in [docs/](docs/) — **the docs are the source of truth.**

## Read first (in order)

1. [docs/00-goals.md](docs/00-goals.md) — goal, scope, non-goals.
2. [docs/01-architecture.md](docs/01-architecture.md) — crates, **NFRs**.
3. [docs/02-spi.md](docs/02-spi.md) — the SPI contract (the most important surface).
4. [docs/decisions/](docs/decisions/) — ADRs: *why* things are the way they are.

## Non-negotiable standards (enforced in CI)

- **No god module/file/type.** Size & complexity budgets — [docs/08](docs/08-engineering-standards.md).
- **No panics / no `anyhow` on the request path.** Every failure is a typed,
  contextual error carrying the decision chain — [docs/02 §4](docs/02-spi.md).
- **≥90% semantic coverage** (SPI/routing core higher); meaningful assertions, not
  just lines — [docs/09](docs/09-testing-and-quality.md).
- **No value/secret in any log or trace, ever.** Observability is shape-only and
  read-only — [docs/05](docs/05-observability.md), ADR-005.
- **Every public SPI item documented** with intent, invariants, example.
- `#![forbid(unsafe_code)]` by default.

## Before you code

- A change to the SPI, placement/epoch model, observability schema, FIPS
  boundary, or any NFR needs an **ADR** in `docs/decisions/` — [docs/10](docs/10-review-process.md).
- Follow the milestone order in [docs/11-roadmap.md](docs/11-roadmap.md). The first
  slice is M1 (single-doc ingest).

## Before you open a PR

- Run the full gate (when `xtask` exists): `cargo xtask ci`.
- Self-review against the checklist in [docs/10](docs/10-review-process.md).
- Update the relevant `docs/` in the **same** change.
- If you added a failure mode, extend the blind-diagnosis test ([docs/09 §3](docs/09-testing-and-quality.md)).

## Key invariants you must not break

- One partition lives in exactly one placement at any instant (ADR-002, ADR-003).
- Write-inject and read-strip are inverse (round-trip symmetry) — [docs/09 §2](docs/09-testing-and-quality.md).
- Read isolation is provably filtered or the request is rejected — ADR-006.
- No write commits against a stale epoch for a migrating partition — [docs/06](docs/06-partition-migration.md).
