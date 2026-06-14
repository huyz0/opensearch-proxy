---
name: spi
description: "WHAT: The public SPI contract (RoutingSpi, TenancySpi, Sink, CryptoProvider) and its invariants. USE WHEN: editing crates osproxy-spi or osproxy-tenancy, or changing any public trait/type an implementer compiles against."
---

# SPI contract

The SPI is the contract between the proxy and the implementer. It is the most
important documented surface in the project; treat changes to it as design
events.

## Rules

- **Two layers.** Low-level `RoutingSpi` (full control, returns `RouteDecision`)
  and high-level `TenancySpi` (declare rules; `osproxy-tenancy` turns them into a
  `RoutingSpi`). Most implementers only touch `TenancySpi`.
- **Document everything.** Every public item on `core`/`spi`/`tenancy` has doc
  comments stating intent, invariants, panics (none), and a runnable example
  (`#![deny(missing_docs)]` is on).
- **Typed errors only.** SPI methods return typed errors carrying `ErrorContext`
  (code, decision chain, retryable, remediation) — never `anyhow`/strings, never
  a panic.
- **Resolve to exactly one target** (no synchronous fan-out — ADR-002). Stamp
  the `PlacementEpoch` the decision was read at (sink rejects stale — docs/06).
- **Stability.** Public SPI types are `#[non_exhaustive]` where growth is
  expected. A breaking SPI change needs an ADR (docs/10).

## Enforced by

- `#![deny(missing_docs)]` on `core`/`spi`/`tenancy`; doc examples via
  `cargo test --doc` (in `cargo xtask doc`).
- clippy deny of `unwrap`/`expect`/`panic` on the request path.
- Review: SPI change → ADR in `docs/decisions/`.

## Deep dive

[docs/02-spi.md](../../../docs/02-spi.md) (the trait reference + error taxonomy +
endpoint matrix).
