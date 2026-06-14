# ADR-001 — Language: Rust

**Status:** Accepted

## Context

We need very high performance, low resource usage, low latency, a library/SPI
consumable form, and a FIPS-capable crypto build. The user's preference was Rust,
with Go acceptable only if FIPS had no good Rust option.

## Options

- **Rust** — low footprint, no GC pauses, strong type system for typed errors;
  FIPS path via aws-lc-rs.
- **Go** — mature FIPS story (BoringCrypto / Go 1.24 FIPS), institutional
  familiarity; GC pauses, larger footprint, weaker compile-time error typing.

## Decision

**Rust.** A credible FIPS path exists (ADR-004), removing the only reason to fall
back to Go. Rust's zero-GC latency profile and type system directly serve the
performance and "every failure is a typed, contextual error" requirements.

## Why

- Latency predictability (no GC) → NFR-P.
- Footprint → low-resource requirement.
- `Result`/enums + `#![forbid(unsafe_code)]` + deny-panic lints → reliability &
  traceability NFRs are enforceable at compile time, not by convention.
- FIPS no longer forces Go (ADR-004).

## Consequences

- Crypto module build complexity (C toolchain for aws-lc-rs FIPS) — accepted,
  pinned in CI (docs/07).
- Team must be comfortable in Rust for an LLM-driven build with high quality bars.
