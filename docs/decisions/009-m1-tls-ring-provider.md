# ADR-009: M1 TLS uses the `ring` provider; aws-lc-rs/FIPS at M6 behind the seam

**Status:** Accepted

## Context

TLS sits behind a `CryptoProvider` seam (docs/02 §3, docs/07). The FIPS story
requires the CMVP-validated **aws-lc-rs** module (ADR-004). But aws-lc-rs builds
its native AWS-LC via CMake + a C toolchain, which is not present on every
developer machine; the pre-commit hook builds the whole workspace, so a missing
cmake would block local commits. FIPS *hardening* is scheduled for M6 (docs/11),
and the M1 roadmap explicitly allows non-FIPS TLS.

## Decision

M1 implements TLS with rustls's **`ring`** crypto provider (pure-Rust build, no
cmake/C toolchain) behind the `CryptoProvider` trait. The FIPS-validated
aws-lc-rs provider implements the *same* trait at M6 and is selected by build
configuration; no request-path or transport code changes when it is swapped in.

## Why

- Keeps local builds and CI green with no native toolchain dependency.
- The `CryptoProvider` seam is exactly the abstraction that makes the provider a
  swap, not a rewrite, so deferring aws-lc-rs costs nothing structurally.
- Aligns with the roadmap: FIPS hardening (live CMVP cert + platform validation)
  is an M6 release blocker, not an M1 concern.

## Consequences

- `RingProvider::fips_mode()` returns `false`; nothing in M1 may claim FIPS.
- M6 adds an `AwsLcFipsProvider` (feature-gated) implementing `CryptoProvider`
  with `fips_mode() == true`, plus the CMake/C build in CI for that path.
- The two providers must produce interchangeable `rustls::ServerConfig`/
  `ClientConfig`, so the seam stays the only coupling point.
