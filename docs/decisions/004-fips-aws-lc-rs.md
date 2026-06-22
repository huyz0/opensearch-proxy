# ADR-004: FIPS via rustls + aws-lc-rs, crypto behind a trait

**Status:** Accepted (with a live-verification release blocker)

## Context

FIPS is a hard, ship-now requirement. Rust's only credible FIPS-TLS path is
`rustls` + `aws-lc-rs` with the `fips` feature, backed by the CMVP-validated
AWS-LC-FIPS module. `ring` is not FIPS validated. Go has a more familiar FIPS
story but no better boundary than aws-lc-rs at this point.

## Decision

Use **rustls + aws-lc-rs (`fips`)** as the default release crypto provider, with
TLS suite/version pinning to the FIPS-approved set. Place crypto behind a
`CryptoProvider` trait so the module is a seam; no request-path code branches on
FIPS.

## Why

- Only credible Rust FIPS path; keeps ADR-001 (Rust).
- The seam allows a non-FIPS provider for fast local/dev builds without touching
  the request path.
- The validation boundary is equivalent to Go's; familiarity alone didn't justify
  abandoning Rust's other advantages.

## Caveats / obligations (compliance is on us, not the crate)

1. Pin the validated AWS-LC-FIPS version (cert-listed); upgrades are compliance
   events, not routine.
2. Deploy only on a CMVP "tested configuration" platform, **verify live**.
3. Offer only approved TLS versions/suites (config layer, not the module).
4. Pinned, reproducible FIPS build toolchain.

## Consequences

- A release blocker: verify the live CMVP cert number and platform match
  (docs/07 §5, docs/specs/fips-boundary.md, docs/11 M6).
- Compliance boundary documentation is a release gate.
