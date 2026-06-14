---
name: fips
description: "WHAT: The FIPS crypto path (rustls + aws-lc-rs) and the compliance boundary obligations. USE WHEN: touching TLS, the CryptoProvider, cipher-suite config, the fips build feature, or docs/specs/fips-boundary.md."
---

# FIPS & crypto

FIPS is a hard, ship-now requirement. The crypto module sits behind the
`CryptoProvider` trait so no request-path code branches on FIPS.

## Rules

- **Default release crypto is rustls + aws-lc-rs (`fips` feature)** — the only
  credible Rust FIPS-TLS path (ADR-004). `ring` is not FIPS-validated; do not use
  it for the FIPS build.
- **Validation covers the module, not the proxy.** Obligations are ours:
  1. Pin the CMVP-validated AWS-LC-FIPS version; upgrades are compliance events,
     not routine `cargo update`.
  2. Deploy only on a cert "tested configuration" platform.
  3. Offer only FIPS-approved TLS versions/suites at the rustls layer.
  4. Pinned, reproducible FIPS build toolchain.
- **The boundary doc is the audit artifact** — keep
  `docs/specs/fips-boundary.md` current; it is a release gate.

## Enforced by

- CI builds both `--features fips` and `--features non-fips`; release artifacts
  must be FIPS-built.
- TLS-negotiation test asserting non-approved suites are refused (NFR-S5).
- Release checklist: verify live CMVP cert + platform (docs/07 §5).

## Deep dive

[docs/07-fips-and-crypto.md](../../../docs/07-fips-and-crypto.md),
[docs/specs/fips-boundary.md](../../../docs/specs/fips-boundary.md), ADR-004.
