# 07 — FIPS & Crypto

## 1. Decision

Rust with **`rustls` + `aws-lc-rs` (`fips` feature)** as the default crypto
provider. This is the only credible FIPS path in Rust and is the crypto backing
AWS's own Rust TLS stack.

Crypto sits behind the `CryptoProvider` trait (docs/02 §3) so the module is a
seam, not a hard dependency baked through the codebase.

## 2. What FIPS validation actually covers (and the caveats)

FIPS 140-3 validation covers the **crypto module** (AWS-LC-FIPS), **not** our
proxy. Our compliance obligations:

1. **Pin the validated version.** The `fips` feature builds against a specific
   AWS-LC-FIPS source version that appears on the CMVP certificate. We do **not**
   freely bump it; a crypto upgrade is a tracked compliance event, not a routine
   `cargo update`. The pinned version + CMVP cert number is recorded in
   [docs/specs/fips-boundary.md](specs/fips-boundary.md).
2. **Platform/tested configuration.** FIPS validation is specific to "tested
   configurations" (OS/arch). We must deploy on a configuration on the
   certificate's list. Target: standard Linux x86_64 / aarch64 — **must be
   verified against the live cert before release** (action item, docs/11).
3. **Cipher suite / version pinning.** The module being validated does not stop
   rustls from negotiating a non-approved suite. The FIPS `CryptoProvider`
   configures rustls to offer **only** FIPS-approved TLS versions (1.2/1.3
   approved suites) and rejects the rest (NFR-S5).
4. **Build toolchain.** The FIPS build compiles AWS-LC in FIPS mode (C toolchain
   + CMake). The build environment is captured and reproducible (pinned in the
   build/CI definition) so the produced binary's crypto provenance is auditable.

## 3. Build modes

- `--features fips` (default for release builds): aws-lc-rs FIPS provider, suite
  pinning on.
- `--features non-fips` (dev/local): aws-lc-rs or ring non-FIPS for faster local
  builds; **never shipped** to a FIPS deployment. CI gates that release artifacts
  are FIPS-built.

The two modes differ **only** behind the `CryptoProvider` seam; no request-path
code branches on FIPS.

## 4. The compliance boundary document

[docs/specs/fips-boundary.md](specs/fips-boundary.md) is the authoritative
artifact and must state:

- The exact CMVP certificate number and module name/version.
- The pinned aws-lc-rs / AWS-LC-FIPS version.
- The deployed platform(s) and their match to the cert's tested configurations.
- The exact set of TLS versions and cipher suites offered in FIPS mode.
- The cryptographic boundary diagram (what crosses the module boundary).

Audits fail on the boundary documentation, not the crate. This doc is a release
gate (docs/10).

## 5. Open verification item

Triple-check, before committing the FIPS claim to any customer: the **live CMVP
certificate** for the pinned AWS-LC-FIPS version and that our deploy OS/arch is
on its tested-configuration list. Tracked in docs/11 as a release blocker.
