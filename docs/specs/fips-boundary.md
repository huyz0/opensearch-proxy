# FIPS Compliance Boundary

> Status: **skeleton / release blocker** — must be completed and verified before
> any FIPS claim (docs/07 §5, docs/11 M6).

This is the authoritative compliance artifact. Audits are passed or failed on
this document, not on the crate.

## 1. Validated module

| Field | Value |
|-------|-------|
| Module name | AWS-LC-FIPS `[VERIFY]` |
| CMVP certificate # | `[VERIFY against NIST CMVP list]` |
| FIPS standard | 140-3 `[VERIFY]` |
| Pinned AWS-LC-FIPS source version | `[PIN]` |
| `aws-lc-rs` crate version (`fips` feature) | `[PIN]` |

## 2. Tested configurations (platform match)

The certificate lists specific tested OS/arch configurations. Our deployment
targets must appear there.

| Deploy target | On cert's tested-config list? |
|---------------|-------------------------------|
| Linux x86_64 | `[VERIFY]` |
| Linux aarch64 | `[VERIFY]` |

If a target is not listed, the FIPS validation claim does not hold for it.

## 3. TLS versions & cipher suites offered in FIPS mode

Suite/version pinning is a **config-layer** control (ADR-004 caveat #3), applied
to *every* provider regardless of the backing module, so it is implemented and
tested without the FIPS toolchain. It lives in `osproxy-transport::tls`
(`FIPS_APPROVED_SUITES`, `FIPS_VERSIONS`); the FIPS provider pins the identical
list (suites are keyed on the provider-independent `rustls::CipherSuite` id).

| TLS version | Approved suites offered |
|-------------|-------------------------|
| TLS 1.3 | `TLS_AES_128_GCM_SHA256`, `TLS_AES_256_GCM_SHA384` |
| TLS 1.2 | `TLS_ECDHE_{ECDSA,RSA}_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_{ECDSA,RSA}_WITH_AES_256_GCM_SHA384` |

`CHACHA20-POLY1305` (all versions) is excluded — not FIPS-approved. Versions are
pinned to TLS 1.2/1.3 via `FIPS_VERSIONS` (`with_protocol_versions`); the rustls
build in the tree ships only 1.2/1.3, so the pin is an explicit, future-proof
constraint by construction rather than a handshake-tested refusal of older
versions.

The **suite** restriction is verified at negotiation in
`crates/osproxy-transport/tests/tls.rs`:
`server_offers_only_the_fips_approved_suites` (the offered set equals the
approved set, CHACHA20 absent) and
`a_chacha20_only_client_is_refused_at_negotiation` (a live handshake offering
only CHACHA20 is rejected, with no fallback to a non-approved suite).

## 4. Build provenance

- FIPS build toolchain (C compiler, CMake versions) pinned in CI.
- Build is reproducible; artifact crypto provenance auditable.
- Release CI gates that shipped artifacts are `--features fips`.

## 5. Cryptographic boundary diagram

`[DIAGRAM: what data crosses the module boundary — TLS handshake, record
encryption/decryption — and what stays outside]`

## 6. Sign-off

- [ ] CMVP cert verified live.
- [ ] Platform match verified.
- [x] Suite pinning test green (`osproxy-transport` tls tests, see §3).
- [ ] Boundary diagram reviewed.
