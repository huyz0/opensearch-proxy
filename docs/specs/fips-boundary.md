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

The `CryptoProvider` (FIPS) configures rustls to offer ONLY approved suites:

| TLS version | Approved suites offered |
|-------------|-------------------------|
| TLS 1.3 | `[FILL: approved suites]` |
| TLS 1.2 | `[FILL: approved suites]` |

Everything else is refused at negotiation (NFR-S5). Verified by a TLS-negotiation
test asserting non-approved suites are rejected.

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
- [ ] Suite pinning test green.
- [ ] Boundary diagram reviewed.
