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

The **suite** restriction is verified at negotiation on the `ring` build in
`crates/osproxy-transport/tests/tls.rs`:
`server_offers_only_the_fips_approved_suites` (the offered set equals the
approved set, CHACHA20 absent) and
`a_chacha20_only_client_is_refused_at_negotiation` (a live handshake offering
only CHACHA20 is rejected, with no fallback to a non-approved suite).

On the **FIPS build** itself, `crates/osproxy-transport/tests/fips.rs` (run by
`cargo xtask check-fips`, folded into `xtask ci` where the toolchain is present)
asserts the linked aws-lc-rs module reports FIPS mode (`fips_mode()`) and offers
exactly the approved suites — the count match catches a silent shrink if the FIPS
module lacked one of the six approved suites.

## 4. Build provenance

- FIPS build toolchain (C compiler, CMake versions) pinned in CI.
- Build is reproducible; artifact crypto provenance auditable.
- Release CI gates that shipped artifacts are `--features fips`.

## 5. Cryptographic boundary diagram

The **cryptographic boundary is the AWS-LC-FIPS module** (the CMVP-validated C
library) — *not* the proxy, *not* rustls, *not* the aws-lc-rs bindings. Every
cryptographic operation crosses into that module; nothing cryptographic happens
outside it. The proxy's own code does **zero** crypto: it produces and consumes
plaintext and drives the TLS protocol state machine, which in turn calls the
module for all key agreement, signing/verification, AEAD, hashing, and random.

```
   downstream client                                          upstream OpenSearch
        │  TLS ciphertext (wire)                                   ▲  TLS ciphertext
        ▼                                                          │
┌───────────────────────────────────────────────────────────────────────────────┐
│ osproxy process (NOT in the cryptographic boundary)                             │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ osproxy request pipeline — routing, tenancy, rewrite, sink   PLAINTEXT    │  │
│  │ only. Never sees keys or ciphertext. (osproxy-engine/-rewrite/-sink…)     │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│                          ▲ plaintext records                                    │
│                          ▼                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ rustls — TLS 1.2/1.3 PROTOCOL state machine (handshake orchestration,     │  │
│  │ record framing, suite/version policy = FIPS_APPROVED_SUITES/FIPS_VERSIONS).│ │
│  │ Holds no crypto of its own; delegates every primitive across the boundary. │ │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│                          │  FFI calls (aws-lc-rs → aws-lc-fips-sys, thin shim,  │
│                          ▼  no crypto of their own)                              │
│  ╔═══════════════════════════════════════════════════════════════════════════╗ │
│  ║ ▓▓▓ CRYPTOGRAPHIC BOUNDARY — AWS-LC-FIPS (CMVP-validated module) ▓▓▓       ║ │
│  ║   • ECDHE key agreement            • RSA/ECDSA signature verify            ║ │
│  ║   • AES-128/256-GCM AEAD (record encrypt / decrypt)                        ║ │
│  ║   • SHA-256/384 (handshake transcript + mTLS cert fingerprint)             ║ │
│  ║   • approved DRBG (random)                                                 ║ │
│  ║   Secret keys are GENERATED AND USED HERE and never cross back out.        ║ │
│  ╚═══════════════════════════════════════════════════════════════════════════╝ │
└───────────────────────────────────────────────────────────────────────────────┘
```

**What crosses the boundary (in → out):**

| Into the module | Out of the module |
|-----------------|-------------------|
| Plaintext TLS records to encrypt | Ciphertext records |
| Ciphertext TLS records to decrypt | Plaintext records |
| Peer key shares, certificates, transcript bytes | Shared secret / session keys (kept inside), verify pass/fail |
| Bytes to hash (transcript, cert DER) | Digest |
| Randomness requests | Random bytes |

**What never crosses out:** private keys, the ECDHE shared secret, and derived
session/traffic keys — they are created and consumed inside the module. The proxy
holds only plaintext payloads and opaque handles; a memory disclosure in proxy
code cannot leak key material, because key material is never in proxy code.

The mTLS client **fingerprint** is a SHA-256 of the peer certificate DER computed
*through the module* (`cert_fingerprint`, cfg-selected to aws-lc-rs in the FIPS
build), so even that incidental hash stays inside the boundary — no non-validated
crypto is linked into a FIPS artifact (enforced by the mutually-exclusive build
features, ADR-009).

## 6. Sign-off

- [ ] CMVP cert verified live.
- [ ] Platform match verified.
- [x] Suite pinning test green (`osproxy-transport` tls tests, see §3).
- [ ] Boundary diagram reviewed.
