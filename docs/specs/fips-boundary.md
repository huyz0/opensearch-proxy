# FIPS Compliance Boundary

> Status: **release blocker** — the module the build links (AWS-LC-FIPS 3.0) is
> on the CMVP *Modules In Process* list; its certificate is not yet awarded, so a
> hard "FIPS 140-3 validated" claim is not defensible until either 3.0's review
> completes or the build is moved to a validated line (§1a). The proxy
> engineering is complete; this is the compliance gate (docs/07 §5, docs/11 M6).

This is the authoritative compliance artifact. Audits are passed or failed on
this document, not on the crate.

**We do not validate anything with NIST.** AWS already had AWS-LC-FIPS validated
by an accredited CMVP lab; as a *consumer* of that module our obligation is only
to (a) pin to a module version with an awarded certificate, (b) confirm our
deploy OS/arch is on that certificate's tested-configuration list, and (c) record
it here. No NIST engagement, no lab, no fees.

## 1. Module the build links (pinned)

| Field | Value |
|-------|-------|
| Module name | AWS-LC-FIPS |
| Module version | **3.0** (`fips-2024-09-27`) |
| CMVP certificate # | **none yet** — on the CMVP *Modules In Process* (MIP) list, "Review Pending" |
| FIPS standard | 140-3, Level 1 (target) |
| Pinned `aws-lc-rs` crate (`fips` feature) | **=1.17.0** (binds AWS-LC-FIPS 3.0.x) |
| Pinned `aws-lc-fips-sys` | 0.13.14 (transitive, via the lockfile) |

The crate version is **pinned exactly** in the workspace `Cargo.toml`; bumping it
changes the linked module and is a tracked compliance event, not a routine
`cargo update`.

### 1a. Validated fallback line (if an awarded cert is required now)

The module line is **coupled to rustls**: `rustls 0.23.40` requires
`aws-lc-rs >= 1.14`, which is the 3.0 line. The most recent **validated** module
is AWS-LC-FIPS **2.0**:

| Module | CMVP cert | Reached by |
|--------|-----------|------------|
| AWS-LC-FIPS 2.0 static (Linux) | **#4816** | `aws-lc-rs < 1.12` |
| AWS-LC-FIPS 2.0 dynamic | **#4759** | `aws-lc-rs < 1.12` |
| AWS-LC-FIPS 1.0 | #4631 | older still |

Moving to 2.0 therefore also pins `rustls`/`tokio-rustls` back to a release that
accepts `aws-lc-rs < 1.12` — a TLS-stack currency vs awarded-cert tradeoff. Until
that is needed, the build tracks 3.0 and inherits its certificate automatically
once the review completes.

## 2. Tested configurations (platform match)

Static AWS-LC-FIPS builds (what this artifact uses) are **Linux-only**. The
certificate's Security Policy lists the specific tested OS/arch operational
environments; our deploy targets must appear there (verify against the awarded
certificate once 3.0 is on the active list, or against #4816 if the 2.0 line is
chosen).

| Deploy target | On cert's tested-config list? |
|---------------|-------------------------------|
| Linux x86_64 | `[VERIFY against the awarded cert's Security Policy]` |
| Linux aarch64 | `[VERIFY against the awarded cert's Security Policy]` |
| Windows / macOS | Not supported for static FIPS builds |

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
