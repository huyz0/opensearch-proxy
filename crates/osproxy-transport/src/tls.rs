//! The TLS crypto seam.
//!
//! [`CryptoProvider`] is the abstraction the rest of the proxy programs against
//! for TLS (`docs/02` §3, `docs/07`). The concrete backend is chosen **at build
//! time** by a mutually-exclusive crate feature, so a FIPS artifact never links a
//! non-validated crypto crate (and vice versa) — this is a separate compiled
//! binary, not a runtime switch (ADR-009, ADR-004):
//!
//! - **`non-fips`** (default): `RingProvider`, rustls's pure-Rust `ring`
//!   provider — no native toolchain, fast local/dev builds. `fips_mode()` is
//!   `false`; nothing may claim FIPS.
//! - **`fips`**: `AwsLcFipsProvider`, the CMVP-validated aws-lc-rs module
//!   (builds AWS-LC-FIPS via cmake + C toolchain + Go). `fips_mode()` is `true`.
//!
//! Both implement the same trait and produce an interchangeable [`ServerConfig`],
//! so request-path and server code never name a concrete provider — they use the
//! [`DefaultCryptoProvider`](crate::DefaultCryptoProvider) alias the active
//! feature resolves. No request-path code branches on FIPS.
//!
//! Independently of which module backs it, every provider pins the wire policy
//! to the FIPS-approved set ([`FIPS_APPROVED_SUITES`], TLS 1.2/1.3 — ADR-004
//! caveat #3, NFR-S5): the module's validation is what differs between `ring` and
//! aws-lc-rs FIPS, not the suites offered, so the suite/version restriction lives
//! here in the config layer and is testable without the FIPS toolchain.

use std::sync::Arc;

use thiserror::Error;
use tokio_rustls::rustls::crypto::CryptoProvider as RustlsCryptoProvider;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::version::{TLS12, TLS13};
use tokio_rustls::rustls::{CipherSuite, ServerConfig, SupportedProtocolVersion};

/// The FIPS-approved TLS cipher suites the proxy offers (`docs/07` §2 caveat 3,
/// NFR-S5):
/// TLS 1.3 and TLS 1.2 AES-GCM only. CHACHA20-POLY1305 is deliberately excluded —
/// it is not a FIPS-approved suite. This wire policy is applied to *every*
/// provider, FIPS-validated or not, so the suites negotiated are identical
/// regardless of the underlying module; the FIPS module changes validation, not
/// the suites on the wire. The set is keyed on the provider-independent
/// [`CipherSuite`] identifier so the aws-lc-rs provider pins the exact same list.
pub const FIPS_APPROVED_SUITES: &[CipherSuite] = &[
    CipherSuite::TLS13_AES_128_GCM_SHA256,
    CipherSuite::TLS13_AES_256_GCM_SHA384,
    CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
    CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
    CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
];

/// The FIPS-approved TLS protocol versions: 1.2 and 1.3 only (NFR-S5). Older
/// versions are refused at negotiation.
const FIPS_VERSIONS: &[&SupportedProtocolVersion] = &[&TLS13, &TLS12];

/// The pluggable TLS backend.
///
/// Hands out a ready [`ServerConfig`] for terminating downstream TLS and reports
/// whether the backend is operating in a FIPS-validated mode (provider-dependent:
/// `false` for the `ring` build, `true` for the aws-lc-rs FIPS build).
pub trait CryptoProvider: Send + Sync + 'static {
    /// The server-side TLS configuration for terminating client connections.
    fn server_config(&self) -> Arc<ServerConfig>;

    /// Whether the backend is a FIPS-validated module in FIPS mode.
    fn fips_mode(&self) -> bool;
}

/// A failure building a [`RingProvider`] from PEM material.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum TlsError {
    /// The certificate or key PEM could not be parsed.
    #[error("invalid PEM: {0}")]
    Pem(&'static str),
    /// No private key was found in the key PEM.
    #[error("no private key found")]
    NoKey,
    /// rustls rejected the certificate/key configuration.
    #[error("rustls configuration error: {0}")]
    Config(String),
}

/// A [`CryptoProvider`] backed by rustls's pure-Rust `ring` module
/// (`non-fips` feature). Not FIPS-validated — `fips_mode()` is always `false`.
/// Server-auth and mutual-TLS, built from PEM.
#[cfg(feature = "non-fips")]
#[derive(Debug, Clone)]
pub struct RingProvider {
    server_config: Arc<ServerConfig>,
}

#[cfg(feature = "non-fips")]
impl RingProvider {
    /// Builds a server-authentication-only provider from a PEM certificate chain
    /// and private key (no client-certificate verification).
    ///
    /// # Errors
    ///
    /// Returns [`TlsError`] if the PEM cannot be parsed or rustls rejects the
    /// certificate/key pair.
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, TlsError> {
        let base = tokio_rustls::rustls::crypto::ring::default_provider();
        Ok(Self {
            server_config: build_server_config(base, cert_pem, key_pem, None)?,
        })
    }

    /// Builds a mutual-TLS provider: clients must present a certificate that
    /// chains to a root in `client_ca_pem`, and the verified identity is exposed
    /// to the handler.
    ///
    /// # Errors
    ///
    /// Returns [`TlsError`] if any PEM cannot be parsed or rustls rejects the
    /// verifier or certificate/key pair.
    pub fn from_pem_mtls(
        cert_pem: &[u8],
        key_pem: &[u8],
        client_ca_pem: &[u8],
    ) -> Result<Self, TlsError> {
        let base = tokio_rustls::rustls::crypto::ring::default_provider();
        Ok(Self {
            server_config: build_server_config(base, cert_pem, key_pem, Some(client_ca_pem))?,
        })
    }
}

#[cfg(feature = "non-fips")]
impl CryptoProvider for RingProvider {
    fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.server_config)
    }

    fn fips_mode(&self) -> bool {
        // ring is not a FIPS module; FIPS requires the `fips` build (aws-lc-rs).
        false
    }
}

/// A [`CryptoProvider`] backed by the CMVP-validated **aws-lc-rs** module in FIPS
/// mode (`fips` feature). The wire policy and PEM handling are identical to
/// [`RingProvider`] — only the underlying validated module differs (ADR-004).
#[cfg(feature = "fips")]
#[derive(Debug, Clone)]
pub struct AwsLcFipsProvider {
    server_config: Arc<ServerConfig>,
}

#[cfg(feature = "fips")]
impl AwsLcFipsProvider {
    /// Builds a server-authentication-only FIPS provider from PEM.
    ///
    /// # Errors
    ///
    /// Returns [`TlsError`] if the PEM cannot be parsed or rustls rejects the
    /// certificate/key pair.
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, TlsError> {
        Ok(Self {
            server_config: build_server_config(fips_base()?, cert_pem, key_pem, None)?,
        })
    }

    /// Builds a mutual-TLS FIPS provider; clients must present a certificate
    /// chaining to a root in `client_ca_pem`.
    ///
    /// # Errors
    ///
    /// Returns [`TlsError`] if any PEM cannot be parsed or rustls rejects the
    /// verifier or certificate/key pair.
    pub fn from_pem_mtls(
        cert_pem: &[u8],
        key_pem: &[u8],
        client_ca_pem: &[u8],
    ) -> Result<Self, TlsError> {
        Ok(Self {
            server_config: build_server_config(
                fips_base()?,
                cert_pem,
                key_pem,
                Some(client_ca_pem),
            )?,
        })
    }
}

/// The aws-lc-rs base provider, but only if the linked module is **actually** in
/// FIPS mode. A FIPS artifact built without AWS-LC-FIPS truly engaging would
/// otherwise link, report `fips_mode() == false`, and ship silently — so we fail
/// loudly at construction instead (the provider's FIPS status is a build fact, so
/// this fails fast on the very first TLS provider built).
#[cfg(feature = "fips")]
fn fips_base() -> Result<RustlsCryptoProvider, TlsError> {
    let base = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
    if base.fips() {
        Ok(base)
    } else {
        Err(TlsError::Config(
            "aws-lc-rs is not operating in FIPS mode: the `fips` build did not engage \
             AWS-LC-FIPS (check the AWS-LC-FIPS toolchain and that the `fips` feature \
             is active)"
                .to_owned(),
        ))
    }
}

#[cfg(feature = "fips")]
impl CryptoProvider for AwsLcFipsProvider {
    fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.server_config)
    }

    fn fips_mode(&self) -> bool {
        // Report what the linked module actually is, not a build-time assumption:
        // a true FIPS module answers `true` here.
        self.server_config.crypto_provider().fips()
    }
}

/// Builds a downstream [`ServerConfig`] from a base crypto provider plus PEM
/// material, applying the shared policy every provider obeys: cipher suites
/// pinned to [`FIPS_APPROVED_SUITES`], versions to [`FIPS_VERSIONS`], optional
/// mutual-TLS client verification, and h2/http-1.1 ALPN. The only thing that
/// varies between the `ring` and aws-lc-rs builds is the `base` passed in.
fn build_server_config(
    base: RustlsCryptoProvider,
    cert_pem: &[u8],
    key_pem: &[u8],
    client_ca_pem: Option<&[u8]>,
) -> Result<Arc<ServerConfig>, TlsError> {
    let provider = fips_pinned_provider(base);
    let versions = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(FIPS_VERSIONS)
        .map_err(|e| TlsError::Config(e.to_string()))?;
    let builder = match client_ca_pem {
        None => versions.with_no_client_auth(),
        Some(ca_pem) => {
            let mut roots = tokio_rustls::rustls::RootCertStore::empty();
            for ca in parse_certs(ca_pem)? {
                roots.add(ca).map_err(|e| TlsError::Config(e.to_string()))?;
            }
            let verifier =
                tokio_rustls::rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                )
                .build()
                .map_err(|e| TlsError::Config(e.to_string()))?;
            versions.with_client_cert_verifier(verifier)
        }
    };
    let certs = parse_certs(cert_pem)?;
    let key = parse_key(key_pem)?;
    let mut config = builder
        .with_single_cert(certs, key)
        .map_err(|e| TlsError::Config(e.to_string()))?;
    // Advertise HTTP/2 (preferred) then HTTP/1.1 via ALPN so a TLS client can
    // negotiate h2; the auto ingress builder serves whichever is selected.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// A crypto provider with its cipher-suite list filtered to the FIPS-approved set
/// ([`FIPS_APPROVED_SUITES`]). Keyed on the provider-independent [`CipherSuite`]
/// id, so both the `ring` and aws-lc-rs bases pin the identical list — only the
/// validated module differs, not the wire policy.
fn fips_pinned_provider(base: RustlsCryptoProvider) -> Arc<RustlsCryptoProvider> {
    let cipher_suites = base
        .cipher_suites
        .iter()
        .copied()
        .filter(|cs| FIPS_APPROVED_SUITES.contains(&cs.suite()))
        .collect();
    Arc::new(RustlsCryptoProvider {
        cipher_suites,
        ..base
    })
}

/// The verified mTLS client identity from a completed handshake, if the peer
/// presented a certificate: a stable `cert:{fingerprint}` id, never the
/// certificate material. Shared by the HTTP and gRPC ingress paths.
#[must_use]
pub(crate) fn client_subject_from_tls(
    tls: &tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> Option<String> {
    let (_, conn) = tls.get_ref();
    conn.peer_certificates()
        .and_then(<[_]>::first)
        .map(|cert| format!("cert:{}", cert_fingerprint(cert.as_ref())))
}

/// A stable identity for a verified client certificate: the lowercase hex
/// SHA-256 fingerprint of its DER. Not the certificate material, so it is safe
/// to carry as an id and surface in telemetry.
#[must_use]
pub(crate) fn cert_fingerprint(der: &[u8]) -> String {
    // Hash with whichever validated module the build linked, so even the cert
    // fingerprint stays on the FIPS module in a FIPS build (ring and aws-lc-rs
    // share this `digest` API).
    #[cfg(feature = "non-fips")]
    let digest = ring::digest::digest(&ring::digest::SHA256, der);
    #[cfg(feature = "fips")]
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, der);
    let mut out = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    out
}

/// Parses a PEM certificate chain into DER certificates, via the
/// `rustls-pki-types` `PemObject` API (the maintained successor to
/// `rustls-pemfile`).
fn parse_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs: Result<Vec<_>, _> = CertificateDer::pem_slice_iter(pem).collect();
    let certs = certs.map_err(|_| TlsError::Pem("certificate"))?;
    if certs.is_empty() {
        return Err(TlsError::Pem("certificate (none found)"));
    }
    Ok(certs)
}

/// Parses the first private key (PKCS#8, PKCS#1, or SEC1) from PEM.
fn parse_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, TlsError> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|_| TlsError::NoKey)
}
