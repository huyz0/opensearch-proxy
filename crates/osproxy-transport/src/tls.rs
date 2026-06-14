//! The TLS crypto seam.
//!
//! [`CryptoProvider`] is the abstraction the rest of the proxy programs against
//! for TLS (`docs/02` §3, `docs/07`). M1 ships [`RingProvider`], built on
//! rustls's pure-Rust `ring` provider — no native toolchain needed. The
//! FIPS-validated aws-lc-rs provider implements the same trait at M6 behind this
//! seam (ADR-009); request-path and server code never name a concrete provider.

use std::sync::Arc;

use thiserror::Error;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;

/// The pluggable TLS backend.
///
/// Hands out a ready [`ServerConfig`] for terminating downstream TLS and reports
/// whether the backend is operating in a FIPS-validated mode (always `false`
/// until the aws-lc-rs provider lands in M6).
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

/// A [`CryptoProvider`] backed by rustls's `ring` crypto provider.
///
/// Server-authentication only in M1 (no client-certificate verification); mTLS
/// client auth attaches here without changing the seam.
#[derive(Debug, Clone)]
pub struct RingProvider {
    server_config: Arc<ServerConfig>,
}

impl RingProvider {
    /// Builds a server-authentication-only provider from a PEM certificate chain
    /// and private key (no client-certificate verification).
    ///
    /// # Errors
    ///
    /// Returns [`TlsError`] if the PEM cannot be parsed or rustls rejects the
    /// certificate/key pair.
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self, TlsError> {
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let builder = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::Config(e.to_string()))?
            .with_no_client_auth();
        Self::finish(builder, cert_pem, key_pem)
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
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());

        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        for ca in parse_certs(client_ca_pem)? {
            roots.add(ca).map_err(|e| TlsError::Config(e.to_string()))?;
        }
        let verifier = tokio_rustls::rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .map_err(|e| TlsError::Config(e.to_string()))?;

        let builder = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::Config(e.to_string()))?
            .with_client_cert_verifier(verifier);
        Self::finish(builder, cert_pem, key_pem)
    }

    /// Completes a [`ServerConfig`] from a verifier-configured builder plus the
    /// server's own certificate and key.
    fn finish(
        builder: tokio_rustls::rustls::ConfigBuilder<
            ServerConfig,
            tokio_rustls::rustls::server::WantsServerCert,
        >,
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<Self, TlsError> {
        let certs = parse_certs(cert_pem)?;
        let key = parse_key(key_pem)?;
        let mut config = builder
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(e.to_string()))?;
        // Advertise HTTP/2 (preferred) then HTTP/1.1 via ALPN so a TLS client can
        // negotiate h2; the auto ingress builder serves whichever is selected.
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            server_config: Arc::new(config),
        })
    }
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
    let digest = ring::digest::digest(&ring::digest::SHA256, der);
    let mut out = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    out
}

impl CryptoProvider for RingProvider {
    fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.server_config)
    }

    fn fips_mode(&self) -> bool {
        // ring is not a FIPS module; FIPS arrives with aws-lc-rs at M6 (ADR-009).
        false
    }
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
