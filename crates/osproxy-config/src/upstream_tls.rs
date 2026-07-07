//! TLS-to-upstream settings.

/// TLS-to-upstream settings: PEM file **paths**, mirroring [`TlsConfig`](crate::TlsConfig)
/// for the client (sink) side. `ca_path` is required (rustls trusts nothing
/// implicitly); `cert_path`/`key_path` are an optional pair for mutual TLS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamTlsConfig {
    /// Path to the trust-anchor bundle PEM the upstream's certificate must
    /// chain to.
    pub ca_path: String,
    /// Path to the client certificate chain PEM, for mutual TLS to the
    /// upstream. Both-or-neither with `key_path`.
    pub cert_path: Option<String>,
    /// Path to the client private key PEM, for mutual TLS to the upstream.
    pub key_path: Option<String>,
}
