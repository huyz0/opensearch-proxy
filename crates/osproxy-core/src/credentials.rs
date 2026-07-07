//! The proxy's own upstream credential for a cluster.
//!
//! Distinct from whatever authenticated the client to the proxy itself
//! (`docs/13`): a multi-tenant proxy funnels many tenants onto shared
//! placements, so it needs one identity with privileges spanning all of
//! them, not each client's own scoped-down credential passed through.

use base64::Engine as _;

/// A single header the sink attaches to every upstream call for one cluster.
///
/// A generic header rather than a closed Basic/Bearer/`ApiKey` taxonomy:
/// OpenSearch's security plugin (and any auth-aware proxy sitting in front of
/// it) is header-based regardless of scheme, so this covers all of them, plus
/// anything an operator's own setup expects, without hardcoding assumptions
/// about which scheme is in play.
///
/// Supplied per cluster by [`crate::ClusterId`]-keyed lookups on the tenancy
/// SPI (`osproxy-spi`'s `TenancySpi::upstream_credentials`), resolved fresh on
/// every route (never cached), so a credential that rotates (a refreshed
/// access token, a short-lived STS-style secret) is naturally supported
/// without extra API surface — the implementer's own lookup does the
/// caching/refresh if it needs any.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UpstreamCredentials {
    /// The header to set (usually `Authorization`).
    pub header_name: String,
    /// The header's value, already in wire form (e.g. `Basic <base64>`,
    /// `Bearer <token>`, or a custom scheme).
    pub header_value: String,
}

impl UpstreamCredentials {
    /// A credential carried on an arbitrary header (builder-free: both parts
    /// are already in wire form).
    #[must_use]
    pub fn new(header_name: impl Into<String>, header_value: impl Into<String>) -> Self {
        Self {
            header_name: header_name.into(),
            header_value: header_value.into(),
        }
    }

    /// `Authorization: Basic <base64(user:password)>`.
    #[must_use]
    pub fn basic(username: &str, password: &str) -> Self {
        let token =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        Self::new("Authorization", format!("Basic {token}"))
    }

    /// `Authorization: Bearer <token>`.
    #[must_use]
    pub fn bearer(token: impl Into<String>) -> Self {
        Self::new("Authorization", format!("Bearer {}", token.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_encodes_user_and_password() {
        let creds = UpstreamCredentials::basic("svc", "s3cret");
        assert_eq!(creds.header_name, "Authorization");
        let expected = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("svc:s3cret")
        );
        assert_eq!(creds.header_value, expected);
    }

    #[test]
    fn bearer_wraps_the_token() {
        let creds = UpstreamCredentials::bearer("abc123");
        assert_eq!(creds.header_name, "Authorization");
        assert_eq!(creds.header_value, "Bearer abc123");
    }

    #[test]
    fn a_custom_header_is_supported_for_non_authorization_schemes() {
        let creds = UpstreamCredentials::new("x-api-key", "k-1");
        assert_eq!(creds.header_name, "x-api-key");
        assert_eq!(creds.header_value, "k-1");
    }
}
