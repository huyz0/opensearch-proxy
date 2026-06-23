//! The client-to-upstream header forwarding policy.
//!
//! When the proxy forwards a request to a cluster it rebuilds the request from
//! scratch, so by default only the headers it manages (content type, trace) reach
//! the upstream. For a sidecar/transparent deployment that is too lossy: the
//! client's own headers (custom routing hints, `Authorization`, vendor tracing
//! like B3, …) should travel through. This module computes the **forwarded set**:
//! every client header except the ones that are unsafe to relay verbatim.
//!
//! Two strip lists:
//!
//! - **Mandatory** (never forwarded, not configurable): hop-by-hop headers
//!   (RFC 9110 §7.6.1) plus `host` and `content-length`, because the proxy
//!   re-frames the request to a different upstream and may rewrite the body, so
//!   the client's framing headers would be wrong.
//! - **Configured deny** (`header_forwarding.deny`): an operator's case-insensitive
//!   list, e.g. add `authorization` to keep the client credential from reaching
//!   the cluster. Empty by default (pass-all, the sidecar-trust default).
//!
//! Trace headers (`traceparent`/`tracestate`) ride through here like any other
//! client header; whether the proxy *overrides* them with its own span is decided
//! separately at dispatch (only when span export is on), so a transparent proxy
//! passes the client's tracing through untouched.

/// Hop-by-hop and framing headers that are never forwarded to the upstream,
/// regardless of policy. Lowercase for case-insensitive comparison.
const NEVER_FORWARD: &[&str] = &[
    // Hop-by-hop (RFC 9110 §7.6.1): meaningful only for a single transport hop.
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    // Framing: the proxy targets a different host and may rewrite the body, so
    // the client's values do not apply. The upstream request builder sets these.
    "host",
    "content-length",
];

/// Whether `name` is a header the proxy must never relay verbatim to the upstream.
fn is_never_forwarded(name: &str) -> bool {
    NEVER_FORWARD.iter().any(|h| name.eq_ignore_ascii_case(h))
}

/// The forwarding policy: whether to forward client headers at all, and which to
/// drop on top of the mandatory hop-by-hop/framing set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ForwardPolicy {
    /// Forward client headers to the upstream at all. `false` restores the
    /// minimal behavior (only proxy-managed headers reach the cluster).
    pub enabled: bool,
    /// Extra headers to drop (case-insensitive), on top of the mandatory set.
    pub deny: Vec<String>,
}

impl ForwardPolicy {
    /// The default sidecar policy: forward every client header (minus the
    /// mandatory hop-by-hop/framing set), nothing extra denied.
    #[must_use]
    pub fn pass_all() -> Self {
        Self {
            enabled: true,
            deny: Vec::new(),
        }
    }

    /// Computes the headers to forward upstream from the raw client headers.
    /// Returns an empty vec when forwarding is disabled. Hop-by-hop/framing
    /// headers and any in the configured `deny` list are dropped.
    #[must_use]
    pub fn forward_set(&self, client: &[(String, String)]) -> Vec<(String, String)> {
        if !self.enabled {
            return Vec::new();
        }
        client
            .iter()
            .filter(|(name, _)| !is_never_forwarded(name))
            .filter(|(name, _)| !self.deny.iter().any(|d| d.eq_ignore_ascii_case(name)))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw() -> Vec<(String, String)> {
        vec![
            ("Authorization".to_owned(), "Bearer s3cret".to_owned()),
            ("X-Tenant".to_owned(), "acme".to_owned()),
            ("traceparent".to_owned(), "00-abc-def-01".to_owned()),
            ("b3".to_owned(), "abc-def-1".to_owned()),
            ("Connection".to_owned(), "keep-alive".to_owned()),
            ("Host".to_owned(), "client.local".to_owned()),
            ("Content-Length".to_owned(), "42".to_owned()),
        ]
    }

    fn names(set: &[(String, String)]) -> Vec<String> {
        set.iter().map(|(k, _)| k.to_ascii_lowercase()).collect()
    }

    #[test]
    fn pass_all_forwards_client_headers_minus_hop_by_hop_and_framing() {
        let set = ForwardPolicy::pass_all().forward_set(&raw());
        let n = names(&set);
        // Application headers (including the client's auth and vendor tracing)
        // pass through by default (sidecar trust).
        assert!(n.contains(&"authorization".to_owned()));
        assert!(n.contains(&"x-tenant".to_owned()));
        assert!(n.contains(&"traceparent".to_owned()));
        assert!(n.contains(&"b3".to_owned()));
        // Hop-by-hop and framing never forwarded.
        assert!(!n.contains(&"connection".to_owned()));
        assert!(!n.contains(&"host".to_owned()));
        assert!(!n.contains(&"content-length".to_owned()));
    }

    #[test]
    fn the_deny_list_drops_named_headers_case_insensitively() {
        let policy = ForwardPolicy {
            enabled: true,
            deny: vec!["AUTHORIZATION".to_owned()],
        };
        let n = names(&policy.forward_set(&raw()));
        assert!(!n.contains(&"authorization".to_owned()), "denied: {n:?}");
        assert!(n.contains(&"x-tenant".to_owned()), "others still pass");
    }

    #[test]
    fn disabled_forwards_nothing() {
        let policy = ForwardPolicy {
            enabled: false,
            deny: Vec::new(),
        };
        assert!(policy.forward_set(&raw()).is_empty());
    }
}
