//! `Authorization: Bearer` parsing and constant-time token comparison, the one
//! place the binary reads bearer credentials, so the request-auth path and the
//! privileged admin channel can't drift apart (e.g. one case-sensitive on the
//! scheme and the other not).
//!
//! The scheme is matched case-insensitively per RFC 6750; the token is whatever
//! follows the first space, verbatim. Token equality uses [`token_eq`], a
//! constant-time compare so a wrong admin token cannot be narrowed by timing.

/// The bearer token from a header list, or `None` if there is no
/// `Authorization` header or its scheme is not `Bearer`.
pub(crate) fn parse(headers: &[(String, String)]) -> Option<&str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .and_then(|(_, v)| v.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, token)| token)
}

/// Whether the request's bearer token equals `expected` (constant-time).
pub(crate) fn matches(headers: &[(String, String)], expected: &str) -> bool {
    token_eq(parse(headers).unwrap_or("").as_bytes(), expected.as_bytes())
}

/// The header list with any `Authorization` header removed (case-insensitive),
/// so the credential consumed at the ingress never travels into the pipeline,
/// observability, or logs.
pub(crate) fn without_authorization(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("authorization"))
        .cloned()
        .collect()
}

/// Constant-time comparison **for equal-length inputs** (no early return on the
/// first differing byte). The length itself is not concealed, acceptable for a
/// fixed shared token, where the length is not the secret.
fn token_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth(value: &str) -> Vec<(String, String)> {
        vec![("Authorization".to_owned(), value.to_owned())]
    }

    #[test]
    fn parse_is_case_insensitive_on_the_scheme_only() {
        assert_eq!(parse(&auth("Bearer tok")), Some("tok"));
        // Scheme case does not matter (RFC 6750); the token is verbatim.
        assert_eq!(parse(&auth("bearer tok")), Some("tok"));
        assert_eq!(parse(&auth("Basic tok")), None, "wrong scheme");
        assert_eq!(parse(&auth("tok")), None, "no scheme");
        assert_eq!(parse(&[]), None, "no header");
    }

    #[test]
    fn matches_requires_an_exact_token() {
        assert!(matches(&auth("Bearer s3cret"), "s3cret"));
        assert!(matches(&auth("bearer s3cret"), "s3cret"));
        assert!(!matches(&auth("Bearer s3cre"), "s3cret"));
        assert!(!matches(&auth("Bearer s3cret!"), "s3cret"));
        assert!(!matches(&auth("s3cret"), "s3cret"), "scheme required");
    }

    #[test]
    fn without_authorization_strips_only_that_header_case_insensitively() {
        let headers = vec![
            ("Authorization".to_owned(), "Bearer s3cret".to_owned()),
            ("content-type".to_owned(), "application/json".to_owned()),
            ("x-tenant".to_owned(), "acme".to_owned()),
        ];
        let safe = without_authorization(&headers);
        assert!(
            !safe
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("authorization")),
            "the credential is gone: {safe:?}"
        );
        // Everything the engine still needs survives.
        assert!(safe.iter().any(|(k, _)| k == "content-type"));
        assert!(safe.iter().any(|(k, v)| k == "x-tenant" && v == "acme"));
    }

    #[test]
    fn token_eq_matches_byte_compare_semantics() {
        assert!(token_eq(b"abc", b"abc"));
        assert!(!token_eq(b"abc", b"abd"));
        assert!(!token_eq(b"abc", b"ab"), "differing lengths differ");
    }
}
