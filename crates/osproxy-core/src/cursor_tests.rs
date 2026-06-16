//! Tests for the cursor-affinity envelope: it round-trips, fails closed on
//! tampering or a wrong key, and carries the upstream id verbatim.
#![allow(clippy::unwrap_used)]

use super::*;

/// A deterministic stand-in for the real HMAC signer: a keyed FNV-1a fold. Same
/// key ⇒ same tag (so a different fleet instance verifies); different key ⇒
/// different tag (so a foreign token fails).
struct FnvSigner(u64);

impl CursorSigner for FnvSigner {
    fn tag(&self, msg: &[u8]) -> Vec<u8> {
        let mut h = 0xcbf2_9ce4_8422_2325 ^ self.0;
        for &b in msg {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h.to_be_bytes().to_vec()
    }
}

const CURSOR: &str = "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAA+base64ish/id==";

#[test]
fn a_wrapped_cursor_round_trips_to_its_cluster_and_id() {
    let signer = FnvSigner(42);
    let token = wrap(&signer, &ClusterId::from("eu-1"), CURSOR);
    let (cluster, cursor) = unwrap(&signer, &token).expect("verifies");
    assert_eq!(cluster, ClusterId::from("eu-1"));
    assert_eq!(cursor, CURSOR);
}

#[test]
fn the_upstream_id_is_carried_verbatim_not_re_encoded() {
    // The (possibly multi-KB) upstream id appears unchanged after the last `.`,
    // so wrapping adds only the small signed prefix.
    let token = wrap(&FnvSigner(1), &ClusterId::from("eu-1"), CURSOR);
    assert!(token.ends_with(&format!(".{CURSOR}")), "{token}");
}

#[test]
fn a_token_from_a_different_key_fails_closed() {
    let token = wrap(&FnvSigner(1), &ClusterId::from("eu-1"), CURSOR);
    assert!(
        unwrap(&FnvSigner(2), &token).is_none(),
        "foreign key must reject"
    );
}

#[test]
fn tampering_with_the_cluster_fails_closed() {
    let signer = FnvSigner(7);
    let token = wrap(&signer, &ClusterId::from("eu-1"), CURSOR);
    // Re-point at another cluster by swapping the cluster_hex (hex of "us-9").
    let mut parts = token.splitn(3, '.');
    let _ = parts.next();
    let rest = format!("{}.{}", parts.next().unwrap(), parts.next().unwrap());
    let forged = format!("{}.{rest}", hex_of("us-9"));
    assert!(
        unwrap(&signer, &forged).is_none(),
        "cluster swap must reject"
    );
}

#[test]
fn tampering_with_the_cursor_fails_closed() {
    let signer = FnvSigner(7);
    let token = wrap(&signer, &ClusterId::from("eu-1"), CURSOR);
    let forged = format!("{token}X"); // mutate the trailing cursor payload
    assert!(
        unwrap(&signer, &forged).is_none(),
        "cursor edit must reject"
    );
}

#[test]
fn malformed_tokens_are_rejected() {
    let signer = FnvSigner(7);
    for bad in ["", "nodots", "one.two", "zz.zz.cursor", "6575.zz.c"] {
        assert!(unwrap(&signer, bad).is_none(), "should reject {bad:?}");
    }
}

/// Hex of an ASCII string, for forging a cluster field in a test.
fn hex_of(s: &str) -> String {
    let mut out = String::new();
    push_hex(&mut out, s.as_bytes());
    out
}
