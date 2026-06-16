//! Tests for the HMAC cursor signer: same key round-trips an envelope, a
//! different key fails it closed.
#![allow(clippy::unwrap_used)]

use super::*;
use osproxy_core::cursor::{unwrap, wrap};
use osproxy_core::ClusterId;

const CURSOR: &str = "DXF1ZXJ5QW5kRmV0Y2grealId==";

#[test]
fn an_envelope_signed_with_the_shared_key_round_trips() {
    let signer = HmacCursorSigner::new(b"fleet-secret");
    let token = wrap(&signer, &ClusterId::from("eu-1"), CURSOR);
    // A second instance built from the same secret verifies it.
    let other = HmacCursorSigner::new(b"fleet-secret");
    let (cluster, id) = unwrap(&other, &token).expect("verifies with the shared key");
    assert_eq!(cluster, ClusterId::from("eu-1"));
    assert_eq!(id, CURSOR);
}

#[test]
fn a_different_key_fails_closed() {
    let token = wrap(
        &HmacCursorSigner::new(b"key-a"),
        &ClusterId::from("eu-1"),
        CURSOR,
    );
    assert!(
        unwrap(&HmacCursorSigner::new(b"key-b"), &token).is_none(),
        "a foreign key must not verify"
    );
}

#[test]
fn the_signer_never_renders_its_key() {
    let dbg = format!("{:?}", HmacCursorSigner::new(b"super-secret"));
    assert!(
        !dbg.contains("super-secret"),
        "key must not leak in Debug: {dbg}"
    );
}
