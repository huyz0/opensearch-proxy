//! Tests for the HMAC `X-Debug-Directive` verifier: a validly signed token
//! authorizes its directive; a forged tag, a wrong key, a tampered payload, an
//! expired token, or malformed input all authorize nothing.

use super::*;
use std::time::Duration;

use osproxy_core::ManualClock;

const KEY: &[u8] = b"operator-shared-secret";

fn clock_at(secs: u64) -> Arc<ManualClock> {
    let clock = Arc::new(ManualClock::new());
    clock.advance(Duration::from_secs(secs));
    clock
}

#[test]
fn a_validly_signed_token_authorizes_its_directive() {
    let clock = clock_at(0);
    let verifier = HmacDirectiveVerifier::new(KEY, clock.clone());
    let payload = br#"{"level":"ShapeTiming","exp":600,"tenant":"acme"}"#;
    let token = sign_token(KEY, payload);

    let d = verifier.verify(&token).expect("valid token authorizes");
    assert_eq!(d.level, DiagLevel::ShapeTiming);
    assert_eq!(
        d.match_.tenant.as_ref().map(PartitionId::as_str),
        Some("acme")
    );
    assert_eq!(d.sample_per_mille, 1000, "defaults to always-sample");
    // Expiry resolved against the clock: 600s out.
    assert_eq!(
        d.expires_at,
        clock.now().saturating_add(Duration::from_secs(600))
    );
}

#[test]
fn a_forged_tag_is_rejected() {
    let verifier = HmacDirectiveVerifier::new(KEY, clock_at(0));
    let payload = br#"{"level":"Shape","exp":600}"#;
    let forged = format!("{}.{}", encode_hex(payload), encode_hex(&[0u8; 32]));
    assert!(
        verifier.verify(&forged).is_none(),
        "a forged tag must not verify"
    );
}

#[test]
fn a_token_signed_with_the_wrong_key_is_rejected() {
    let verifier = HmacDirectiveVerifier::new(KEY, clock_at(0));
    let payload = br#"{"level":"Shape","exp":600}"#;
    let token = sign_token(b"attacker-key", payload);
    assert!(
        verifier.verify(&token).is_none(),
        "wrong key must not verify"
    );
}

#[test]
fn a_tampered_payload_is_rejected() {
    let verifier = HmacDirectiveVerifier::new(KEY, clock_at(0));
    let payload = br#"{"level":"Shape","exp":600}"#;
    let token = sign_token(KEY, payload);
    // Swap the signed payload for a higher-privilege one, keeping the old tag.
    let (_, sig_hex) = token.split_once('.').unwrap();
    let tampered_payload = br#"{"level":"ShapeRewriteDiff","exp":600}"#;
    let tampered = format!("{}.{sig_hex}", encode_hex(tampered_payload));
    assert!(
        verifier.verify(&tampered).is_none(),
        "the tag must not validate a swapped payload"
    );
}

#[test]
fn an_expired_token_authorizes_nothing() {
    // Clock is at 1000s; the token expired at 600s.
    let verifier = HmacDirectiveVerifier::new(KEY, clock_at(1000));
    let payload = br#"{"level":"Shape","exp":600}"#;
    let token = sign_token(KEY, payload);
    assert!(
        verifier.verify(&token).is_none(),
        "a token past its expiry authorizes nothing even when correctly signed"
    );
}

#[test]
fn malformed_input_is_rejected() {
    let verifier = HmacDirectiveVerifier::new(KEY, clock_at(0));
    for bad in ["", "no-dot", "zz.zz", &sign_token(KEY, b"not json")] {
        assert!(verifier.verify(bad).is_none(), "rejected: {bad:?}");
    }
    // Unknown level name fails to parse even when correctly signed.
    let token = sign_token(KEY, br#"{"level":"Verbose","exp":600}"#);
    assert!(verifier.verify(&token).is_none(), "unknown level rejected");
}

#[test]
fn a_fully_populated_payload_maps_to_every_field() {
    let verifier = HmacDirectiveVerifier::new(KEY, clock_at(0));
    let payload = br#"{"level":"ShapeRewriteDiff","exp":600,"tenant":"acme",
        "index":"orders","principal":"svc","sample_per_mille":250,"ring_buffer":true}"#;
    let d = verifier
        .verify(&sign_token(KEY, payload))
        .expect("valid token");
    assert_eq!(d.level, DiagLevel::ShapeRewriteDiff);
    assert_eq!(
        d.match_.tenant.as_ref().map(PartitionId::as_str),
        Some("acme")
    );
    assert_eq!(
        d.match_.index.as_ref().map(IndexName::as_str),
        Some("orders")
    );
    assert_eq!(
        d.match_.principal.as_ref().map(PrincipalId::as_str),
        Some("svc")
    );
    assert_eq!(d.sample_per_mille, 250);
    assert!(d.ring_buffer);
}

#[test]
fn an_out_of_range_sampling_rate_authorizes_nothing() {
    // A signed-but-bogus sample rate must fail closed (reject), not widen capture
    // to always-on.
    let verifier = HmacDirectiveVerifier::new(KEY, clock_at(0));
    for bad in [
        r#"{"level":"Shape","exp":600,"sample_per_mille":1001}"#,
        r#"{"level":"Shape","exp":600,"sample_per_mille":70000}"#,
    ] {
        let token = sign_token(KEY, bad.as_bytes());
        assert!(verifier.verify(&token).is_none(), "rejected: {bad}");
    }
}

#[test]
fn a_token_expiring_exactly_now_authorizes_nothing() {
    // The boundary: exp == now is treated as already expired (no zero-lifetime
    // directive slips through).
    let verifier = HmacDirectiveVerifier::new(KEY, clock_at(600));
    let token = sign_token(KEY, br#"{"level":"Shape","exp":600}"#);
    assert!(verifier.verify(&token).is_none(), "exp == now is expired");
}
