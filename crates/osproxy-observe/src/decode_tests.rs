//! Tests for the fleet directive-set decoder: a well-formed body decodes every
//! field; any malformed or out-of-range field rejects the whole set.

use super::*;
use std::sync::Arc;

use osproxy_core::ManualClock;

fn decode(body: &str) -> Result<DirectiveSet, &'static str> {
    let clock = Arc::new(ManualClock::new());
    decode_directive_set(body.as_bytes(), clock.as_ref())
}

#[test]
fn a_well_formed_body_decodes_every_directive() {
    let set = decode(
        r#"{"directives":[
            {"id":"a","level":"ShapeTiming","ttl_secs":1800,"tenant":"acme"},
            {"id":"b","level":"Shape","ttl_secs":60,"sample_per_mille":250,"ring_buffer":true}
        ]}"#,
    )
    .expect("valid body decodes");
    assert_eq!(set.len(), 2);
}

#[test]
fn an_empty_directive_list_is_valid_a_clear() {
    // Publishing an empty set is the "turn everything off" operation.
    assert_eq!(decode(r#"{"directives":[]}"#).unwrap().len(), 0);
}

#[test]
fn malformed_bodies_are_rejected_whole() {
    for (body, reason) in [
        ("not json", "invalid_json"),
        (r"{}", "missing_directives"),
        (
            r#"{"directives":[{"level":"Shape","ttl_secs":60}]}"#,
            "missing_id",
        ),
        (
            r#"{"directives":[{"id":"a","ttl_secs":60}]}"#,
            "missing_level",
        ),
        (
            r#"{"directives":[{"id":"a","level":"Nope","ttl_secs":60}]}"#,
            "unknown_level",
        ),
        (
            r#"{"directives":[{"id":"a","level":"Shape"}]}"#,
            "missing_ttl_secs",
        ),
        (
            r#"{"directives":[{"id":"a","level":"Shape","ttl_secs":0}]}"#,
            "zero_ttl",
        ),
        (
            r#"{"directives":[{"id":"a","level":"Shape","ttl_secs":60,"sample_per_mille":1001}]}"#,
            "bad_sample_rate",
        ),
        // A misspelled targeting key must be rejected, not silently dropped —
        // otherwise "tennant" would publish a fleet-wide directive by accident.
        (
            r#"{"directives":[{"id":"a","level":"Shape","ttl_secs":60,"tennant":"acme"}]}"#,
            "unknown_field",
        ),
        (
            r#"{"directives":[{"id":"a","level":"Shape","ttl_secs":60}],"extra":1}"#,
            "unknown_field",
        ),
    ] {
        assert_eq!(decode(body).err(), Some(reason), "body: {body}");
    }
}

#[test]
fn a_relative_ttl_resolves_to_an_absolute_expiry_on_the_clock() {
    let clock = Arc::new(ManualClock::new());
    let set = decode_directive_set(
        br#"{"directives":[{"id":"a","level":"Shape","ttl_secs":600}]}"#,
        clock.as_ref(),
    )
    .unwrap();
    // Evaluated before expiry: applies. After advancing past the TTL: gone.
    let attrs = crate::RequestAttrs {
        tenant: None,
        index: "i",
        principal: &osproxy_core::PrincipalId::from("svc"),
        endpoint: osproxy_core::EndpointKind::Search,
    };
    let rid = osproxy_core::RequestId::from("r");
    assert_eq!(set.evaluate(&attrs, clock.now(), &rid), DiagLevel::Shape);
    clock.advance(std::time::Duration::from_secs(601));
    assert_eq!(
        set.evaluate(&attrs, clock.now(), &rid),
        DiagLevel::Off,
        "the TTL-resolved expiry applies"
    );
}
