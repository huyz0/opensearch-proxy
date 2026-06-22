use super::*;
use osproxy_core::{Clock, ManualClock};
use std::time::Duration;

fn at(secs: u64) -> Instant {
    let clock = ManualClock::new();
    clock.advance(Duration::from_secs(secs));
    clock.now()
}

fn directive(
    level: DiagLevel,
    match_: DirectiveMatch,
    sample_per_mille: u16,
) -> DiagnosticsDirective {
    DiagnosticsDirective {
        id: "d1".to_owned(),
        match_,
        level,
        sample_per_mille,
        expires_at: at(100),
        ring_buffer: false,
        capture: false,
    }
}

fn attrs<'a>(
    tenant: Option<&'a PartitionId>,
    index: &'a str,
    principal: &'a PrincipalId,
    endpoint: EndpointKind,
) -> RequestAttrs<'a> {
    RequestAttrs {
        tenant,
        index,
        principal,
        endpoint,
    }
}

#[test]
fn an_empty_set_is_off() {
    let set = DirectiveSet::new();
    let p = PrincipalId::from("svc");
    let a = attrs(None, "orders", &p, EndpointKind::Search);
    assert_eq!(
        set.evaluate(&a, at(1), &RequestId::from("r")),
        DiagLevel::Off
    );
}

#[test]
fn the_highest_matching_level_wins() {
    let set = DirectiveSet::from_directives(vec![
        directive(DiagLevel::Shape, DirectiveMatch::all(), 1000),
        directive(
            DiagLevel::ShapeRewriteDiff,
            DirectiveMatch::all().for_index(IndexName::from("orders")),
            1000,
        ),
    ]);
    let p = PrincipalId::from("svc");
    let a = attrs(None, "orders", &p, EndpointKind::Search);
    assert_eq!(
        set.evaluate(&a, at(1), &RequestId::from("r")),
        DiagLevel::ShapeRewriteDiff
    );
}

#[test]
fn each_target_field_narrows_the_match() {
    let acme = PartitionId::from("acme");
    let p = PrincipalId::from("svc");
    let m = DirectiveMatch::all()
        .for_tenant(acme.clone())
        .for_endpoint(EndpointKind::Search);
    let set = DirectiveSet::from_directives(vec![directive(DiagLevel::Shape, m, 1000)]);

    // Matches: right tenant + endpoint.
    let hit = attrs(Some(&acme), "orders", &p, EndpointKind::Search);
    assert_eq!(
        set.evaluate(&hit, at(1), &RequestId::from("r")),
        DiagLevel::Shape
    );
    // Misses: wrong endpoint.
    let miss = attrs(Some(&acme), "orders", &p, EndpointKind::Count);
    assert_eq!(
        set.evaluate(&miss, at(1), &RequestId::from("r")),
        DiagLevel::Off
    );
    // Misses: tenant not yet resolved.
    let unresolved = attrs(None, "orders", &p, EndpointKind::Search);
    assert_eq!(
        set.evaluate(&unresolved, at(1), &RequestId::from("r")),
        DiagLevel::Off
    );
}

#[test]
fn wants_capture_tracks_a_matching_unexpired_capture_directive() {
    let acme = PartitionId::from("acme");
    let p = PrincipalId::from("svc");
    let mut d = directive(
        DiagLevel::Off,
        DirectiveMatch::all().for_tenant(acme.clone()),
        1000,
    );
    d.capture = true;
    let set = DirectiveSet::from_directives(vec![d]);

    let hit = attrs(Some(&acme), "orders", &p, EndpointKind::Search);
    let other = PartitionId::from("globex");
    let miss = attrs(Some(&other), "orders", &p, EndpointKind::Search);
    let r = RequestId::from("r");

    assert!(
        set.wants_capture(&hit, at(1), &r),
        "matches the target tenant"
    );
    assert!(
        !set.wants_capture(&miss, at(1), &r),
        "other tenant is not captured"
    );
    assert!(
        !set.wants_capture(&hit, at(150), &r),
        "past expiry, capture turns back off on its own (TTL)"
    );
    // A directive without the capture flag never enables capture, even when it
    // raises the diagnostics level.
    assert!(!DirectiveSet::from_directives(vec![directive(
        DiagLevel::Shape,
        DirectiveMatch::all(),
        1000
    )])
    .wants_capture(&hit, at(1), &r));
}

#[test]
fn an_expired_directive_does_not_apply() {
    let set = DirectiveSet::from_directives(vec![directive(
        DiagLevel::Shape,
        DirectiveMatch::all(),
        1000,
    )]);
    let p = PrincipalId::from("svc");
    let a = attrs(None, "orders", &p, EndpointKind::Search);
    // expires_at is at(100); a request at(150) is past it.
    assert_eq!(
        set.evaluate(&a, at(150), &RequestId::from("r")),
        DiagLevel::Off
    );
}

#[test]
fn sampling_is_deterministic_and_bounded() {
    // rate 0 never records; rate 1000 always; partial is stable per request.
    let p = PrincipalId::from("svc");
    let a = attrs(None, "orders", &p, EndpointKind::Search);
    let never =
        DirectiveSet::from_directives(vec![directive(DiagLevel::Shape, DirectiveMatch::all(), 0)]);
    let always = DirectiveSet::from_directives(vec![directive(
        DiagLevel::Shape,
        DirectiveMatch::all(),
        1000,
    )]);
    assert_eq!(
        never.evaluate(&a, at(1), &RequestId::from("r")),
        DiagLevel::Off
    );
    assert_eq!(
        always.evaluate(&a, at(1), &RequestId::from("r")),
        DiagLevel::Shape
    );

    let half = DirectiveSet::from_directives(vec![directive(
        DiagLevel::Shape,
        DirectiveMatch::all(),
        500,
    )]);
    let r = RequestId::from("req-123");
    let first = half.evaluate(&a, at(1), &r);
    assert_eq!(
        first,
        half.evaluate(&a, at(1), &r),
        "same request decides the same way"
    );
    // Across many requests, partial sampling admits some and not others.
    let admitted = (0..1000)
        .filter(|n| {
            half.evaluate(&a, at(1), &RequestId::from(format!("r{n}").as_str())) == DiagLevel::Shape
        })
        .count();
    assert!(
        (300..700).contains(&admitted),
        "≈half admitted, got {admitted}"
    );
}

#[test]
fn introspect_renders_the_well_defined_settings_schema() {
    let set = DirectiveSet::from_directives(vec![DiagnosticsDirective {
        id: "raise-acme".to_owned(),
        match_: DirectiveMatch::all()
            .for_tenant(PartitionId::from("acme"))
            .for_index(IndexName::from("orders")),
        level: DiagLevel::ShapeTiming,
        sample_per_mille: 250,
        expires_at: at(100),
        ring_buffer: true,
        capture: true,
    }]);
    // Before expiry: the directive is live and fully described.
    let v = set.introspect(at(10));
    let d = &v["directives"][0];
    assert_eq!(d["id"], "raise-acme");
    assert_eq!(d["level"], "ShapeTiming");
    assert_eq!(d["tenant"], "acme");
    assert_eq!(d["index"], "orders");
    assert_eq!(d["sample_per_mille"], 250);
    assert_eq!(d["ring_buffer"], true);
    assert_eq!(d["capture"], true);
    assert_eq!(d["expired"], false);
    // Unset targets are omitted (wildcards), not rendered null.
    assert!(d.get("principal").is_none());
    assert!(d.get("endpoint").is_none());
    // After expiry: same shape, the `expired` flag flips, the live read.
    let later = set.introspect(at(200));
    assert_eq!(later["directives"][0]["expired"], true);
}
