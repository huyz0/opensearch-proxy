//! Tests for [`super::TraceContext`] W3C trace-context propagation.

use super::*;

const SAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[test]
fn parses_a_valid_traceparent_and_round_trips() {
    let ctx = TraceContext::parse(SAMPLE).expect("valid");
    assert!(ctx.sampled());
    assert_eq!(ctx.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
    // Re-emitting the parsed context reproduces it verbatim.
    assert_eq!(ctx.to_traceparent(), SAMPLE);
}

#[test]
fn rejects_malformed_traceparents() {
    for bad in [
        "",
        "trash",
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // version
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0",  // short flags
        "00-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-00f067aa0ba902b7-01", // non-hex
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01", // zero trace
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // zero span
    ] {
        assert!(TraceContext::parse(bad).is_none(), "should reject: {bad:?}");
    }
}

#[test]
fn propagation_preserves_the_incoming_trace_id_but_starts_a_new_span() {
    let rid = RequestId::from("req-1");
    let ctx = TraceContext::propagate(Some(SAMPLE), None, &rid);
    // Same trace: downstream stays connected to the caller's trace.
    assert_eq!(ctx.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
    // New span: the downstream call is a child of the proxy, not the caller.
    let downstream = ctx.to_traceparent();
    assert!(downstream.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
    assert!(
        !downstream.contains("00f067aa0ba902b7"),
        "proxy must present its own span id, not the caller's"
    );
}

#[test]
fn propagation_retains_the_callers_span_as_the_parent() {
    let ctx = TraceContext::propagate(Some(SAMPLE), None, &RequestId::from("req-1"));
    // The caller's span id (from SAMPLE) becomes this hop's parent, so the
    // proxy's own span nests under it.
    assert_eq!(
        ctx.parent_span_id_hex().as_deref(),
        Some("00f067aa0ba902b7")
    );
    // ...and the parent is never the proxy's own span.
    assert_ne!(ctx.parent_span_id_hex(), Some(ctx.span_id_hex()));
}

#[test]
fn tracestate_is_forwarded_verbatim_when_continuing_a_trace() {
    let ctx = TraceContext::propagate(Some(SAMPLE), Some("a=1,b=2"), &RequestId::from("r"));
    assert_eq!(
        ctx.to_tracestate(),
        Some("a=1,b=2"),
        "the proxy forwards the caller's tracestate unchanged"
    );
}

#[test]
fn tracestate_without_a_valid_traceparent_is_dropped() {
    // A tracestate is meaningless without a trace to attach it to.
    let ctx = TraceContext::propagate(None, Some("a=1"), &RequestId::from("r"));
    assert!(ctx.to_tracestate().is_none());
    let ctx = TraceContext::propagate(Some("garbage"), Some("a=1"), &RequestId::from("r"));
    assert!(ctx.to_tracestate().is_none());
}

#[test]
fn an_oversized_or_empty_tracestate_is_dropped() {
    let huge = "x".repeat(MAX_TRACESTATE_LEN + 1);
    let ctx = TraceContext::propagate(Some(SAMPLE), Some(&huge), &RequestId::from("r"));
    assert!(ctx.to_tracestate().is_none(), "over the W3C cap → dropped");
    let ctx = TraceContext::propagate(Some(SAMPLE), Some("   "), &RequestId::from("r"));
    assert!(ctx.to_tracestate().is_none(), "blank → dropped");
}

#[test]
fn a_minted_root_has_no_parent() {
    let ctx = TraceContext::propagate(None, None, &RequestId::from("req-7"));
    assert!(
        ctx.parent_span_id_hex().is_none(),
        "a root span has no parent to nest under"
    );
}

#[test]
fn an_unsampled_parent_keeps_its_flag() {
    let unsampled = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    let ctx = TraceContext::propagate(Some(unsampled), None, &RequestId::from("r"));
    assert!(
        !ctx.sampled(),
        "sampling decision is inherited from the parent"
    );
}

#[test]
fn a_missing_or_malformed_parent_mints_a_sampled_root() {
    for incoming in [None, Some("garbage")] {
        let ctx = TraceContext::propagate(incoming, None, &RequestId::from("req-7"));
        assert!(ctx.sampled(), "a freshly minted root is sampled");
        assert_eq!(ctx.to_traceparent().len(), TRACEPARENT_LEN);
    }
}

#[test]
fn a_different_process_seed_yields_disjoint_ids_for_the_same_request() {
    // The fleet-uniqueness invariant: two instances (distinct process seeds)
    // must not derive the same id for the same (process-local) request id —
    // otherwise unrelated requests on different instances would collide.
    let s = b"req-5";
    assert_ne!(
        derive16_with(1, s),
        derive16_with(2, s),
        "different seeds must give different trace ids"
    );
    assert_ne!(
        fnv1a(7 ^ 1, s),
        fnv1a(7 ^ 2, s),
        "different seeds must give different span ids"
    );
}

#[test]
fn derived_ids_are_stable_per_request_and_distinct_across_requests() {
    let a1 = TraceContext::propagate(None, None, &RequestId::from("a")).to_traceparent();
    let a2 = TraceContext::propagate(None, None, &RequestId::from("a")).to_traceparent();
    let b = TraceContext::propagate(None, None, &RequestId::from("b")).to_traceparent();
    assert_eq!(a1, a2, "same request id derives the same context");
    assert_ne!(a1, b, "different requests get different traces");
}
