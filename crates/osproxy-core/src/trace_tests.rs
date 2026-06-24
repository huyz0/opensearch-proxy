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
    // must not derive the same id for the same (process-local) request id,
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

// --- B3 (Zipkin/Istio) ingress ---------------------------------------------

const B3_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const B3_SPAN: &str = "00f067aa0ba902b7";

#[test]
fn parses_a_b3_single_header_128_and_64_bit() {
    // 128-bit trace id, sampled.
    let c = TraceContext::parse_b3(&format!("{B3_TRACE}-{B3_SPAN}-1")).expect("128-bit");
    assert_eq!(c.trace_id_hex(), B3_TRACE);
    assert!(c.sampled());
    // 64-bit trace id is right-aligned into 128 bits.
    let c64 = TraceContext::parse_b3(&format!("a3ce929d0e0e4736-{B3_SPAN}")).expect("64-bit");
    assert_eq!(c64.trace_id_hex(), "0000000000000000a3ce929d0e0e4736");
    // Sampling flag honored.
    assert!(!TraceContext::parse_b3(&format!("{B3_TRACE}-{B3_SPAN}-0"))
        .unwrap()
        .sampled());
}

#[test]
fn rejects_b3_without_a_trace_to_continue() {
    for bad in [
        "0",                                                    // sampling-only deny
        "1",                                                    // sampling-only accept
        B3_TRACE,                                               // trace but no span
        &format!("xxxx-{B3_SPAN}"),                             // non-hex trace
        &format!("{B3_TRACE}-{B3_SPAN}-2"),                     // bad sampling flag
        &format!("00000000000000000000000000000000-{B3_SPAN}"), // zero trace
    ] {
        assert!(
            TraceContext::parse_b3(bad).is_none(),
            "should reject: {bad:?}"
        );
    }
}

#[test]
fn b3_continues_the_trace_when_no_traceparent_is_present() {
    let rid = RequestId::from("req-b3");
    let b3 = format!("{B3_TRACE}-{B3_SPAN}-1");
    let ctx = TraceContext::propagate_with_b3(None, None, Some(&b3), &rid);
    // The proxy's span shares the client's B3 trace id (continuity), with a fresh
    // span for the proxy hop and the client's span as its parent.
    assert_eq!(ctx.trace_id_hex(), B3_TRACE);
    assert_eq!(ctx.parent_span_id_hex().as_deref(), Some(B3_SPAN));
    assert_ne!(
        ctx.span_id_hex(),
        B3_SPAN,
        "the proxy hop gets its own span"
    );
    assert!(ctx.sampled());
    // B3 carries no tracestate.
    assert!(ctx.to_tracestate().is_none());
}

#[test]
fn w3c_traceparent_wins_over_b3_when_both_are_present() {
    let b3 = "11111111111111111111111111111111-2222222222222222-1";
    let ctx = TraceContext::propagate_with_b3(Some(SAMPLE), None, Some(b3), &RequestId::from("r"));
    assert_eq!(
        ctx.trace_id_hex(),
        "4bf92f3577b34da6a3ce929d0e0e4736",
        "W3C trace id, not B3"
    );
}
