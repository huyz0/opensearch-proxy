//! Tests for the break-glass ring buffer: captures are retained in order and the
//! oldest is evicted past capacity (the tape is bounded).

use super::*;
use serde_json::json;

#[test]
fn an_empty_buffer_has_no_captures() {
    let buf = BreakGlassBuffer::new(4);
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);
    assert!(buf.snapshot().is_empty());
}

#[test]
fn captures_are_retained_in_order_and_bounded() {
    let buf = BreakGlassBuffer::new(2);
    buf.capture(json!({"request_id": "a"}));
    buf.capture(json!({"request_id": "b"}));
    buf.capture(json!({"request_id": "c"}));

    // Capacity 2: the oldest ("a") is evicted, order preserved oldest-first.
    let tape = buf.snapshot();
    assert_eq!(tape.len(), 2);
    assert_eq!(tape[0]["request_id"], "b");
    assert_eq!(tape[1]["request_id"], "c");
}

#[test]
fn capacity_is_at_least_one() {
    // A zero capacity would make capture a no-op; clamp to 1 so a flipped
    // directive always captures at least the most recent request.
    let buf = BreakGlassBuffer::new(0);
    buf.capture(json!({"request_id": "x"}));
    assert_eq!(buf.len(), 1);
    assert_eq!(buf.snapshot()[0]["request_id"], "x");
}
