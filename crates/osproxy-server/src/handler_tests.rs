// Unit tests for the handler's pure routing predicates.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

#[test]
fn opens_scroll_detects_the_scroll_param_in_any_position() {
    // A scroll-opening search returns a `_scroll_id` that must be affinity-wrapped
    // against the whole response body, so it keeps the buffered (non-streamed) path.
    assert!(opens_scroll(Some("scroll")));
    assert!(opens_scroll(Some("scroll=1m")));
    assert!(opens_scroll(Some("q=foo&scroll=1m")));
    assert!(opens_scroll(Some("scroll=1m&pretty")));
    assert!(opens_scroll(Some("pretty&scroll")));
}

#[test]
fn opens_scroll_ignores_lookalikes_and_absence() {
    // Only the exact `scroll` key counts — not a value mentioning it, nor a longer
    // key that merely starts with it; and no query string means a plain search.
    assert!(!opens_scroll(None));
    assert!(!opens_scroll(Some("")));
    assert!(!opens_scroll(Some("q=scroll")));
    assert!(!opens_scroll(Some("scrollx=1")));
    assert!(!opens_scroll(Some("no_scroll=1")));
    assert!(!opens_scroll(Some("pretty&q=match_all")));
}
