//! Tests for the directive store seam: a published set is visible to the next
//! load (the fleet-wide flip), and expiry still applies through the store.

use super::*;
use std::time::Duration;

use osproxy_core::{Clock, EndpointKind, Instant, ManualClock, PrincipalId, RequestId};

use crate::directive::{DiagLevel, DiagnosticsDirective, DirectiveMatch, RequestAttrs};

fn directive(level: DiagLevel, expires_at: Instant) -> DiagnosticsDirective {
    DiagnosticsDirective {
        id: "d".to_owned(),
        match_: DirectiveMatch::all(),
        level,
        sample_per_mille: 1000,
        expires_at,
        ring_buffer: false,
        capture: false,
    }
}

fn attrs(principal: &PrincipalId) -> RequestAttrs<'_> {
    RequestAttrs {
        tenant: None,
        index: "logical",
        principal,
        endpoint: EndpointKind::Search,
    }
}

/// Evaluates `store` at `now` for a fixed request, the level a request would get.
fn level_at(store: &impl DirectiveStore, now: Instant) -> DiagLevel {
    let principal = PrincipalId::from("svc");
    store
        .load()
        .evaluate(&attrs(&principal), now, &RequestId::from("r"))
}

#[test]
fn an_empty_store_evaluates_to_off() {
    let store = InMemoryDirectiveStore::new();
    assert_eq!(level_at(&store, ManualClock::new().now()), DiagLevel::Off);
}

#[test]
fn a_published_set_is_visible_to_the_next_load() {
    let store = InMemoryDirectiveStore::new();
    let now = ManualClock::new().now();
    let future = now.saturating_add(Duration::from_secs(3600));

    // Before publish: Off. After publish: the fleet-wide flip takes effect with
    // no restart, visible to the very next load.
    assert_eq!(level_at(&store, now), DiagLevel::Off);
    store.publish(DirectiveSet::from_directives(vec![directive(
        DiagLevel::ShapeTiming,
        future,
    )]));
    assert_eq!(
        level_at(&store, now),
        DiagLevel::ShapeTiming,
        "published set is live"
    );
}

#[test]
fn the_arc_snapshot_seam_loads_a_static_set() {
    // `Arc<DirectiveSet>` is itself a `DirectiveStore` (the static default).
    let now = ManualClock::new().now();
    let future = now.saturating_add(Duration::from_secs(60));
    let set: Arc<DirectiveSet> = Arc::new(DirectiveSet::from_directives(vec![directive(
        DiagLevel::Shape,
        future,
    )]));
    assert_eq!(level_at(&set, now), DiagLevel::Shape);
}

#[test]
fn an_expired_published_directive_self_expires_through_the_store() {
    let store = InMemoryDirectiveStore::new();
    let now = ManualClock::new().now();
    // Published with an expiry at `now`: a forgotten fleet "on" stops applying.
    store.publish(DirectiveSet::from_directives(vec![directive(
        DiagLevel::Shape,
        now,
    )]));
    assert_eq!(
        level_at(&store, now),
        DiagLevel::Off,
        "an expired directive does not apply even while still in the store"
    );
}
