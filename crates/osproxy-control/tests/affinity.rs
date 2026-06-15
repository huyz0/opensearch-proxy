//! Cursor (scroll / PIT) affinity (`docs/03` §6): a binding resolves to its
//! creating cluster, expires with the cursor TTL, and the map is capacity-bound
//! so a flood of cursors cannot grow memory without limit. Time is driven by a
//! `ManualClock`, so expiry is deterministic.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use osproxy_control::CursorAffinity;
use osproxy_core::{ClusterId, ManualClock};

const TTL: Duration = Duration::from_secs(300);

fn affinity(capacity: usize) -> (CursorAffinity, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new());
    let aff = CursorAffinity::new(TTL, capacity).with_clock(clock.clone());
    (aff, clock)
}

#[test]
fn a_pinned_cursor_resolves_to_its_creating_cluster() {
    let (aff, _clock) = affinity(16);
    aff.pin("scroll-abc", ClusterId::from("eu-1"));
    aff.pin("pit-xyz", ClusterId::from("us-1"));

    assert_eq!(aff.resolve("scroll-abc"), Some(ClusterId::from("eu-1")));
    assert_eq!(aff.resolve("pit-xyz"), Some(ClusterId::from("us-1")));
    assert_eq!(aff.resolve("unknown"), None);
}

#[test]
fn a_binding_expires_with_the_cursor_ttl() {
    let (aff, clock) = affinity(16);
    aff.pin("scroll-1", ClusterId::from("eu-1"));

    // Just before the TTL it still resolves.
    clock.advance(TTL.saturating_sub(Duration::from_secs(1)));
    assert_eq!(aff.resolve("scroll-1"), Some(ClusterId::from("eu-1")));

    // At/after the TTL the stale binding is never returned.
    clock.advance(Duration::from_secs(1));
    assert_eq!(aff.resolve("scroll-1"), None);
}

#[test]
fn re_pinning_refreshes_the_binding_ttl() {
    let (aff, clock) = affinity(16);
    aff.pin("scroll-1", ClusterId::from("eu-1"));
    clock.advance(TTL.saturating_sub(Duration::from_secs(1)));
    // A continuation re-pins, extending the lease.
    aff.pin("scroll-1", ClusterId::from("eu-1"));
    clock.advance(TTL.saturating_sub(Duration::from_secs(1)));
    assert_eq!(aff.resolve("scroll-1"), Some(ClusterId::from("eu-1")));
}

#[test]
fn the_map_is_capacity_bounded_evicting_the_oldest() {
    // Capacity 2: a third pin evicts the oldest binding.
    let (aff, clock) = affinity(2);
    aff.pin("a", ClusterId::from("c-a"));
    clock.advance(Duration::from_secs(1));
    aff.pin("b", ClusterId::from("c-b"));
    clock.advance(Duration::from_secs(1));
    aff.pin("c", ClusterId::from("c-c"));

    assert!(aff.len() <= 2, "never exceeds capacity");
    assert_eq!(aff.resolve("a"), None, "oldest evicted");
    assert_eq!(aff.resolve("b"), Some(ClusterId::from("c-b")));
    assert_eq!(aff.resolve("c"), Some(ClusterId::from("c-c")));
}

#[test]
fn pin_sweeps_expired_bindings_before_enforcing_capacity() {
    // Expired entries are reclaimed on pin, so a live binding is not evicted to
    // make room when stale ones can be dropped instead.
    let (aff, clock) = affinity(2);
    aff.pin("old", ClusterId::from("c-old"));
    clock.advance(TTL); // "old" is now expired
    aff.pin("fresh1", ClusterId::from("c1"));
    aff.pin("fresh2", ClusterId::from("c2"));

    assert_eq!(aff.resolve("old"), None);
    assert_eq!(aff.resolve("fresh1"), Some(ClusterId::from("c1")));
    assert_eq!(aff.resolve("fresh2"), Some(ClusterId::from("c2")));
}

#[test]
fn release_drops_a_binding() {
    let (aff, _clock) = affinity(16);
    aff.pin("scroll-1", ClusterId::from("eu-1"));
    aff.release("scroll-1");
    assert_eq!(aff.resolve("scroll-1"), None);
    assert!(aff.is_empty());
}
