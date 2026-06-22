//! Unit tests for the snapshot-application logic, the fail-safe "keep last good"
//! rule and the cheap `load`. The watch/connect path is exercised by the
//! Docker-gated `tests/etcd_live.rs` integration test.

use super::*;

use osproxy_core::ManualClock;

const GOOD: &[u8] = br#"{"directives":[{"id":"a","level":"Shape","ttl_secs":600}]}"#;

#[test]
fn a_valid_value_is_applied_and_load_reflects_it() {
    let clock = ManualClock::new();
    let current = Arc::new(ArcSwap::from_pointee(DirectiveSet::new()));
    let store = EtcdDirectiveStore {
        current: Arc::clone(&current),
    };
    assert_eq!(store.load().len(), 0, "starts empty");

    apply_value(&current, GOOD, &clock);
    assert_eq!(store.load().len(), 1, "the published set is now live");
}

#[test]
fn a_malformed_value_keeps_the_last_good_snapshot() {
    // A bad publish must never blank fleet diagnostics, the previous set stays.
    let clock = ManualClock::new();
    let current = Arc::new(ArcSwap::from_pointee(DirectiveSet::new()));
    apply_value(&current, GOOD, &clock);

    apply_value(&current, b"not json", &clock);
    let store = EtcdDirectiveStore {
        current: Arc::clone(&current),
    };
    assert_eq!(
        store.load().len(),
        1,
        "the malformed update was rejected, last-good kept"
    );

    // A fail-closed field (typo'd target) is also rejected wholesale.
    apply_value(
        &current,
        br#"{"directives":[{"id":"b","level":"Shape","ttl_secs":60,"tennant":"acme"}]}"#,
        &clock,
    );
    assert_eq!(
        store.load().len(),
        1,
        "an unknown-key publish is rejected too"
    );
}
