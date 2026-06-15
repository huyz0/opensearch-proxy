//! Tests for the metrics snapshot — counters tally by outcome, the snapshot
//! reflects them, and it round-trips as JSON.
#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn record_tallies_total_ok_and_error() {
    let m = Metrics::new();
    m.record(true);
    m.record(true);
    m.record(false);
    let snap = m.snapshot(vec![]);
    assert_eq!(snap.requests_total, 3);
    assert_eq!(snap.requests_ok, 2);
    assert_eq!(snap.requests_error, 1);
}

#[test]
fn a_fresh_collector_is_all_zero() {
    let snap = Metrics::new().snapshot(vec![]);
    assert_eq!(snap.requests_total, 0);
    assert_eq!(snap.requests_ok, 0);
    assert_eq!(snap.requests_error, 0);
    assert!(snap.pools.is_empty());
}

#[test]
fn the_snapshot_carries_per_cluster_pool_reuse() {
    let pools = vec![PoolSnapshot {
        cluster: "eu-1".to_owned(),
        opened: 2,
        dispatched: 100,
        reused: 98,
    }];
    let snap = Metrics::new().snapshot(pools);
    assert_eq!(snap.pools.len(), 1);
    assert_eq!(snap.pools[0].cluster, "eu-1");
    assert_eq!(snap.pools[0].reused, 98);
}

#[test]
fn the_snapshot_round_trips_through_json() {
    let m = Metrics::new();
    m.record(true);
    let snap = m.snapshot(vec![PoolSnapshot {
        cluster: "eu-1".to_owned(),
        opened: 1,
        dispatched: 10,
        reused: 9,
    }]);
    let back: StatsSnapshot = serde_json::from_str(&snap.to_json()).unwrap();
    assert_eq!(snap, back);
}
