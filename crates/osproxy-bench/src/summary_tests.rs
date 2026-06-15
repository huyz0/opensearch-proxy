//! Tests for [`LatencySummary`] — the percentile math must be exact and
//! deterministic, and refuse to summarize nothing.
#![allow(clippy::unwrap_used)]

use super::*;

#[test]
fn an_empty_sample_set_has_no_summary() {
    assert!(LatencySummary::from_nanos(&[]).is_none());
}

#[test]
fn a_single_sample_is_every_percentile() {
    let s = LatencySummary::from_nanos(&[42]).unwrap();
    assert_eq!(s.count, 1);
    assert_eq!(s.min_ns, 42);
    assert_eq!(s.max_ns, 42);
    assert_eq!(s.mean_ns, 42);
    assert_eq!(s.p50_ns, 42);
    assert_eq!(s.p99_ns, 42);
}

#[test]
fn percentiles_use_nearest_rank_over_1_to_100() {
    // 1..=100 ns. Nearest-rank: p50 -> rank 50 -> value 50; p90 -> 90; p99 -> 99.
    let samples: Vec<u64> = (1..=100).collect();
    let s = LatencySummary::from_nanos(&samples).unwrap();
    assert_eq!(s.count, 100);
    assert_eq!(s.min_ns, 1);
    assert_eq!(s.max_ns, 100);
    assert_eq!(s.p50_ns, 50);
    assert_eq!(s.p90_ns, 90);
    assert_eq!(s.p99_ns, 99);
    assert_eq!(s.mean_ns, 50); // (1+..+100)/100 = 5050/100 = 50 (integer)
}

#[test]
fn input_order_does_not_change_the_summary() {
    let mut shuffled = vec![5, 1, 4, 2, 3, 100, 50, 9, 8, 7];
    let a = LatencySummary::from_nanos(&shuffled).unwrap();
    shuffled.reverse();
    let b = LatencySummary::from_nanos(&shuffled).unwrap();
    assert_eq!(a, b, "summary must be independent of sample order");
}

#[test]
fn the_p99_tracks_the_tail_not_the_max() {
    // 99 fast samples and one slow outlier: p99 is still fast, max is the spike.
    let mut samples = vec![10u64; 99];
    samples.push(1_000_000);
    let s = LatencySummary::from_nanos(&samples).unwrap();
    assert_eq!(
        s.p99_ns, 10,
        "p99 of 100 samples is rank 99 — still the fast body"
    );
    assert_eq!(s.max_ns, 1_000_000, "the outlier shows only in max");
}

#[test]
fn the_summary_round_trips_through_json() {
    let s = LatencySummary::from_nanos(&[1, 2, 3, 4]).unwrap();
    let json = serde_json::to_string(&s).unwrap();
    let back: LatencySummary = serde_json::from_str(&json).unwrap();
    assert_eq!(s, back);
}
