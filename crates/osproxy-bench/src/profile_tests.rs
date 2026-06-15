//! Tests for [`NfrProfile`] — added latency is a derived, saturating difference,
//! and the profile round-trips as JSON.
#![allow(clippy::unwrap_used)]

use super::*;

fn summary(p50: u64, p99: u64) -> LatencySummary {
    // A two-sample set whose median/p99 land on the requested values: with two
    // samples [a, b], nearest-rank p50 = a (rank 1) and p99 = b (rank 2).
    LatencySummary::from_nanos(&[p50, p99]).unwrap()
}

fn profile(baseline: LatencySummary, proxy: LatencySummary) -> NfrProfile {
    NfrProfile {
        samples: 1000,
        concurrency: 16,
        baseline,
        proxy,
        pool_reuse_rate: 0.99,
        throughput_rps: 12_000.0,
    }
}

#[test]
fn added_latency_is_the_proxy_minus_baseline_difference() {
    let p = profile(summary(1_000, 2_000), summary(2_500, 5_000));
    assert_eq!(p.added_p50_ns(), 1_500);
    assert_eq!(p.added_p99_ns(), 3_000);
}

#[test]
fn added_latency_saturates_when_the_proxy_measures_faster() {
    // Noise can make the proxy side look faster than direct; overhead floors at 0.
    let p = profile(summary(2_000, 4_000), summary(1_000, 1_000));
    assert_eq!(p.added_p50_ns(), 0, "never reports negative overhead");
    assert_eq!(p.added_p99_ns(), 0);
}

#[test]
fn the_profile_round_trips_through_json() {
    let p = profile(summary(1_000, 2_000), summary(2_500, 5_000));
    let back: NfrProfile = serde_json::from_str(&p.to_json()).unwrap();
    assert_eq!(p, back);
}
