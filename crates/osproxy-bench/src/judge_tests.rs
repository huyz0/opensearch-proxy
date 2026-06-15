//! Tests for the NFR-P [`judge`] — a profile passes only when every bound holds,
//! and each finding names the NFR it scores.
#![allow(clippy::unwrap_used)]

use super::*;
use crate::LatencySummary;

/// A profile with the given added-latency and reuse characteristics: baseline is
/// fixed, proxy is baseline + the requested overhead, reuse as given.
fn profile_with(added_p50_ns: u64, added_p99_ns: u64, reuse: f64) -> NfrProfile {
    let baseline = LatencySummary::from_nanos(&[1_000, 2_000]).unwrap(); // p50=1k p99=2k
    let proxy = LatencySummary::from_nanos(&[1_000 + added_p50_ns, 2_000 + added_p99_ns]).unwrap();
    NfrProfile {
        samples: 1000,
        concurrency: 16,
        baseline,
        proxy,
        pool_reuse_rate: reuse,
        throughput_rps: 12_000.0,
    }
}

#[test]
fn a_profile_inside_every_bound_passes() {
    let p = profile_with(500_000, 3_000_000, 0.999);
    let v = judge(&p, &NfrThresholds::provisional());
    assert!(v.pass, "all NFRs met: {:?}", v.findings);
    assert_eq!(v.findings.len(), 3);
    assert!(v.findings.iter().all(|f| f.pass));
}

#[test]
fn exceeding_added_p50_fails_only_that_nfr() {
    // 5 ms added p50 > 2 ms provisional bound; tail and reuse stay fine.
    let p = profile_with(5_000_000, 3_000_000, 0.999);
    let v = judge(&p, &NfrThresholds::provisional());
    assert!(!v.pass);
    let p1 = v.findings.iter().find(|f| f.nfr == "NFR-P1").unwrap();
    assert!(!p1.pass, "NFR-P1 should fail: {}", p1.detail);
    assert!(v.findings.iter().find(|f| f.nfr == "NFR-P2").unwrap().pass);
    assert!(v.findings.iter().find(|f| f.nfr == "NFR-P4").unwrap().pass);
}

#[test]
fn low_pool_reuse_fails_nfr_p4() {
    let p = profile_with(500_000, 3_000_000, 0.80);
    let v = judge(&p, &NfrThresholds::provisional());
    assert!(!v.pass);
    let p4 = v.findings.iter().find(|f| f.nfr == "NFR-P4").unwrap();
    assert!(!p4.pass, "reuse below floor must fail: {}", p4.detail);
}

#[test]
fn a_non_finite_reuse_rate_fails_closed() {
    // A zero-traffic run could supply 0/0 = NaN; it must score as a failure, not
    // sail past the floor (NaN comparisons are always false).
    let p = profile_with(500_000, 3_000_000, f64::NAN);
    let v = judge(&p, &NfrThresholds::provisional());
    assert!(!v.pass);
    let p4 = v.findings.iter().find(|f| f.nfr == "NFR-P4").unwrap();
    assert!(!p4.pass, "NaN reuse must fail closed: {}", p4.detail);
}

#[test]
fn the_verdict_round_trips_through_json() {
    let p = profile_with(500_000, 3_000_000, 0.999);
    let v = judge(&p, &NfrThresholds::provisional());
    let back: Verdict = serde_json::from_str(&v.to_json()).unwrap();
    assert_eq!(v, back);
}
