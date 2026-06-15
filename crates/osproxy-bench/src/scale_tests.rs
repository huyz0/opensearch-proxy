//! Tests for the scalability curve — tail amplification and throughput scaling
//! are read from the sweep's ends, an empty sweep has no curve, and the judge
//! fails closed.
#![allow(clippy::unwrap_used)]

use super::*;
use crate::LatencySummary;

/// A point whose p99 is `p99_ns` and sustained rate is `rps`, at `concurrency`.
fn point(concurrency: u32, p99_ns: u64, rps: f64) -> ScalabilityPoint {
    // A two-sample set lands p99 on the second value (nearest-rank rank 2 of 2).
    ScalabilityPoint {
        concurrency,
        latency: LatencySummary::from_nanos(&[p99_ns / 2, p99_ns]).unwrap(),
        throughput_rps: rps,
    }
}

#[test]
fn an_empty_sweep_has_no_curve() {
    assert!(ScalabilityCurve::new(vec![]).is_none());
}

#[test]
fn tail_amplification_is_heaviest_over_lightest_p99() {
    let curve = ScalabilityCurve::new(vec![
        point(1, 1_000, 100.0),
        point(8, 1_500, 600.0),
        point(64, 4_000, 1_200.0),
    ])
    .unwrap();
    // 4000 / 1000 = 4x tail growth across the sweep.
    assert!((curve.tail_amplification() - 4.0).abs() < 1e-9);
    // Peak 1200 / base 100 = 12x throughput scaling.
    assert!((curve.throughput_scaling() - 12.0).abs() < 1e-9);
}

#[test]
fn a_flat_tail_that_scales_passes() {
    let curve =
        ScalabilityCurve::new(vec![point(1, 1_000, 100.0), point(16, 1_200, 800.0)]).unwrap();
    let v = judge_scalability(&curve, &ScalabilityThresholds::provisional());
    assert!(v.pass, "1.2x tail, 8x scaling is healthy: {:?}", v.findings);
}

#[test]
fn a_blowing_up_tail_fails_nfr_p2() {
    let curve = ScalabilityCurve::new(vec![
        point(1, 1_000, 100.0),
        point(64, 10_000, 800.0), // 10x tail growth — amplification
    ])
    .unwrap();
    let v = judge_scalability(&curve, &ScalabilityThresholds::provisional());
    assert!(!v.pass);
    let amp = v.findings.iter().find(|f| f.nfr == "NFR-P2").unwrap();
    assert!(!amp.pass, "tail blow-up must fail: {}", amp.detail);
}

#[test]
fn throughput_that_does_not_scale_fails() {
    // Flat tail, but added concurrency bought no throughput (proxy serializing).
    let curve = ScalabilityCurve::new(vec![
        point(1, 1_000, 500.0),
        point(64, 1_100, 520.0), // only 1.04x scaling
    ])
    .unwrap();
    let v = judge_scalability(&curve, &ScalabilityThresholds::provisional());
    assert!(!v.pass);
    let scaling = v
        .findings
        .iter()
        .find(|f| f.nfr == "NFR-P2-scaling")
        .unwrap();
    assert!(!scaling.pass, "non-scaling must fail: {}", scaling.detail);
}

#[test]
fn the_curve_round_trips_through_json() {
    let curve =
        ScalabilityCurve::new(vec![point(1, 1_000, 100.0), point(16, 1_200, 800.0)]).unwrap();
    let json = serde_json::to_string(&curve).unwrap();
    let back: ScalabilityCurve = serde_json::from_str(&json).unwrap();
    assert_eq!(curve, back);
}
