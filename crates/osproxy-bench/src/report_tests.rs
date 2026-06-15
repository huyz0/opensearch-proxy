//! Tests for the Markdown briefs — they surface the load-bearing numbers and the
//! pass/fail verdict, and reflect the verdict's polarity.
#![allow(clippy::unwrap_used)]

use super::*;
use crate::scale::ScalabilityPoint;
use crate::{
    judge, judge_footprint, judge_scalability, FootprintProfile, FootprintThresholds,
    LatencySummary, NfrThresholds, ScalabilityThresholds,
};

fn summary(p50: u64, p99: u64) -> LatencySummary {
    LatencySummary::from_nanos(&[p50, p99]).unwrap()
}

#[test]
fn the_profile_brief_shows_added_latency_reuse_and_verdict() {
    let profile = NfrProfile {
        samples: 2000,
        concurrency: 16,
        baseline: summary(1_000_000, 2_000_000),
        proxy: summary(1_080_000, 2_500_000),
        pool_reuse_rate: 0.999,
        throughput_rps: 12_000.0,
    };
    let verdict = judge(&profile, &NfrThresholds::provisional());
    let md = profile_brief(&profile, &verdict);
    assert!(md.contains("### NFR-P latency & reuse"));
    assert!(md.contains("added p50"));
    assert!(md.contains("pool reuse"));
    assert!(md.contains("PASS"), "healthy profile reads PASS:\n{md}");
    assert!(md.contains("NFR-P1"), "names the gated NFRs");
}

#[test]
fn the_scalability_brief_lists_every_point_and_the_scaling_summary() {
    let curve = ScalabilityCurve::new(vec![
        ScalabilityPoint {
            concurrency: 1,
            latency: summary(1_000_000, 2_000_000),
            throughput_rps: 50.0,
        },
        ScalabilityPoint {
            concurrency: 32,
            latency: summary(1_200_000, 4_000_000),
            throughput_rps: 1200.0,
        },
    ])
    .unwrap();
    let verdict = judge_scalability(&curve, &ScalabilityThresholds::provisional());
    let md = scalability_brief(&curve, &verdict);
    assert!(md.contains("### NFR-P2 scalability"));
    assert!(
        md.contains("| 1 |") && md.contains("| 32 |"),
        "rows per point:\n{md}"
    );
    assert!(md.contains("throughput scaling"));
}

#[test]
fn the_footprint_brief_reflects_a_failing_verdict() {
    // Oversized idle → the verdict fails, and the brief must say so.
    let profile = FootprintProfile {
        idle_rss_bytes: 512 * 1024 * 1024,
        soak_rss_bytes: 520 * 1024 * 1024,
        soak_requests: 50_000,
    };
    let verdict = judge_footprint(&profile, &FootprintThresholds::provisional());
    let md = footprint_brief(&profile, &verdict);
    assert!(md.contains("### NFR-P6 footprint"));
    assert!(md.contains("idle RSS"));
    assert!(md.contains("FAIL"), "oversized idle reads FAIL:\n{md}");
}

#[test]
fn the_header_is_a_markdown_h2() {
    assert_eq!(brief_header("Run X"), "## Run X\n\n");
}
