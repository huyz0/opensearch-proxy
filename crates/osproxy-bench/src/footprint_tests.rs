//! Tests for the footprint profile — growth is a saturating difference, the
//! ratio guards a zero idle reading, and the judge gates idle + growth.
#![allow(clippy::unwrap_used)]

use super::*;

const MIB: u64 = 1024 * 1024;

fn profile(idle_mib: u64, soak_mib: u64, reqs: u64) -> FootprintProfile {
    FootprintProfile {
        idle_rss_bytes: idle_mib * MIB,
        soak_rss_bytes: soak_mib * MIB,
        soak_requests: reqs,
    }
}

#[test]
fn growth_is_soak_minus_idle() {
    let p = profile(100, 130, 50_000);
    assert_eq!(p.growth_bytes(), 30 * MIB);
    assert!((p.growth_ratio() - 1.3).abs() < 1e-9);
}

#[test]
fn growth_saturates_when_the_process_shrinks() {
    // The allocator returned pages after the soak: no negative growth.
    let p = profile(130, 100, 50_000);
    assert_eq!(p.growth_bytes(), 0);
    assert!(p.growth_ratio() < 1.0, "ratio reflects the shrink");
}

#[test]
fn a_zero_idle_reading_reads_as_no_growth() {
    let p = profile(0, 0, 0);
    assert!((p.growth_ratio() - 1.0).abs() < 1e-9);
}

#[test]
fn a_bounded_footprint_passes() {
    let p = profile(80, 96, 100_000); // 80 MiB idle, 1.2x growth
    let v = judge_footprint(&p, &FootprintThresholds::provisional());
    assert!(v.pass, "within idle + growth bounds: {:?}", v.findings);
}

#[test]
fn an_oversized_idle_footprint_fails_nfr_p6() {
    let p = profile(512, 520, 100_000); // 512 MiB idle > 256 MiB bound
    let v = judge_footprint(&p, &FootprintThresholds::provisional());
    assert!(!v.pass);
    let idle = v.findings.iter().find(|f| f.nfr == "NFR-P6").unwrap();
    assert!(!idle.pass, "oversized idle must fail: {}", idle.detail);
}

#[test]
fn a_small_idle_with_a_modest_absolute_gain_passes_on_the_absolute_bound() {
    // 12 -> 23 MiB is ~1.9x (over the 1.5x ratio) but only +11 MiB absolute
    // (under the 64 MiB floor) — a normal working set, not a leak.
    let p = profile(12, 23, 50_000);
    let v = judge_footprint(&p, &FootprintThresholds::provisional());
    assert!(v.pass, "small absolute growth is fine: {:?}", v.findings);
}

#[test]
fn unbounded_growth_fails_both_bounds() {
    // 5x ratio AND +320 MiB absolute — past both the ratio and the byte floor.
    let p = profile(80, 400, 1_000_000);
    let v = judge_footprint(&p, &FootprintThresholds::provisional());
    assert!(!v.pass);
    let growth = v
        .findings
        .iter()
        .find(|f| f.nfr == "NFR-P6-growth")
        .unwrap();
    assert!(!growth.pass, "runaway growth must fail: {}", growth.detail);
}

#[test]
fn the_profile_round_trips_through_json() {
    let p = profile(80, 96, 100_000);
    let back: FootprintProfile = serde_json::from_str(&p.to_json()).unwrap();
    assert_eq!(p, back);
}
