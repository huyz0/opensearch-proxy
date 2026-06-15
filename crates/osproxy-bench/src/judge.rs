//! The automated NFR-P gate: score a [`NfrProfile`] against thresholds, emitting
//! a per-NFR verdict. This is what turns a load run's numbers into a pass/fail.

use serde::{Deserialize, Serialize};

use crate::profile::NfrProfile;

/// The bounds a [`NfrProfile`] is judged against — one field per quantitative
/// NFR-P target (`docs/01`). The targets themselves are `[CALIBRATE]` in the
/// architecture doc: the *method* is fixed (this judge) but the numbers are set
/// once a real baseline exists, so these are constructed explicitly by the caller
/// rather than hidden in a single blessed default that would masquerade as
/// validated.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct NfrThresholds {
    /// Max added p50 latency over direct-to-cluster, nanoseconds (NFR-P1).
    pub added_p50_ns_max: u64,
    /// Max added p99 latency over direct-to-cluster, nanoseconds (NFR-P2).
    pub added_p99_ns_max: u64,
    /// Min upstream connection reuse rate, `0.0..=1.0` (NFR-P4).
    pub pool_reuse_rate_min: f64,
}

impl NfrThresholds {
    /// The doc's *suggested* starting bounds (`docs/01`: added p50 ~1–2 ms, reuse
    /// ≥99 %) as a provisional placeholder. Named "provisional" deliberately:
    /// until a calibration run sets real numbers these are a sketch, not a
    /// validated SLO — callers gating CI must pass their own measured bounds.
    #[must_use]
    pub fn provisional() -> Self {
        Self {
            added_p50_ns_max: 2_000_000,  // ~2 ms
            added_p99_ns_max: 10_000_000, // ~10 ms
            pool_reuse_rate_min: 0.99,
        }
    }
}

/// One NFR's result: which target, whether it passed, and a human/LLM-readable
/// detail line naming the observed value against the bound.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    /// The NFR id this finding scores, e.g. `"NFR-P1"`. Owned so a [`Verdict`]
    /// round-trips through JSON (the gate's machine-readable output).
    pub nfr: String,
    /// Whether the profile met this NFR's bound.
    pub pass: bool,
    /// Observed-vs-bound detail, suitable for a log line or an LLM to reason over.
    pub detail: String,
}

/// The overall scoring of a profile: pass only if every finding passed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    /// `true` iff every [`Finding`] passed.
    pub pass: bool,
    /// One finding per judged NFR, in NFR-id order.
    pub findings: Vec<Finding>,
}

impl Verdict {
    /// The verdict as pretty JSON — the gate's machine-readable output.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("{{\"error\":\"verdict serialize failed: {e}\"}}"))
    }
}

/// Scores `profile` against `thresholds`, one [`Finding`] per quantitative NFR-P
/// target (P1 added p50, P2 added p99, P4 reuse rate). The verdict passes iff all
/// findings pass.
///
/// The profile's `throughput_rps` and `samples`/`concurrency` are *not* gated
/// here — they are recorded context until a steady-state target is calibrated, so
/// a green verdict means "the gated NFRs held", not "all of NFR-P passed".
///
/// Fails closed on a non-finite reuse rate: a `NaN` (e.g. a zero-traffic run's
/// 0/0) is never `>=` the floor, so it scores as a failure rather than passing.
#[must_use]
pub fn judge(profile: &NfrProfile, thresholds: &NfrThresholds) -> Verdict {
    let findings = vec![
        max_finding(
            "NFR-P1",
            "added p50",
            profile.added_p50_ns(),
            thresholds.added_p50_ns_max,
        ),
        max_finding(
            "NFR-P2",
            "added p99",
            profile.added_p99_ns(),
            thresholds.added_p99_ns_max,
        ),
        min_rate_finding(
            "NFR-P4",
            profile.pool_reuse_rate,
            thresholds.pool_reuse_rate_min,
        ),
    ];
    Verdict {
        pass: findings.iter().all(|f| f.pass),
        findings,
    }
}

/// A finding for an "observed ≤ max" latency bound (NFR-P1/P2).
fn max_finding(nfr: &str, label: &str, observed_ns: u64, max_ns: u64) -> Finding {
    Finding {
        nfr: nfr.to_owned(),
        pass: observed_ns <= max_ns,
        detail: format!(
            "{label} {:.3} ms vs bound {:.3} ms",
            ms(observed_ns),
            ms(max_ns)
        ),
    }
}

/// A finding for an "observed ≥ min" rate bound (NFR-P4).
fn min_rate_finding(nfr: &str, observed: f64, min: f64) -> Finding {
    Finding {
        nfr: nfr.to_owned(),
        pass: observed >= min,
        detail: format!("pool reuse {observed:.4} vs floor {min:.4}"),
    }
}

/// Nanoseconds as milliseconds, for readable findings. The cast is lossy only
/// above 2^52 ns (~52 days of latency), which no real measurement reaches, so the
/// precision-loss lint is suppressed here rather than complicating the formatter.
#[allow(clippy::cast_precision_loss)]
fn ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

#[cfg(test)]
#[path = "judge_tests.rs"]
mod tests;
