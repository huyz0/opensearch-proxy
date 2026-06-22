//! The scalability curve: how the proxy's tail latency and throughput move as
//! offered concurrency climbs. NFR-P2 bounds *tail amplification*, a healthy
//! proxy serves more in-flight requests by reusing its pool, not by letting p99
//! blow up, so a curve is a sweep of [`LatencySummary`] at rising concurrency
//! and a judge over its shape.

use serde::{Deserialize, Serialize};

use crate::judge::{Finding, Verdict};
use crate::summary::LatencySummary;

/// One point on the curve: the proxy driven at a fixed `concurrency`, with the
/// resulting latency distribution and sustained rate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScalabilityPoint {
    /// Offered in-flight request count this point was driven at.
    pub concurrency: u32,
    /// Latency distribution observed at this concurrency.
    pub latency: LatencySummary,
    /// Sustained request rate at this concurrency, requests/second.
    pub throughput_rps: f64,
}

/// A concurrency sweep: the same proxy measured at increasing offered
/// concurrency. The points are kept in the order they were swept (ascending
/// concurrency); the judge reads the first and last to bound how much the tail
/// grew across the range.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScalabilityCurve {
    /// Sweep points, ascending by concurrency (`points[0]` is the lightest load).
    pub points: Vec<ScalabilityPoint>,
}

impl ScalabilityCurve {
    /// A curve from its sweep points. Returns `None` for an empty sweep, a curve
    /// with no points has no shape to judge.
    #[must_use]
    pub fn new(points: Vec<ScalabilityPoint>) -> Option<Self> {
        if points.is_empty() {
            return None;
        }
        Some(Self { points })
    }

    /// Tail-amplification ratio: p99 at the heaviest load over p99 at the
    /// lightest. `1.0` is a flat tail (ideal pooling); `> 1.0` means the tail grew
    /// under load. Guards a zero/absent lightest-load p99 by returning `1.0` (no
    /// measurable amplification rather than a divide-by-zero).
    ///
    /// The `u64 -> f64` cast is lossy only above 2^52 ns (~52 days), which no
    /// latency sample reaches, so the precision-loss lint is suppressed.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn tail_amplification(&self) -> f64 {
        let first = self.points.first();
        let last = self.points.last();
        match (first, last) {
            (Some(f), Some(l)) if f.latency.p99_ns > 0 => {
                l.latency.p99_ns as f64 / f.latency.p99_ns as f64
            }
            _ => 1.0,
        }
    }

    /// Throughput scaling: peak sustained rate over the lightest-load rate. `> 1.0`
    /// means the proxy did more work as concurrency rose (it scaled); `<= 1.0`
    /// means added concurrency bought no throughput (saturated or collapsing).
    #[must_use]
    pub fn throughput_scaling(&self) -> f64 {
        let base = self.points.first().map_or(0.0, |p| p.throughput_rps);
        let peak = self
            .points
            .iter()
            .map(|p| p.throughput_rps)
            .fold(0.0_f64, f64::max);
        if base > 0.0 {
            peak / base
        } else {
            0.0
        }
    }
}

/// The bounds a [`ScalabilityCurve`] is judged against (`docs/01` NFR-P2). Like
/// [`crate::NfrThresholds`], the numbers are `[CALIBRATE]`: set from a real sweep,
/// so the caller supplies them rather than trusting a blessed default.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScalabilityThresholds {
    /// Max acceptable tail-amplification ratio across the sweep (NFR-P2): how much
    /// p99 may grow from the lightest to the heaviest load.
    pub max_tail_amplification: f64,
    /// Min throughput scaling the sweep must show, proof that added concurrency
    /// actually buys work and the proxy is not serializing requests.
    pub min_throughput_scaling: f64,
}

impl ScalabilityThresholds {
    /// A provisional placeholder (not a validated SLO): tail may at most triple
    /// across the sweep, and throughput must at least double. Replace with values
    /// from a calibrated sweep before gating CI.
    #[must_use]
    pub fn provisional() -> Self {
        Self {
            max_tail_amplification: 3.0,
            min_throughput_scaling: 2.0,
        }
    }
}

/// Scores a curve against `thresholds`: one finding for tail amplification
/// (NFR-P2) and one for throughput scaling. Passes iff both hold. Fails closed on
/// a non-finite ratio (e.g. a degenerate sweep), like the latency judge.
#[must_use]
pub fn judge_scalability(curve: &ScalabilityCurve, thresholds: &ScalabilityThresholds) -> Verdict {
    let amp = curve.tail_amplification();
    let scaling = curve.throughput_scaling();
    let findings = vec![
        Finding {
            nfr: "NFR-P2".to_owned(),
            pass: amp.is_finite() && amp <= thresholds.max_tail_amplification,
            detail: format!(
                "tail amplification {amp:.2}x vs bound {:.2}x",
                thresholds.max_tail_amplification
            ),
        },
        Finding {
            nfr: "NFR-P2-scaling".to_owned(),
            pass: scaling.is_finite() && scaling >= thresholds.min_throughput_scaling,
            detail: format!(
                "throughput scaling {scaling:.2}x vs floor {:.2}x",
                thresholds.min_throughput_scaling
            ),
        },
    ];
    Verdict {
        pass: findings.iter().all(|f| f.pass),
        findings,
    }
}

#[cfg(test)]
#[path = "scale_tests.rs"]
mod tests;
