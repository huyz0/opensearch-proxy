//! The memory-footprint profile: the proxy's resident set when idle and after a
//! soak (`docs/01` NFR-P6, "idle footprint bounded by config; no unbounded
//! buffers/queues"). Two RSS readings of the proxy process plus the soak size,
//! and a judge over the absolute idle footprint and how much it grew.

use serde::{Deserialize, Serialize};

use crate::judge::{Finding, Verdict};

/// A footprint measurement: the proxy process's resident set size (RSS) when
/// idle and again after sustaining `soak_requests`. The two readings are taken
/// from the *same* process, so their difference is the proxy's own growth under
/// load, the signal that catches an unbounded buffer or queue (NFR-P6), since a
/// well-behaved proxy returns near its idle footprint after a soak.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FootprintProfile {
    /// Resident set after startup settles, before load, the idle footprint.
    pub idle_rss_bytes: u64,
    /// Resident set after the soak completed.
    pub soak_rss_bytes: u64,
    /// Number of requests driven through the proxy during the soak.
    pub soak_requests: u64,
}

impl FootprintProfile {
    /// Bytes the resident set grew over the soak. Saturating: a process that
    /// measures *smaller* after the soak (the allocator returned pages) reports
    /// zero growth, never a nonsensical negative.
    #[must_use]
    pub fn growth_bytes(&self) -> u64 {
        self.soak_rss_bytes.saturating_sub(self.idle_rss_bytes)
    }

    /// Post-soak RSS as a multiple of idle RSS. `1.0` is "returned to idle"; a
    /// ratio climbing with `soak_requests` is the fingerprint of an unbounded
    /// buffer. Guards a zero idle reading (unmeasurable) by returning `1.0`.
    ///
    /// The `u64 -> f64` casts are exact below 2^52 bytes (4 PiB), so the
    /// precision-loss lint is suppressed.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn growth_ratio(&self) -> f64 {
        if self.idle_rss_bytes == 0 {
            return 1.0;
        }
        // Both fit well under 2^52, so the f64 conversion is exact.
        self.soak_rss_bytes as f64 / self.idle_rss_bytes as f64
    }

    /// The profile as pretty JSON, the artifact a soak run writes and a judge
    /// reads. Plain numeric fields can't fail to serialize; a serializer error
    /// collapses to an explicit error string rather than a panic.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("{{\"error\":\"footprint serialize failed: {e}\"}}"))
    }
}

/// The bounds a [`FootprintProfile`] is judged against (NFR-P6). `[CALIBRATE]`
/// like the other thresholds: the caller supplies measured numbers rather than
/// trusting a blessed default.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct FootprintThresholds {
    /// Max acceptable idle resident set, bytes (NFR-P6 "idle footprint").
    pub max_idle_rss_bytes: u64,
    /// Max acceptable post-soak/idle RSS ratio, one half of the leak guard.
    pub max_growth_ratio: f64,
    /// Max acceptable *absolute* soak growth, bytes, the other half. Growth
    /// passes if it is within **either** bound: a proportional ratio is the right
    /// guard for a large footprint, but for a small idle footprint a normal
    /// steady-state working set (per-connection buffers, allocator arenas) is a
    /// large *ratio* yet a trivially small *absolute* gain. An unbounded buffer
    /// blows past both; a healthy working set stays under at least one.
    pub max_growth_bytes: u64,
}

impl FootprintThresholds {
    /// A provisional placeholder (not a validated SLO): idle ≤ 256 MiB, and soak
    /// growth within 1.5× **or** 64 MiB absolute. Replace with values from a real
    /// soak before gating CI.
    #[must_use]
    pub fn provisional() -> Self {
        Self {
            max_idle_rss_bytes: 256 * 1024 * 1024,
            max_growth_ratio: 1.5,
            max_growth_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Scores a footprint against `thresholds`: one finding for the idle footprint
/// and one for soak growth (the unbounded-buffer guard). Passes iff both hold.
/// Growth passes within **either** the ratio or the absolute-bytes bound; it
/// fails closed on a non-finite ratio.
#[must_use]
pub fn judge_footprint(profile: &FootprintProfile, thresholds: &FootprintThresholds) -> Verdict {
    let ratio = profile.growth_ratio();
    let growth = profile.growth_bytes();
    let within_ratio = ratio.is_finite() && ratio <= thresholds.max_growth_ratio;
    let within_abs = growth <= thresholds.max_growth_bytes;
    let findings = vec![
        Finding {
            nfr: "NFR-P6".to_owned(),
            pass: profile.idle_rss_bytes <= thresholds.max_idle_rss_bytes,
            detail: format!(
                "idle {:.1} MiB vs bound {:.1} MiB",
                mib(profile.idle_rss_bytes),
                mib(thresholds.max_idle_rss_bytes)
            ),
        },
        Finding {
            nfr: "NFR-P6-growth".to_owned(),
            pass: within_ratio || within_abs,
            detail: format!(
                "soak growth {ratio:.2}x / {:.1} MiB vs bound {:.2}x or {:.1} MiB over {} reqs",
                mib(growth),
                thresholds.max_growth_ratio,
                mib(thresholds.max_growth_bytes),
                profile.soak_requests
            ),
        },
    ];
    Verdict {
        pass: findings.iter().all(|f| f.pass),
        findings,
    }
}

/// Bytes as mebibytes, for readable findings. Lossy only above 2^52 bytes (far
/// beyond any process), so the precision-loss lint is suppressed.
#[allow(clippy::cast_precision_loss)]
fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[cfg(test)]
#[path = "footprint_tests.rs"]
mod tests;
