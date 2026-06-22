//! Latency percentiles over a set of samples, the nearest-rank summary that
//! reduces a load run's raw timings to the few numbers the NFRs are stated in.

use serde::{Deserialize, Serialize};

/// A latency distribution reduced to the order statistics the NFR-P targets use
/// (`docs/01`): the median and the p99 tail, with min/max/mean for context.
///
/// Built from raw nanosecond samples with [`LatencySummary::from_nanos`]. The
/// percentiles use the **nearest-rank** method (no interpolation), so a summary
/// is an exact function of its samples, two runs with identical samples produce
/// byte-identical summaries, which is what lets [`crate::judge()`] gate on them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySummary {
    /// Number of samples the summary was computed from.
    pub count: u64,
    /// Smallest sample, in nanoseconds.
    pub min_ns: u64,
    /// Largest sample, in nanoseconds.
    pub max_ns: u64,
    /// Arithmetic mean, in nanoseconds (integer; sub-nanosecond is not material).
    pub mean_ns: u64,
    /// 50th percentile (median), nearest-rank, in nanoseconds.
    pub p50_ns: u64,
    /// 90th percentile, nearest-rank, in nanoseconds.
    pub p90_ns: u64,
    /// 99th percentile (the tail NFR-P2 bounds), nearest-rank, in nanoseconds.
    pub p99_ns: u64,
}

impl LatencySummary {
    /// Summarizes `samples` (nanosecond latencies). Returns `None` for an empty
    /// set, a percentile of no observations is undefined, and a caller must not
    /// silently treat "no data" as "fast".
    #[must_use]
    pub fn from_nanos(samples: &[u64]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let count = sorted.len();
        // Sum in u128 so a long run of large samples cannot overflow before the
        // divide; the mean itself fits back in u64.
        let sum: u128 = sorted.iter().map(|&s| u128::from(s)).sum();
        let mean_ns = u64::try_from(sum / count as u128).unwrap_or(u64::MAX);
        Some(Self {
            count: count as u64,
            min_ns: sorted[0],
            max_ns: sorted[count - 1],
            mean_ns,
            p50_ns: percentile(&sorted, 50),
            p90_ns: percentile(&sorted, 90),
            p99_ns: percentile(&sorted, 99),
        })
    }
}

/// The nearest-rank percentile of an already-sorted, non-empty slice: the value
/// at rank `ceil(p/100 * n)`, 1-based, clamped into range. `p` is a whole
/// percentile in `1..=100`.
fn percentile(sorted: &[u64], p: usize) -> u64 {
    debug_assert!(!sorted.is_empty(), "percentile of empty slice");
    let n = sorted.len();
    // rank = ceil(p * n / 100), computed in usize without floats so it is exact
    // and indexes the slice directly (no narrowing cast).
    let rank = (p * n).div_ceil(100).max(1);
    let idx = (rank - 1).min(n - 1);
    sorted[idx]
}

#[cfg(test)]
#[path = "summary_tests.rs"]
mod tests;
