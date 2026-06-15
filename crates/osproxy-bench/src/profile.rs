//! The machine-readable NFR-P profile: what a load run *produces* and an
//! operator (or an LLM) *reads*. Proxy-vs-baseline added latency is derived here.

use serde::{Deserialize, Serialize};

use crate::summary::LatencySummary;

/// A single load run's performance profile: the proxy measured against a direct-
/// to-cluster baseline under the same workload, plus the steady-state numbers the
/// NFRs bound.
///
/// The two summaries are gathered the same way against the same cluster — the
/// only difference is whether the request went *through the proxy* or *direct* —
/// so their difference isolates the proxy's overhead. That difference is computed
/// by [`NfrProfile::added_p50_ns`] / [`NfrProfile::added_p99_ns`] rather than
/// stored, so "added latency" is defined in exactly one place and a profile can
/// never carry an inconsistent baseline/proxy/added triple.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NfrProfile {
    /// Number of requests issued on each side of the comparison.
    pub samples: u64,
    /// Concurrency the run was driven at (the in-flight request count).
    pub concurrency: u32,
    /// Latency talking **directly** to the cluster — the baseline.
    pub baseline: LatencySummary,
    /// Latency talking to the cluster **through the proxy**.
    pub proxy: LatencySummary,
    /// Upstream connection reuse rate over the run, `0.0..=1.0` (NFR-P4): reused
    /// dispatches over total dispatches. Supplied by the load runner from the
    /// proxy's `PoolStats` (reused/dispatched); not computed in this pure crate.
    pub pool_reuse_rate: f64,
    /// Sustained request rate the proxy held over the run, requests/second
    /// (NFR-P2 steady state). Recorded for the operator; supplied by the load
    /// runner and not gated by [`judge`](crate::judge()) until a target is set.
    pub throughput_rps: f64,
}

impl NfrProfile {
    /// Added median latency the proxy imposes over direct-to-cluster, in
    /// nanoseconds (NFR-P1). Saturating: a proxy that happens to measure *faster*
    /// than the baseline on a noisy run reports zero added latency, never a
    /// nonsensical "negative overhead".
    #[must_use]
    pub fn added_p50_ns(&self) -> u64 {
        self.proxy.p50_ns.saturating_sub(self.baseline.p50_ns)
    }

    /// Added tail (p99) latency the proxy imposes over direct-to-cluster, in
    /// nanoseconds (NFR-P2). Saturating, for the same reason as
    /// [`NfrProfile::added_p50_ns`].
    #[must_use]
    pub fn added_p99_ns(&self) -> u64 {
        self.proxy.p99_ns.saturating_sub(self.baseline.p99_ns)
    }

    /// The profile as pretty JSON — the artifact a load run writes out and a
    /// judge (human or LLM) reads. Serialization of plain numeric fields cannot
    /// fail, so a serializer error collapses to an explicit error string rather
    /// than a panic.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("{{\"error\":\"profile serialize failed: {e}\"}}"))
    }
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
