//! The always-on operational metrics snapshot — the one observability surface
//! that works in **every** environment, including production where the
//! `/debug/*` introspection tools are off.
//!
//! An external agent (or a Prometheus-style scraper) polls each proxy's snapshot
//! to see what it is doing: how much traffic it served, how it fared, and whether
//! its upstream pools are amortizing handshakes. The readout is deliberately
//! **per instance** — building a fleet-wide rollup is the job of the external
//! metrics/log aggregator the deployment already runs, not of the proxy. The
//! proxy's only obligation is to expose a clean, shape-only source to scrape.
//!
//! **Shape-only by construction** (`docs/05`): counts, rates, and cluster *ids* —
//! never tenant values, document bodies, query literals, or principals. A counter
//! cannot become a value-leak channel, so the snapshot is safe to expose
//! unauthenticated and to ship anywhere.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Live, lock-free counters a proxy increments as it serves data-plane requests.
/// Cheap enough to update on every request (three relaxed atomic adds); the
/// snapshot is taken on demand when an agent scrapes.
// The `requests_*` prefix is the intended metric naming (flat, scraper-friendly),
// not accidental field-name repetition.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Default)]
pub struct Metrics {
    requests_total: AtomicU64,
    requests_ok: AtomicU64,
    requests_error: AtomicU64,
}

impl Metrics {
    /// A fresh zeroed collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one completed data-plane request and whether it succeeded (a 2xx
    /// response). Introspection requests (`/debug/*`, `/metrics`) are not counted
    /// — this measures the proxy's actual proxying.
    pub fn record(&self, ok: bool) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        let bucket = if ok {
            &self.requests_ok
        } else {
            &self.requests_error
        };
        bucket.fetch_add(1, Ordering::Relaxed);
    }

    /// Builds a serializable snapshot from the current counters and the supplied
    /// per-cluster pool readout (gathered by the caller, which owns the sink).
    #[must_use]
    pub fn snapshot(&self, pools: Vec<PoolSnapshot>) -> StatsSnapshot {
        StatsSnapshot {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            requests_ok: self.requests_ok.load(Ordering::Relaxed),
            requests_error: self.requests_error.load(Ordering::Relaxed),
            pools,
        }
    }
}

/// One upstream cluster's connection-reuse counters — the signal that the pool is
/// amortizing TLS/TCP handshakes (`opened` far below `dispatched`). `cluster` is
/// an infrastructure id, not tenant data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolSnapshot {
    /// The cluster id (infrastructure identifier).
    pub cluster: String,
    /// Connections the pool opened to the cluster (cold handshakes).
    pub opened: u64,
    /// Requests dispatched to the cluster (cold + reused).
    pub dispatched: u64,
    /// Requests that rode a reused pooled connection (`dispatched - opened`).
    pub reused: u64,
}

/// A single proxy instance's operational snapshot. Per-instance by definition;
/// the fleet rollup is the external aggregator's job. Safe to serve
/// unauthenticated — it is shape-only.
// `requests_*` is the intended flat metric naming, not field-name repetition.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsSnapshot {
    /// Data-plane requests served since start.
    pub requests_total: u64,
    /// Of those, how many responded 2xx.
    pub requests_ok: u64,
    /// Of those, how many responded with an error status.
    pub requests_error: u64,
    /// Per-cluster upstream pool reuse counters.
    pub pools: Vec<PoolSnapshot>,
}

impl StatsSnapshot {
    /// The snapshot as compact JSON — what a scrape returns and an agent parses.
    /// Serialization of plain counters cannot fail; an error collapses to an
    /// explicit error object rather than a panic.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|e| format!("{{\"error\":\"stats serialize failed: {e}\"}}"))
    }
}

#[cfg(test)]
#[path = "stats_tests.rs"]
mod tests;
