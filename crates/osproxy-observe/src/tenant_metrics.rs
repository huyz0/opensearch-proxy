//! Opt-in, bounded per-tenant (partition) request counters.
//!
//! [`Metrics`](crate::Metrics) stays shape-only on purpose: aggregate counts,
//! never a tenant dimension. [`TenantMetrics`] is the deliberate exception,
//! answering "which tenant is failing or slow" — a question the aggregate
//! can't. It is opt-in and off by default (no deployment pays for it unless
//! it asks); a partition id is treated as an id throughout this crate (it
//! already appears in `/debug/explain`'s `partition_id` field, `docs/05`),
//! never a redacted value, so using it as a metrics label is the same
//! trust boundary this crate already draws elsewhere.
//!
//! Cardinality is bounded two ways, not one: a tenant idle for `idle_ttl` is
//! evicted, and a hard `max_tenants` cap protects against a burst of one-off
//! or adversarial tenant ids before the idle timer would ever apply. Exported
//! cardinality therefore tracks how many tenants are live *right now*, never
//! how many have ever existed — the realistic shape for a fleet (thousands of
//! tenants arriving over minutes, not millions at once).

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use moka::sync::Cache;

/// The default bound: 50k live tenants, 15 minute idle eviction.
pub const DEFAULT_MAX_TENANTS: u64 = 50_000;

/// The default idle window before a tenant with no traffic is evicted.
pub const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(15 * 60);

/// One tenant's counters. Plain atomics behind the cache's own `Arc`, so a
/// concurrent `record` never blocks a concurrent snapshot/export.
#[derive(Debug, Default)]
struct Counters {
    requests: AtomicU64,
    failures: AtomicU64,
    duration_nanos: AtomicU64,
}

/// One tenant's counters at snapshot time, for a caller (e.g. an OTLP metrics
/// exporter) that wants the raw numbers rather than the Prometheus rendering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantSnapshot {
    /// The partition id these counters belong to.
    pub tenant: String,
    /// Total requests recorded for this tenant.
    pub requests: u64,
    /// Requests that ended in a 4xx/5xx status.
    pub failures: u64,
    /// Cumulative wall-time spent serving this tenant's requests, in nanoseconds.
    pub duration_nanos: u64,
}

/// The bounded per-tenant counter map. Cheap to construct; a deployment that
/// never enables it never allocates one.
#[derive(Debug, Clone)]
pub struct TenantMetrics {
    by_tenant: Cache<String, std::sync::Arc<Counters>>,
}

impl TenantMetrics {
    /// A fresh collector with the default bounds ([`DEFAULT_MAX_TENANTS`],
    /// [`DEFAULT_IDLE_TTL`]).
    #[must_use]
    pub fn new() -> Self {
        Self::with_bounds(DEFAULT_MAX_TENANTS, DEFAULT_IDLE_TTL)
    }

    /// A fresh collector with explicit bounds (for tests, or a deployment that
    /// wants a different cardinality/staleness trade-off).
    #[must_use]
    pub fn with_bounds(max_tenants: u64, idle_ttl: Duration) -> Self {
        Self {
            by_tenant: Cache::builder()
                .max_capacity(max_tenants)
                .time_to_idle(idle_ttl)
                .build(),
        }
    }

    /// Tallies one completed request for `tenant`.
    pub fn record(&self, tenant: &str, ok: bool, duration_nanos: u64) {
        let counters = self.by_tenant.get_with(tenant.to_owned(), || {
            std::sync::Arc::new(Counters::default())
        });
        counters.requests.fetch_add(1, Ordering::Relaxed);
        if !ok {
            counters.failures.fetch_add(1, Ordering::Relaxed);
        }
        counters
            .duration_nanos
            .fetch_add(duration_nanos, Ordering::Relaxed);
    }

    /// Live tenants right now (bounded by `max_tenants`, never all-time count).
    #[must_use]
    pub fn live_tenant_count(&self) -> u64 {
        self.by_tenant.run_pending_tasks();
        self.by_tenant.entry_count()
    }

    /// An immutable snapshot of every currently-live tenant's counters.
    #[must_use]
    pub fn snapshot(&self) -> Vec<TenantSnapshot> {
        self.by_tenant
            .iter()
            .map(|(tenant, counters)| TenantSnapshot {
                tenant: tenant.as_ref().clone(),
                requests: counters.requests.load(Ordering::Relaxed),
                failures: counters.failures.load(Ordering::Relaxed),
                duration_nanos: counters.duration_nanos.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// A Prometheus exposition-format rendering of every currently-live
    /// tenant: one `# HELP`/`# TYPE` pair per metric name (not per series, the
    /// format requires them exactly once), then one sample line per tenant.
    /// Labels carry only tenant (partition) ids.
    #[must_use]
    pub fn to_prometheus_text(&self) -> String {
        let snapshot = self.snapshot();
        let mut out = String::new();
        metric_header(
            &mut out,
            "osproxy_tenant_requests_total",
            "counter",
            "Total requests seen for this tenant.",
        );
        for s in &snapshot {
            sample(
                &mut out,
                "osproxy_tenant_requests_total",
                &s.tenant,
                s.requests,
            );
        }
        metric_header(
            &mut out,
            "osproxy_tenant_failures_total",
            "counter",
            "Requests for this tenant that ended in a 4xx/5xx status.",
        );
        for s in &snapshot {
            sample(
                &mut out,
                "osproxy_tenant_failures_total",
                &s.tenant,
                s.failures,
            );
        }
        metric_header(
            &mut out,
            "osproxy_tenant_latency_nanos_total",
            "counter",
            "Cumulative wall-time spent serving this tenant's requests, in nanoseconds.",
        );
        for s in &snapshot {
            sample(
                &mut out,
                "osproxy_tenant_latency_nanos_total",
                &s.tenant,
                s.duration_nanos,
            );
        }
        out
    }
}

impl Default for TenantMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn metric_header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn sample(out: &mut String, name: &str, tenant: &str, value: u64) {
    let _ = writeln!(out, "{name}{{tenant=\"{}\"}} {value}", escape(tenant));
}

/// Escapes a label value per the Prometheus exposition format (backslash and
/// quote).
fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tallies_requests_failures_and_latency_per_tenant() {
        let metrics = TenantMetrics::new();
        metrics.record("acme", true, 1_000_000);
        metrics.record("acme", true, 2_000_000);
        metrics.record("acme", false, 3_000_000);
        metrics.record("globex", true, 500_000);

        let mut snapshot = metrics.snapshot();
        snapshot.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].tenant, "acme");
        assert_eq!(snapshot[0].requests, 3);
        assert_eq!(snapshot[0].failures, 1);
        assert_eq!(snapshot[0].duration_nanos, 6_000_000);
        assert_eq!(snapshot[1].tenant, "globex");
        assert_eq!(snapshot[1].requests, 1);
        assert_eq!(snapshot[1].failures, 0);
    }

    #[test]
    fn prometheus_text_carries_help_and_type_once_per_metric() {
        let metrics = TenantMetrics::new();
        metrics.record("acme", true, 100);
        let text = metrics.to_prometheus_text();
        assert!(text.contains("# HELP osproxy_tenant_requests_total"));
        assert!(text.contains("# TYPE osproxy_tenant_requests_total counter"));
        assert!(text.contains("osproxy_tenant_requests_total{tenant=\"acme\"} 1"));
        assert_eq!(text.matches("# TYPE").count(), 3);
    }

    #[test]
    fn escapes_quotes_and_backslashes_in_tenant_labels() {
        let metrics = TenantMetrics::new();
        metrics.record("weird\"tenant\\", true, 100);
        assert!(metrics
            .to_prometheus_text()
            .contains("tenant=\"weird\\\"tenant\\\\\""));
    }

    #[test]
    fn live_tenant_count_reflects_currently_tracked_tenants() {
        let metrics = TenantMetrics::new();
        assert_eq!(metrics.live_tenant_count(), 0);
        metrics.record("acme", true, 1);
        metrics.record("globex", true, 1);
        assert_eq!(metrics.live_tenant_count(), 2);
    }

    #[test]
    fn a_hard_cap_bounds_live_entries_even_before_idle_eviction() {
        let metrics = TenantMetrics::with_bounds(2, DEFAULT_IDLE_TTL);
        metrics.record("a", true, 1);
        metrics.record("b", true, 1);
        metrics.record("c", true, 1);
        assert!(metrics.live_tenant_count() <= 2);
    }
}
