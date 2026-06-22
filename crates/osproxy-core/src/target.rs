//! Where a routed request is sent.
//!
//! A [`Target`] is the physical destination a routing decision resolves to: a
//! concrete cluster and a concrete index. In v1 every request resolves to
//! exactly one target, there is no synchronous fan-out (`docs/00` non-goals,
//! ADR-002). The tenancy layer turns a partition's placement into a `Target`;
//! the sink and upstream pool consume it.

use std::fmt;

use crate::ids::{ClusterId, IndexName};

/// The physical destination of a single routed request.
///
/// Both fields are ids/names (never tenant values), so a `Target` is safe to
/// render in telemetry and `/debug/explain` (`docs/05` §7).
///
/// # Examples
///
/// ```
/// use osproxy_core::{ClusterId, IndexName, Target};
///
/// let target = Target::new(ClusterId::from("eu-1"), IndexName::from("logs-shared"));
/// assert_eq!(target.cluster.as_str(), "eu-1");
/// assert_eq!(target.to_string(), "eu-1/logs-shared");
/// ```
#[derive(Clone, Debug)]
pub struct Target {
    /// The physical OpenSearch cluster the request is sent to.
    pub cluster: ClusterId,
    /// The concrete (physical) index the request operates on.
    pub index: IndexName,
    /// The cluster's base URL, supplied by the tenancy as part of the placement
    /// result (the sink builds a pool for it on first use). `None` only in unit
    /// tests that dispatch to an in-memory sink, which ignores it.
    ///
    /// Excluded from identity (equality/hashing/`Display`): the endpoint is a
    /// function of the cluster, not part of *which* target this is, so two ops
    /// for the same `cluster`+`index` stay one demux key regardless of it.
    pub endpoint: Option<String>,
}

impl Target {
    /// Constructs a target from a cluster and an index (no endpoint).
    #[must_use]
    pub fn new(cluster: ClusterId, index: IndexName) -> Self {
        Self {
            cluster,
            index,
            endpoint: None,
        }
    }

    /// Sets the cluster's base URL (builder style), as resolved from the
    /// placement result.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.endpoint = endpoint;
        self
    }
}

// Identity is (cluster, index) only; the endpoint is dispatch metadata derived
// from the cluster, so it is deliberately excluded.
impl PartialEq for Target {
    fn eq(&self, other: &Self) -> bool {
        self.cluster == other.cluster && self.index == other.index
    }
}
impl Eq for Target {}
impl std::hash::Hash for Target {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.cluster.hash(state);
        self.index.hash(state);
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.cluster, self.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_exposes_cluster_and_index_and_displays_path_like() {
        let target = Target::new(ClusterId::from("us-2"), IndexName::from("orders-7"));
        assert_eq!(target.cluster.as_str(), "us-2");
        assert_eq!(target.index.as_str(), "orders-7");
        assert_eq!(target.to_string(), "us-2/orders-7");
    }

    #[test]
    fn targets_compare_by_both_fields() {
        let a = Target::new(ClusterId::from("c"), IndexName::from("i"));
        let b = Target::new(ClusterId::from("c"), IndexName::from("j"));
        assert_ne!(a, b);
        assert_eq!(a, a.clone());
    }
}
