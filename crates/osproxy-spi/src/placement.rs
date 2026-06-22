//! Where a partition currently lives.
//!
//! A [`Placement`] is the resolved home of a partition; [`PlacementAt`] pairs it
//! with the [`Epoch`] it was read at, so a write can be epoch-stamped and the
//! sink can reject a stale-epoch write during a migration (`docs/03`, `docs/06`).

use osproxy_core::{ClusterId, Epoch, IndexName};

use crate::rules::InjectedField;

/// The resolved home of a partition.
///
/// The three modes trade isolation against density (`docs/03` §3):
/// - `DedicatedCluster`: the partition owns a whole cluster (its index name is
///   carried unchanged from the request's logical index).
/// - `DedicatedIndex`: the partition owns a physical index on a shared cluster.
/// - `SharedIndex`: many partitions share one physical index; isolation is
///   enforced by injected partition fields (whose names the SPI chose) plus a
///   partition filter on read.
///
/// Deliberately *not* `#[non_exhaustive]`: the proxy core must interpret every
/// placement mode to route correctly, so adding a mode should force every match
/// in the workspace to be updated rather than silently fall through (`docs/03`).
///
/// # Examples
///
/// ```
/// use osproxy_spi::Placement;
/// use osproxy_spi::core::{ClusterId, IndexName};
///
/// let p = Placement::SharedIndex {
///     cluster: ClusterId::from("eu-1"),
///     index: IndexName::from("shared"),
///     inject: vec![],
/// };
/// assert_eq!(p.cluster().as_str(), "eu-1");
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Placement {
    /// The partition has a dedicated cluster.
    DedicatedCluster {
        /// The cluster that exclusively serves this partition.
        cluster: ClusterId,
    },
    /// The partition has a dedicated index on a shared cluster.
    DedicatedIndex {
        /// The hosting cluster.
        cluster: ClusterId,
        /// The physical index for this partition.
        index: IndexName,
    },
    /// The partition shares a physical index with others, isolated by the
    /// injected fields named here.
    SharedIndex {
        /// The hosting cluster.
        cluster: ClusterId,
        /// The shared physical index.
        index: IndexName,
        /// Fields injected on ingest and stripped on read to isolate tenants.
        inject: Vec<InjectedField>,
    },
}

impl Placement {
    /// The cluster this placement resolves to, regardless of mode.
    #[must_use]
    pub fn cluster(&self) -> &ClusterId {
        match self {
            Self::DedicatedCluster { cluster }
            | Self::DedicatedIndex { cluster, .. }
            | Self::SharedIndex { cluster, .. } => cluster,
        }
    }
}

/// The partition's migration phase at read time, a shape-only label (never
/// tenant data) so observability can show where a migration is (`docs/06` §5).
///
/// # Examples
///
/// ```
/// use osproxy_spi::MigrationPhase;
/// assert_eq!(MigrationPhase::default(), MigrationPhase::Settled);
/// assert_eq!(MigrationPhase::Cutover.as_str(), "cutover");
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum MigrationPhase {
    /// Not migrating; the placement is settled.
    #[default]
    Settled,
    /// Migrating, copy phase, writes still go to the origin.
    Draining,
    /// Migrating, cutover window, writes are held (stale-epoch retry).
    Cutover,
}

impl MigrationPhase {
    /// A stable lowercase label for telemetry.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Settled => "settled",
            Self::Draining => "draining",
            Self::Cutover => "cutover",
        }
    }
}

/// A [`Placement`] together with the placement-table epoch it was read at and the
/// partition's migration phase.
///
/// The epoch flows into the routing decision and onto the write so migration
/// cutover can detect a write resolved against a superseded placement
/// (`docs/06` §2); the phase is shape-only context for observability.
///
/// # Examples
///
/// ```
/// use osproxy_spi::{Placement, PlacementAt, MigrationPhase};
/// use osproxy_spi::core::{ClusterId, Epoch};
///
/// let at = PlacementAt::new(
///     Placement::DedicatedCluster { cluster: ClusterId::from("eu-1") },
///     Epoch::new(7),
/// )
/// .with_phase(MigrationPhase::Draining);
/// assert_eq!(at.epoch, Epoch::new(7));
/// assert_eq!(at.phase, MigrationPhase::Draining);
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PlacementAt {
    /// The resolved placement.
    pub placement: Placement,
    /// The epoch the placement table was at when this was read.
    pub epoch: Epoch,
    /// The partition's migration phase at read time.
    pub phase: MigrationPhase,
    /// The base URL of the placement's cluster. The tenancy is the source of
    /// truth for where each cluster lives; the sink builds a pool for this URL
    /// the first time it routes to the cluster. Required to reach a live cluster
    /// (an in-memory sink ignores it).
    pub endpoint: Option<String>,
}

impl PlacementAt {
    /// Pairs a placement with the epoch it was read at (settled, not migrating,
    /// no endpoint).
    #[must_use]
    pub fn new(placement: Placement, epoch: Epoch) -> Self {
        Self {
            placement,
            epoch,
            phase: MigrationPhase::Settled,
            endpoint: None,
        }
    }

    /// Sets the migration phase (builder style).
    #[must_use]
    pub fn with_phase(mut self, phase: MigrationPhase) -> Self {
        self.phase = phase;
        self
    }

    /// Sets the cluster's base URL (builder style). This is how the tenancy tells
    /// the proxy where the placement's cluster lives, e.g.
    /// `.with_endpoint("https://eu-1.internal:9200")`.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_is_extracted_for_every_mode() {
        let dc = Placement::DedicatedCluster {
            cluster: ClusterId::from("c1"),
        };
        let di = Placement::DedicatedIndex {
            cluster: ClusterId::from("c2"),
            index: IndexName::from("i"),
        };
        let si = Placement::SharedIndex {
            cluster: ClusterId::from("c3"),
            index: IndexName::from("shared"),
            inject: Vec::new(),
        };
        assert_eq!(dc.cluster().as_str(), "c1");
        assert_eq!(di.cluster().as_str(), "c2");
        assert_eq!(si.cluster().as_str(), "c3");
    }

    #[test]
    fn placement_at_pairs_epoch() {
        let at = PlacementAt::new(
            Placement::DedicatedCluster {
                cluster: ClusterId::from("c"),
            },
            Epoch::new(5),
        );
        assert_eq!(at.epoch, Epoch::new(5));
    }
}
