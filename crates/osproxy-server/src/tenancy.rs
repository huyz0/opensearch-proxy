//! The reference tenancy implementation the binary serves.
//!
//! A minimal but complete [`TenancySpi`]: the partition is the `tenant_id`
//! body field on ingest (or the `x-tenant` header on by-id reads, which carry
//! no body), every document gets a `_tenant` field and a `{partition}:{body.id}`
//! id with routing, and every partition lives on one shared index. It exists to
//! make the binary runnable and to demonstrate the SPI; real consumers provide
//! their own.

use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, IdTemplate, InjectedField, InjectedValue, JsonPath, PartitionKeySpec,
    Placement, PlacementAt, SpiError, TenancySpi,
};

/// The injected tenancy field name.
const TENANT_FIELD: &str = "_tenant";

/// The header carrying the partition on by-id reads (which have no body).
const TENANT_HEADER: &str = "x-tenant";

/// Which placement kind the reference tenancy resolves every partition to. The
/// binary defaults to [`PlacementMode::SharedIndex`] (the body-rewrite mode); the
/// other two let one reference impl demonstrate the **no-body-rewrite** routing
/// modes, where isolation is by cluster or by index and the document is forwarded
/// unchanged (`docs/guide/10-choosing-a-mode`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlacementMode {
    /// Many partitions share one index; isolation by an injected field and a
    /// partition-scoped `_id` — the document body is rewritten on ingest.
    #[default]
    SharedIndex,
    /// Each partition owns a physical index on the shared cluster; isolation is by
    /// index name, so the body is forwarded unchanged (no rewrite).
    DedicatedIndex,
    /// Each partition owns a whole cluster; isolation is by cluster, so the body is
    /// forwarded unchanged (no rewrite).
    DedicatedCluster,
}

/// A reference tenancy: every partition resolves to a placement of the configured
/// [`PlacementMode`]. The default `SharedIndex` mode isolates by an injected
/// `_tenant` field; the dedicated modes isolate by index/cluster with no body
/// rewrite.
#[derive(Debug)]
pub struct ReferenceTenancy {
    cluster: ClusterId,
    index: IndexName,
    endpoint: String,
    mode: PlacementMode,
}

impl ReferenceTenancy {
    /// Builds the reference tenancy over one cluster and shared index, served at
    /// `endpoint` (the cluster's base URL, reported as part of the placement
    /// result so the sink can pool it). Defaults to [`PlacementMode::SharedIndex`].
    #[must_use]
    pub fn new(cluster: ClusterId, index: IndexName, endpoint: impl Into<String>) -> Self {
        Self {
            cluster,
            index,
            endpoint: endpoint.into(),
            mode: PlacementMode::SharedIndex,
        }
    }

    /// Sets the placement mode (builder). `SharedIndex` rewrites the body;
    /// `DedicatedIndex`/`DedicatedCluster` route without touching it.
    #[must_use]
    pub fn with_placement_mode(mut self, mode: PlacementMode) -> Self {
        self.mode = mode;
        self
    }
}

impl TenancySpi for ReferenceTenancy {
    fn resolve_partition(
        &self,
        ctx: &osproxy_spi::RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<osproxy_core::PartitionId, osproxy_spi::SpiError> {
        // Ingest carries the partition in the body; by-id reads have no body, so
        // they carry it in a header set by the caller (or an auth gateway).
        let spec = PartitionKeySpec::AnyOf(vec![
            PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            PartitionKeySpec::Header(TENANT_HEADER.to_owned()),
        ]);
        osproxy_tenancy::resolve_partition_spec(&spec, ctx, body)
    }

    fn doc_id_rule(&self) -> Option<DocIdRule> {
        // Only the shared index needs a partition-scoped id; the dedicated modes
        // isolate by cluster/index and leave the id (and the body) untouched.
        match self.mode {
            PlacementMode::SharedIndex => {
                Some(DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true))
            }
            PlacementMode::DedicatedIndex | PlacementMode::DedicatedCluster => None,
        }
    }

    fn injected_fields(&self) -> Vec<InjectedField> {
        // The injected isolation field exists only in the shared index; the
        // dedicated modes inject nothing (no body rewrite).
        match self.mode {
            PlacementMode::SharedIndex => vec![InjectedField::new(
                FieldName::from(TENANT_FIELD),
                InjectedValue::PartitionId,
            )],
            PlacementMode::DedicatedIndex | PlacementMode::DedicatedCluster => Vec::new(),
        }
    }

    // `sensitive_fields` is left at the deny-by-default `all_sensitive`: this
    // tenancy carries real tenant payloads, so every value is redacted unless a
    // future revision allow-lists a known-safe field.

    fn cluster_endpoint(&self, cluster: &ClusterId) -> Option<String> {
        // The cursor-affinity path routes by cluster id with no placement; resolve
        // its endpoint here (this reference tenancy has exactly one cluster).
        (cluster == &self.cluster).then(|| self.endpoint.clone())
    }

    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        // Every partition resolves to a placement of the configured mode. A
        // constant epoch: this reference tenancy has no migration (the epoch story
        // is exercised by the PlacementTable-backed implementations).
        let placement = match self.mode {
            // Shared: isolation is by the injected field + scoped id, so every
            // partition can share the one physical index.
            PlacementMode::SharedIndex => Placement::SharedIndex {
                cluster: self.cluster.clone(),
                index: self.index.clone(),
                inject: self.injected_fields(),
            },
            // Dedicated index: isolation IS the index, so each partition must get a
            // distinct physical index (`{index}-{partition}`) — a shared index here
            // would put two tenants in one index with no isolation field.
            PlacementMode::DedicatedIndex => Placement::DedicatedIndex {
                cluster: self.cluster.clone(),
                index: IndexName::from(format!("{}-{}", self.index.as_str(), partition.as_str())),
            },
            // Dedicated cluster: isolation is the cluster. This reference has a
            // single cluster/endpoint, so every partition maps to it; a real
            // multi-cluster tenancy would resolve `partition` to its own cluster.
            PlacementMode::DedicatedCluster => Placement::DedicatedCluster {
                cluster: self.cluster.clone(),
            },
        };
        Ok(PlacementAt::new(placement, Epoch::new(1)).with_endpoint(self.endpoint.clone()))
    }
}
