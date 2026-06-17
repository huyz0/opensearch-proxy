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
    DocIdRule, IdTemplate, InjectedField, InjectedValue, JsonPath, PartitionKeySpec, Placement,
    PlacementAt, SensitivitySpec, SpiError, TenancySpi,
};

/// The injected tenancy field name.
const TENANT_FIELD: &str = "_tenant";

/// The header carrying the partition on by-id reads (which have no body).
const TENANT_HEADER: &str = "x-tenant";

/// A single-shared-index tenancy: all partitions share one physical index,
/// isolated by an injected `_tenant` field.
#[derive(Debug)]
pub struct ReferenceTenancy {
    cluster: ClusterId,
    index: IndexName,
    endpoint: String,
}

impl ReferenceTenancy {
    /// Builds the reference tenancy over one cluster and shared index, served at
    /// `endpoint` (the cluster's base URL, reported as part of the placement
    /// result so the sink can pool it).
    #[must_use]
    pub fn new(cluster: ClusterId, index: IndexName, endpoint: impl Into<String>) -> Self {
        Self {
            cluster,
            index,
            endpoint: endpoint.into(),
        }
    }
}

impl TenancySpi for ReferenceTenancy {
    fn partition_key(&self) -> PartitionKeySpec {
        // Ingest carries the partition in the body; by-id reads have no body, so
        // they carry it in a header set by the caller (or an auth gateway).
        PartitionKeySpec::AnyOf(vec![
            PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            PartitionKeySpec::Header(TENANT_HEADER.to_owned()),
        ])
    }

    fn doc_id_rule(&self) -> Option<DocIdRule> {
        Some(DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true))
    }

    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![InjectedField::new(
            FieldName::from(TENANT_FIELD),
            InjectedValue::PartitionId,
        )]
    }

    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::none()
    }

    fn cluster_endpoint(&self, cluster: &ClusterId) -> Option<String> {
        // The cursor-affinity path routes by cluster id with no placement; resolve
        // its endpoint here (this reference tenancy has exactly one cluster).
        (cluster == &self.cluster).then(|| self.endpoint.clone())
    }

    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        // Every partition resolves to the same shared index. A constant epoch:
        // this reference tenancy has no migration (the epoch story is exercised
        // by the PlacementTable-backed implementations).
        Ok(PlacementAt::new(
            Placement::SharedIndex {
                cluster: self.cluster.clone(),
                index: self.index.clone(),
                inject: self.injected_fields(),
            },
            Epoch::new(1),
        )
        .with_endpoint(self.endpoint.clone()))
    }
}
