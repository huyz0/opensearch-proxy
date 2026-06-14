//! The migration write gate, end-to-end through the engine pipeline (`docs/06`
//! §2, INV-M1/M2). A `PlacementTable`-backed tenancy drives a partition through
//! its migration phases while the same ingest request is replayed; the pipeline
//! must reject the write with a retryable stale-epoch error during cutover and
//! after the pointer flips, and admit it otherwise — never committing to the
//! wrong cluster.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use osproxy_core::{
    ClusterId, EndpointKind, Epoch, ErrorCode, FieldName, IndexName, PartitionId, PrincipalId,
    RequestId,
};
use osproxy_engine::{Pipeline, PipelineResponse, RequestError};
use osproxy_sink::MemorySink;
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter, WriteAdmission};
use serde_json::json;

/// A tenancy whose placement *and* migration gate are backed by a live table.
struct MigratingTenancy {
    table: Arc<PlacementTable>,
}

impl TenancySpi for MigratingTenancy {
    fn partition_key(&self) -> PartitionKeySpec {
        PartitionKeySpec::BodyField(JsonPath::new("tenant_id"))
    }
    fn doc_id_rule(&self) -> Option<DocIdRule> {
        Some(DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true))
    }
    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )]
    }
    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::none()
    }
    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        self.table
            .get(partition)
            .ok_or_else(|| SpiError::PlacementMissing {
                partition: partition.clone(),
            })
    }
    async fn admit_write(&self, partition: &PartitionId, epoch: Epoch) -> bool {
        self.table.admit_write(partition, epoch) == WriteAdmission::Admit
    }
}

fn shared_on(cluster: &str) -> Placement {
    Placement::SharedIndex {
        cluster: ClusterId::from(cluster),
        index: IndexName::from("orders-shared"),
        inject: vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )],
    }
}

async fn ingest(
    pipeline: &Pipeline<MigratingTenancy, MemorySink>,
) -> Result<PipelineResponse, RequestError> {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("req-1");
    let headers: Vec<(String, String)> = vec![];
    let body = serde_json::to_vec(&json!({ "tenant_id": "acme", "id": 1, "msg": "hi" })).unwrap();
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Put,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "orders-logical",
        HeaderView::new(&headers),
        &body,
    );
    pipeline.handle(&ctx).await
}

#[tokio::test]
async fn ingest_is_gated_through_the_migration_lifecycle() {
    let table = Arc::new(PlacementTable::new());
    let p = PartitionId::from("acme");
    table.set(p.clone(), shared_on("eu-1"));
    let pipeline = Pipeline::new(
        TenancyRouter::new(MigratingTenancy {
            table: Arc::clone(&table),
        }),
        MemorySink::new(),
    );

    // Active: the write commits to the origin cluster.
    assert!(ingest(&pipeline).await.unwrap().status >= 200);
    assert_eq!(pipeline.sink().recorded().len(), 1);

    // Draining: writes still flow to the origin (only cutover rejects).
    table.begin_migration(&p, shared_on("us-1")).unwrap();
    assert!(ingest(&pipeline).await.is_ok());
    assert_eq!(pipeline.sink().recorded().len(), 2);

    // Cutover: the write is held with a retryable stale-epoch error and never
    // reaches the sink (INV-M1).
    table.enter_cutover(&p).unwrap();
    let err = ingest(&pipeline).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::StaleEpoch);
    assert!(err.retryable(), "stale-epoch must be retryable");
    assert_eq!(
        pipeline.sink().recorded().len(),
        2,
        "no write during cutover"
    );

    // After the flip the write commits again — now to the new cluster (us-1),
    // proving the gate re-resolved rather than landing on the old placement
    // (INV-M2).
    table.complete_migration(&p).unwrap();
    assert!(ingest(&pipeline).await.is_ok());
    let recorded = pipeline.sink().recorded();
    assert_eq!(recorded.len(), 3);
    assert_eq!(
        recorded[2].ops()[0].target.cluster,
        ClusterId::from("us-1"),
        "post-migration write lands on the new cluster"
    );
}
