//! The migration write gate on the `_bulk` path (`docs/06` §2, INV-M1): in a
//! mixed-partition bulk, items for a partition in cutover are held with a
//! positioned, retryable `409` while items for a settled partition commit — the
//! gate is per item, not per request, so one migrating partition does not stall
//! the rest of the batch.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use osproxy_core::{
    ClusterId, EndpointKind, Epoch, FieldName, IndexName, PartitionId, PrincipalId, RequestId,
};
use osproxy_engine::{Pipeline, PipelineResponse};
use osproxy_sink::MemorySink;
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter, WriteAdmission};
use serde_json::Value;

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
    async fn placement_for(&self, p: &PartitionId) -> Result<PlacementAt, SpiError> {
        self.table.get(p).ok_or_else(|| SpiError::PlacementMissing {
            partition: p.clone(),
        })
    }
    async fn admit_write(&self, partition: &PartitionId, epoch: Epoch) -> bool {
        self.table.admit_write(partition, epoch) == WriteAdmission::Admit
    }
}

fn shared_on(cluster: &str, index: &str) -> Placement {
    Placement::SharedIndex {
        cluster: ClusterId::from(cluster),
        index: IndexName::from(index),
        inject: vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )],
    }
}

async fn bulk(p: &Pipeline<MigratingTenancy, MemorySink>, body: &[u8]) -> PipelineResponse {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("b");
    let headers = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestBulk,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        body,
    );
    p.handle(&ctx).await.unwrap()
}

#[tokio::test]
async fn bulk_gates_per_item_holding_only_the_migrating_partition() {
    let table = Arc::new(PlacementTable::new());
    table.set(PartitionId::from("acme"), shared_on("eu-1", "acme-idx"));
    table.set(PartitionId::from("globex"), shared_on("eu-1", "globex-idx"));
    // globex is mid-cutover; acme is settled.
    table
        .begin_migration(
            &PartitionId::from("globex"),
            shared_on("us-1", "globex-idx"),
        )
        .unwrap();
    table.enter_cutover(&PartitionId::from("globex")).unwrap();

    let pipeline = Pipeline::new(
        TenancyRouter::new(MigratingTenancy {
            table: Arc::clone(&table),
        }),
        MemorySink::new(),
    );

    // Interleaved: acme, globex, globex, acme.
    let body = concat!(
        "{\"index\":{}}\n{\"tenant_id\":\"acme\",\"id\":1,\"msg\":\"a1\"}\n",
        "{\"index\":{}}\n{\"tenant_id\":\"globex\",\"id\":2,\"msg\":\"g2\"}\n",
        "{\"index\":{}}\n{\"tenant_id\":\"globex\",\"id\":3,\"msg\":\"g3\"}\n",
        "{\"index\":{}}\n{\"tenant_id\":\"acme\",\"id\":4,\"msg\":\"a4\"}\n",
    );
    let resp = bulk(&pipeline, body.as_bytes()).await;
    assert_eq!(resp.status, 200);
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["errors"], true);
    let items = doc["items"].as_array().unwrap();

    // Order preserved. acme items committed; globex items held with 409 stale.
    assert_eq!(items[0]["index"]["status"], 201);
    assert_eq!(items[0]["index"]["_id"], "1");
    assert_eq!(items[1]["index"]["status"], 409);
    assert_eq!(items[1]["index"]["error"]["type"], "stale_epoch");
    assert_eq!(items[2]["index"]["status"], 409);
    assert_eq!(items[2]["index"]["error"]["type"], "stale_epoch");
    assert_eq!(items[3]["index"]["status"], 201);
    assert_eq!(items[3]["index"]["_id"], "4");

    // Only the admitted (acme) writes reached the sink — nothing for globex
    // during its cutover (INV-M1).
    let recorded: usize = pipeline
        .sink()
        .recorded()
        .iter()
        .map(|b| b.ops().len())
        .sum();
    assert_eq!(recorded, 2, "only the two acme writes committed");
}
