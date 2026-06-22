//! The M1 write-path spine, short of the wire: a request flows through the
//! tenancy router, the engine's plan builder, and into a [`MemorySink`], and we
//! assert the recorded write is correctly tenanted (target, injected field,
//! constructed id, routing) and that stripping the injected field recovers the
//! client's original document, the round-trip symmetry the read path will rely
//! on in M2.

use std::sync::Arc;

use osproxy_core::{
    ClusterId, EndpointKind, FieldName, IndexName, PartitionId, PrincipalId, RequestId,
};
use osproxy_engine::build_write_batch;
use osproxy_rewrite::strip_fields;
use osproxy_sink::{DocOp, MemorySink, Sink, WriteBatch};
use osproxy_spi::{
    BodyDoc, DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter};
use serde_json::json;

struct SharedTenancy {
    table: Arc<PlacementTable>,
}

impl TenancySpi for SharedTenancy {
    fn resolve_partition(
        &self,
        ctx: &osproxy_spi::RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<osproxy_core::PartitionId, osproxy_spi::SpiError> {
        osproxy_tenancy::resolve_partition_spec(
            &PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            ctx,
            body,
        )
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
}

#[tokio::test]
async fn single_doc_ingest_routes_transforms_and_writes() {
    // Place partition "acme" on a shared index.
    let table = Arc::new(PlacementTable::new());
    let inject = vec![InjectedField::new(
        FieldName::from("_tenant"),
        InjectedValue::PartitionId,
    )];
    let epoch = table.set(
        PartitionId::from("acme"),
        Placement::SharedIndex {
            cluster: ClusterId::from("eu-1"),
            index: IndexName::from("orders-shared"),
            inject,
        },
    );

    let router = TenancyRouter::new(SharedTenancy {
        table: Arc::clone(&table),
    });
    let sink = MemorySink::new();

    // A client ingest request.
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("req-1");
    let headers: Vec<(String, String)> = vec![];
    let client_doc = json!({ "tenant_id": "acme", "id": 1001, "msg": "hello" });
    let body = serde_json::to_vec(&client_doc).unwrap();
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

    // Resolve → plan → dispatch.
    let resolved = router.resolve(&ctx).await.unwrap();
    let batch: WriteBatch = build_write_batch(&resolved, &body).unwrap();
    let ack = sink.write(batch).await.unwrap();

    // The write was acknowledged with the constructed id.
    assert!(ack.all_succeeded());
    assert_eq!(ack.results()[0].id, "acme:1001");

    // Inspect what actually hit the sink.
    let recorded = sink.recorded();
    assert_eq!(recorded.len(), 1);
    let op = &recorded[0].ops()[0];
    assert_eq!(
        op.target,
        osproxy_core::Target::new(ClusterId::from("eu-1"), IndexName::from("orders-shared"))
    );
    assert_eq!(op.epoch, epoch);

    let DocOp::Index { id, routing, body } = &op.doc else {
        unreachable!("ingest produces an Index op")
    };
    assert_eq!(id.as_deref(), Some("acme:1001"));
    assert_eq!(routing.as_deref(), Some("acme"));

    // The stored doc carries the injected tenancy field...
    let mut stored: serde_json::Value = serde_json::from_slice(body).unwrap();
    assert_eq!(stored["_tenant"], json!("acme"));

    // ...and stripping it recovers the client's original document (symmetry).
    let removed = strip_fields(&mut stored, &[FieldName::from("_tenant")]);
    assert_eq!(removed, 1);
    assert_eq!(stored, client_doc);
}
