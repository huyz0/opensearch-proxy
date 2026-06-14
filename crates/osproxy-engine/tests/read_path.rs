//! The get-by-id read path through the pipeline (`docs/04` §5): example-based
//! companions to the property test in `round_trip.rs`, covering a hit, a miss,
//! and the shape-only read spans recorded for blind diagnosis (`docs/05`).

// Test scaffolding (helpers + a tenancy impl, not `#[test]` fns).
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use osproxy_core::{
    ClusterId, EndpointKind, FieldName, IndexName, PartitionId, PrincipalId, RequestId,
};
use osproxy_engine::{Pipeline, PipelineResponse};
use osproxy_sink::MemorySink;
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter};
use serde_json::Value;

struct SharedTenancy {
    table: Arc<PlacementTable>,
}

impl TenancySpi for SharedTenancy {
    fn partition_key(&self) -> PartitionKeySpec {
        PartitionKeySpec::AnyOf(vec![
            PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            PartitionKeySpec::Header("x-tenant".to_owned()),
        ])
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
}

fn pipeline() -> Pipeline<SharedTenancy, MemorySink> {
    let table = Arc::new(PlacementTable::new());
    table.set(
        PartitionId::from("acme"),
        Placement::SharedIndex {
            cluster: ClusterId::from("eu-1"),
            index: IndexName::from("shared"),
            inject: vec![InjectedField::new(
                FieldName::from("_tenant"),
                InjectedValue::PartitionId,
            )],
        },
    );
    Pipeline::new(
        TenancyRouter::new(SharedTenancy { table }),
        MemorySink::new(),
    )
}

async fn write(p: &Pipeline<SharedTenancy, MemorySink>, body: &[u8]) {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("w");
    let headers = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        body,
    );
    assert_eq!(p.handle(&ctx).await.unwrap().status, 201);
}

async fn read(
    p: &Pipeline<SharedTenancy, MemorySink>,
    rid: &str,
    logical_id: &str,
) -> PipelineResponse {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from(rid);
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Get,
        EndpointKind::GetById,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        b"",
    )
    .with_doc_id(Some(logical_id));
    p.handle(&ctx).await.unwrap()
}

#[tokio::test]
async fn get_by_id_returns_the_logical_document() {
    let p = pipeline();
    write(&p, br#"{"tenant_id":"acme","id":7,"msg":"hello"}"#).await;

    let resp = read(&p, "r", "7").await;
    assert_eq!(resp.status, 200);
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    // The client sees its logical id and document, with no tenancy leakage.
    assert_eq!(doc["_id"], "7");
    assert_eq!(doc["_index"], "orders");
    assert!(doc.get("_routing").is_none());
    assert!(doc["_source"].get("_tenant").is_none());
    assert_eq!(doc["_source"]["msg"], "hello");
    assert_eq!(doc["_source"]["id"], 7);
}

#[tokio::test]
async fn get_by_id_miss_is_logical_not_found() {
    let p = pipeline();
    let resp = read(&p, "r", "404").await;
    assert_eq!(resp.status, 404);
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["_id"], "404");
    assert_eq!(doc["found"], false);
}

#[tokio::test]
async fn get_by_id_records_shape_only_read_spans() {
    let p = pipeline();
    write(&p, br#"{"tenant_id":"acme","id":7,"msg":"hello"}"#).await;
    read(&p, "r", "7").await;

    let doc = p.explain(&RequestId::from("r")).expect("trace recorded");
    assert_eq!(doc["outcome"], "ok");
    assert_eq!(doc["spans"]["classify"]["endpoint_kind"], "GetById");
    assert_eq!(doc["spans"]["spi.resolve"]["partition_id"], "acme");
    assert_eq!(doc["spans"]["dispatch"]["upstream_status"], 200);
    assert_eq!(doc["spans"]["egress"]["status"], 200);
    // No tenant value leaked into the trace.
    let text = doc.to_string();
    assert!(!text.contains("hello"), "value leaked: {text}");
}
