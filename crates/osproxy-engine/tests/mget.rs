//! The multi-get (`_mget`) read path through the pipeline (`docs/04` §5): the
//! read counterpart of the `_bulk` demux. Asserts the per-document results are
//! re-interleaved in input order, each shaped into the client's logical view
//! (injected tenancy fields stripped, physical id mapped back), a miss is a
//! logical `found: false`, and no tenant value leaks.

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

async fn mget(p: &Pipeline<SharedTenancy, MemorySink>, body: &[u8]) -> PipelineResponse {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("mg");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::MultiGet,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        body,
    );
    p.handle(&ctx).await.unwrap()
}

#[tokio::test]
async fn mget_reinterleaves_in_input_order_and_shapes_each_doc() {
    let p = pipeline();
    write(&p, br#"{"tenant_id":"acme","id":7,"msg":"seven"}"#).await;
    write(&p, br#"{"tenant_id":"acme","id":9,"msg":"nine"}"#).await;

    // Request 9, then a miss, then 7 — the response must echo that order.
    let resp = mget(&p, br#"{"ids":["9","404","7"]}"#).await;
    assert_eq!(resp.status, 200);
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    let docs = doc["docs"].as_array().unwrap();
    assert_eq!(docs.len(), 3);

    // [0] = logical doc 9, tenancy machinery stripped.
    assert_eq!(docs[0]["_index"], "orders");
    assert_eq!(docs[0]["_id"], "9");
    assert_eq!(docs[0]["found"], true);
    assert!(docs[0]["_source"].get("_tenant").is_none());
    assert!(docs[0].get("_routing").is_none());
    assert_eq!(docs[0]["_source"]["msg"], "nine");

    // [1] = a logical not-found, positioned in place.
    assert_eq!(docs[1]["_id"], "404");
    assert_eq!(docs[1]["found"], false);

    // [2] = logical doc 7.
    assert_eq!(docs[2]["_id"], "7");
    assert_eq!(docs[2]["found"], true);
    assert_eq!(docs[2]["_source"]["msg"], "seven");

    // No tenant value or physical id leaked anywhere in the response.
    let text = doc.to_string();
    assert!(!text.contains("acme:"), "physical id leaked: {text}");
    assert!(!text.contains("_tenant"), "injected field leaked: {text}");
}

#[tokio::test]
async fn mget_docs_form_maps_logical_ids_to_physical_reads() {
    let p = pipeline();
    write(&p, br#"{"tenant_id":"acme","id":7,"msg":"seven"}"#).await;

    let resp = mget(&p, br#"{"docs":[{"_index":"orders","_id":"7"}]}"#).await;
    assert_eq!(resp.status, 200);
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["docs"][0]["_id"], "7");
    assert_eq!(doc["docs"][0]["found"], true);
    assert_eq!(doc["docs"][0]["_source"]["msg"], "seven");
}
