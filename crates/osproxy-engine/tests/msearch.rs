//! The multi-search (`_msearch`) read path through the pipeline (`docs/04` §4):
//! the search counterpart of the `_bulk` demux. Asserts the per-search responses
//! are re-interleaved in input order, each stripped to the client's logical view,
//! every dispatched query carries the mandatory partition filter, and no tenant
//! value leaks.

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

fn pipeline() -> Pipeline<TenancyRouter<SharedTenancy>, MemorySink> {
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

async fn write(p: &Pipeline<TenancyRouter<SharedTenancy>, MemorySink>, body: &[u8]) {
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

async fn msearch(
    p: &Pipeline<TenancyRouter<SharedTenancy>, MemorySink>,
    body: &[u8],
) -> PipelineResponse {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("ms");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::MultiSearch,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        body,
    );
    p.handle(&ctx).await.unwrap()
}

#[tokio::test]
async fn msearch_reinterleaves_responses_and_strips_each_to_logical() {
    let p = pipeline();
    write(&p, br#"{"tenant_id":"acme","id":7,"msg":"hello"}"#).await;

    // Two searches; both run against the caller's partition.
    let body = concat!(
        "{}\n",
        "{\"query\":{\"match\":{\"msg\":\"hello\"}}}\n",
        "{\"index\":\"orders\"}\n",
        "{\"query\":{\"match_all\":{}}}\n",
    );
    let resp = msearch(&p, body.as_bytes()).await;
    assert_eq!(resp.status, 200);
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    let responses = doc["responses"].as_array().unwrap();
    assert_eq!(responses.len(), 2);

    for r in responses {
        assert_eq!(r["status"], 200);
        let hit = &r["hits"]["hits"][0];
        assert_eq!(hit["_index"], "orders");
        assert_eq!(hit["_id"], "7");
        assert!(hit["_source"].get("_tenant").is_none());
        assert_eq!(hit["_source"]["msg"], "hello");
    }

    // Every dispatched query nests the client query under the mandatory filter.
    let dispatched = p.sink().recorded_searches();
    assert_eq!(dispatched.len(), 2);
    for s in dispatched {
        let q: Value = serde_json::from_slice(&s.body).unwrap();
        assert_eq!(q["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
    }

    // No tenant value or physical id leaked into the response.
    let text = doc.to_string();
    assert!(!text.contains("acme:"), "physical id leaked: {text}");
    assert!(!text.contains("_tenant"), "injected field leaked: {text}");
}

#[tokio::test]
async fn msearch_query_cannot_escape_the_partition_filter() {
    let p = pipeline();
    // An adversarial sub-search aiming at another tenant.
    let body = concat!("{}\n", "{\"query\":{\"term\":{\"_tenant\":\"globex\"}}}\n",);
    msearch(&p, body.as_bytes()).await;

    let dispatched = p.sink().recorded_searches();
    let q: Value = serde_json::from_slice(&dispatched[0].body).unwrap();
    // The client term is nested under `must`; the proxy filter is an
    // inseparable sibling pinning the caller's partition (docs/03 §5).
    assert_eq!(q["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
    assert_eq!(q["query"]["bool"]["must"][0]["term"]["_tenant"], "globex");
}
