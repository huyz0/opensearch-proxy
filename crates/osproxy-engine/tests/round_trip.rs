//! The headline M2 correctness property: a document written through the proxy
//! reads back through the proxy as the client's **original logical document**
//! (`docs/11` M2, `docs/03`).
//!
//! This is the full write→read round-trip — not just the write-side inverse the
//! `osproxy-rewrite` symmetry test proves — exercised over arbitrary documents
//! against an in-memory sink that faithfully emulates the OpenSearch get-by-id
//! envelope. Whatever the ingest path injects (the `_tenant` field, the
//! partition-prefixed `_id`, `_routing`), the read path strips, so the tenant
//! never observes the proxy's tenancy machinery.

// Test scaffolding (helpers + a spawned tenancy impl, not `#[test]` fns).
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
use proptest::prelude::*;
use serde_json::{json, Map, Value};

const PARTITION: &str = "acme";
const LOGICAL_INDEX: &str = "orders";

/// A shared-index tenancy: partition from the `tenant_id` body field on ingest
/// or the `x-tenant` header on by-id reads; `_tenant` injected; partition-
/// prefixed routed id.
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
        PartitionId::from(PARTITION),
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

async fn write(
    p: &Pipeline<TenancyRouter<SharedTenancy>, MemorySink>,
    body: &[u8],
) -> PipelineResponse {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("w");
    let headers = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        LOGICAL_INDEX,
        HeaderView::new(&headers),
        body,
    );
    p.handle(&ctx).await.unwrap()
}

async fn read(
    p: &Pipeline<TenancyRouter<SharedTenancy>, MemorySink>,
    logical_id: &str,
) -> PipelineResponse {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![("x-tenant".to_owned(), PARTITION.to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Get,
        EndpointKind::GetById,
        Protocol::Http1,
        LOGICAL_INDEX,
        HeaderView::new(&headers),
        b"",
    )
    .with_doc_id(Some(logical_id));
    p.handle(&ctx).await.unwrap()
}

/// An arbitrary client object with a stable `id` natural key and `tenant_id`,
/// plus a few payload fields whose keys never start with `_` (so they cannot
/// collide with injected tenancy fields) and are never `id`/`tenant_id`.
fn client_doc() -> impl Strategy<Value = (i64, Value)> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        "[a-z ]{0,12}".prop_map(Value::from),
    ];
    let extras = prop::collection::vec(("[a-z]{1,6}", leaf), 0..5);
    (any::<i64>(), extras).prop_map(|(id, entries)| {
        let mut obj = Map::new();
        obj.insert("tenant_id".to_owned(), json!(PARTITION));
        obj.insert("id".to_owned(), json!(id));
        for (k, v) in entries {
            if k != "id" && k != "tenant_id" {
                obj.insert(k, v);
            }
        }
        (id, Value::Object(obj))
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn write_then_read_recovers_the_logical_document((id, doc) in client_doc()) {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let p = pipeline();
            let body = serde_json::to_vec(&doc).unwrap();

            let w = write(&p, &body).await;
            prop_assert_eq!(w.status, 201);

            let r = read(&p, &id.to_string()).await;
            prop_assert_eq!(r.status, 200);
            let got: Value = serde_json::from_slice(&r.body).unwrap();

            // The client sees its logical id, the logical index, and no tenancy
            // machinery — and the `_source` is exactly the document it wrote.
            prop_assert_eq!(&got["_id"], &json!(id.to_string()));
            prop_assert_eq!(&got["_index"], &json!(LOGICAL_INDEX));
            prop_assert!(got.get("_routing").is_none());
            prop_assert!(got["_source"].get("_tenant").is_none());
            prop_assert_eq!(&got["_source"], &doc);
            Ok(())
        })?;
    }
}
