use super::*;
use std::sync::Arc;

use osproxy_core::{ClusterId, FieldName, IndexName, PartitionId, PrincipalId, RequestId};
use osproxy_sink::MemorySink;
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, SensitivitySpec,
};
use osproxy_tenancy::PlacementTable;

struct Tenancy {
    table: Arc<PlacementTable>,
}

impl TenancySpi for Tenancy {
    fn partition_key(&self) -> PartitionKeySpec {
        // Ingest resolves from the body; by-id reads (no body) from a header.
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

fn pipeline() -> Pipeline<Tenancy, MemorySink> {
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
        TenancyRouter::new(Tenancy {
            table: Arc::clone(&table),
        }),
        MemorySink::new(),
    )
}

fn ctx<'a>(
    principal: &'a Principal,
    rid: &'a RequestId,
    headers: &'a [(String, String)],
    endpoint: EndpointKind,
    body: &'a [u8],
) -> RequestCtx<'a> {
    RequestCtx::new(
        principal,
        rid,
        HttpMethod::Put,
        endpoint,
        Protocol::Http1,
        "logical",
        HeaderView::new(headers),
        body,
    )
}

#[tokio::test]
async fn ingest_doc_returns_created_response() {
    let p = pipeline();
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":7}"#,
    );
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 201);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(body.contains(r#""_id":"acme:7""#), "{body}");
    assert!(body.contains(r#""result":"created""#));
}

#[tokio::test]
async fn unimplemented_endpoint_is_unsupported() {
    // Admin endpoints (`_cat`/`_cluster`) have no tenancy semantics and are not
    // wired in the pipeline — they fall through to a typed unsupported error.
    let p = pipeline();
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::Admin,
        br#"{"q":1}"#,
    );
    let err = p.handle(&c).await.unwrap_err();
    assert!(matches!(
        err,
        RequestError::Spi(SpiError::UnsupportedEndpoint {
            endpoint: EndpointKind::Admin
        })
    ));
}

#[tokio::test]
async fn explain_records_success_spans() {
    let p = pipeline();
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("trace-ok");
    let headers = vec![];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":7}"#,
    );
    p.handle(&c).await.unwrap();

    let doc = p.explain(&rid).expect("trace recorded");
    assert_eq!(doc["outcome"], "ok");
    assert_eq!(doc["spans"]["spi.resolve"]["partition_id"], "acme");
    assert_eq!(doc["spans"]["spi.resolve"]["routing"], true);
    assert_eq!(
        doc["spans"]["rewrite"]["transform_kind"],
        "inject+construct_id"
    );
    assert_eq!(doc["spans"]["egress"]["status"], 201);
    assert!(doc["error"].is_null());
}

#[tokio::test]
async fn explain_records_failure_with_remediation() {
    // A placement-missing failure: the reference table here always resolves,
    // so drive an unsupported endpoint instead — still a recorded failure.
    let p = pipeline();
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("trace-err");
    let headers = vec![];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestBulk,
        br#"{"q":1}"#,
    );
    let _ = p.handle(&c).await;

    let doc = p.explain(&rid).expect("trace recorded");
    assert_eq!(doc["outcome"], "error");
    assert_eq!(doc["error"]["code"], "unsupported_endpoint");
    assert!(doc["error"]["remediation"].is_string());
}
