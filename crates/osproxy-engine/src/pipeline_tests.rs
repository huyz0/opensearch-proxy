use super::*;
use std::sync::Arc;

use osproxy_core::{ClusterId, FieldName, IndexName, PartitionId, PrincipalId, RequestId};
use osproxy_sink::MemorySink;
use osproxy_spi::{
    BodyDoc, DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, SensitivitySpec, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter};

struct Tenancy {
    table: Arc<PlacementTable>,
}

impl TenancySpi for Tenancy {
    fn resolve_partition(
        &self,
        ctx: &osproxy_spi::RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<osproxy_core::PartitionId, osproxy_spi::SpiError> {
        // Ingest resolves from the body; by-id reads (no body) from a header.
        let spec = PartitionKeySpec::AnyOf(vec![
            PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            PartitionKeySpec::Header("x-tenant".to_owned()),
        ]);
        osproxy_tenancy::resolve_partition_spec(&spec, ctx, body)
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

fn pipeline() -> Pipeline<TenancyRouter<Tenancy>, MemorySink> {
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
    // The response echoes the client's *logical* id (`7`), not the partition-
    // prefixed physical id (`acme:7`) the proxy wrote upstream, so the id round-
    // trips: a later GET/DELETE of `7` resolves to the same document (`docs/03` §4).
    assert!(body.contains(r#""_id":"7""#), "{body}");
    assert!(
        !body.contains("acme:7"),
        "physical id must not leak: {body}"
    );
    assert!(body.contains(r#""result":"created""#));
}

#[tokio::test]
async fn unimplemented_endpoint_is_unsupported() {
    // Admin endpoints (`_cat`/`_cluster`) have no tenancy semantics and are not
    // wired in the pipeline, they fall through to a typed unsupported error.
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
async fn a_global_aggregation_search_is_rejected_before_dispatch() {
    // End-to-end through the live dispatch (not just the pure `wrap_query` unit):
    // a shared-index `_search` carrying a `global` aggregation, which OpenSearch
    // evaluates across the whole index ignoring the partition filter, is refused
    // at the rewrite stage (NFR-S4, `docs/03` §5), so it never reaches the sink.
    let p = pipeline();
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    // A search body carries the query, so the partition resolves from the header.
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::Search,
        br#"{"size":0,"aggs":{"leak":{"global":{},"aggs":{"all":{"top_hits":{"size":50}}}}}}"#,
    );
    let err = p.handle(&c).await.unwrap_err();
    // Surfaced as a rejected request shape (the isolation guard), non-retryable.
    assert!(matches!(err, RequestError::Rewrite(_)), "{err:?}");
    assert_eq!(err.code(), osproxy_core::ErrorCode::UnsupportedEndpoint);
    assert!(!err.retryable());
    // The in-memory sink saw no search: the request failed before any dispatch.
    assert!(
        p.sink().recorded_searches().is_empty(),
        "must not reach the cluster"
    );
}

/// An exporter that reports itself enabled, so the tracing-gate treats the proxy
/// as adding a span of its own.
#[derive(Debug)]
struct OnExporter;
impl osproxy_observe::SpanExporter for OnExporter {
    fn enabled(&self) -> bool {
        true
    }
    fn export(&self, _payload: serde_json::Value) {}
}

#[test]
fn upstream_trace_is_gated_on_span_export() {
    // The transparent-tracing rule: with export off (the default) the proxy adds
    // no span and injects no `traceparent` upstream (the client's own trace
    // headers ride through the forwarded set instead); with export on it injects
    // its hop so the upstream span nests under the proxy's.
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![("traceparent".to_owned(), "00-abc-def-01".to_owned())];
    let c = ctx(&principal, &rid, &headers, EndpointKind::Search, b"{}");

    let off = pipeline();
    assert!(
        off.upstream_trace(&c).is_none(),
        "no proxy traceparent injected when span export is off"
    );

    let on = pipeline().with_exporter(std::sync::Arc::new(OnExporter));
    assert!(
        on.upstream_trace(&c).is_some(),
        "the proxy injects its span when span export is on"
    );
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
    // so drive an unsupported endpoint instead, still a recorded failure.
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

#[path = "pipeline_async_tests.rs"]
mod async_mode;
