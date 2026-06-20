//! The placement backend is polled fresh per request; a *momentary*
//! unavailability is retried with backoff in-proxy rather than failing the write
//! outright (`docs/06` §3a). Proven through the full ingest pipeline with a
//! tenancy whose `placement_for` fails a bounded number of times before
//! recovering, and one that never recovers.

#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use osproxy_core::{
    ClusterId, EndpointKind, ErrorCode, FieldName, IndexName, PartitionId, PrincipalId, RequestId,
};
use osproxy_engine::{Pipeline, PipelineResponse, RequestError, RetryPolicy};
use osproxy_sink::MemorySink;
use osproxy_spi::{
    BodyDoc, HeaderView, HttpMethod, InjectedField, InjectedValue, JsonPath, PartitionKeySpec,
    Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec, SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter};
use serde_json::json;

/// A tenancy that reports the backend unavailable for its first `fail_first`
/// placement lookups, then resolves normally from the table.
struct FlakyTenancy {
    table: Arc<PlacementTable>,
    fail_first: u32,
    calls: AtomicU32,
}

impl TenancySpi for FlakyTenancy {
    fn resolve_partition(
        &self,
        ctx: &osproxy_spi::RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<osproxy_core::PartitionId, osproxy_spi::SpiError> {
        // Header-or-body: ingest carries the tenant in the doc; a bodyless read
        // (`_mget`/`_msearch`) carries it in `x-tenant`. Order is BodyField first so
        // existing ingest tests (no header) resolve exactly as before.
        osproxy_tenancy::resolve_partition_spec(
            &PartitionKeySpec::AnyOf(vec![
                PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
                PartitionKeySpec::Header("x-tenant".to_owned()),
            ]),
            ctx,
            body,
        )
    }
    fn doc_id_rule(&self) -> Option<osproxy_spi::DocIdRule> {
        // SharedIndex requires a partition-scoped id (docs/03 §4), enforced by the
        // router; provide one so this retry test uses a valid tenancy config.
        Some(osproxy_spi::DocIdRule::new(osproxy_spi::IdTemplate::new(
            "{partition}:{body.id}",
        )))
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
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.fail_first {
            return Err(SpiError::PlacementBackend { retryable: true });
        }
        self.table
            .get(partition)
            .ok_or_else(|| SpiError::PlacementMissing {
                partition: partition.clone(),
            })
    }
}

fn pipeline(fail_first: u32) -> Pipeline<TenancyRouter<FlakyTenancy>, MemorySink> {
    let table = Arc::new(PlacementTable::new());
    table.set(
        PartitionId::from("acme"),
        Placement::SharedIndex {
            cluster: ClusterId::from("eu-1"),
            index: IndexName::from("orders-shared"),
            inject: vec![InjectedField::new(
                FieldName::from("_tenant"),
                InjectedValue::PartitionId,
            )],
        },
    );
    Pipeline::new(
        TenancyRouter::new(FlakyTenancy {
            table,
            fail_first,
            calls: AtomicU32::new(0),
        }),
        MemorySink::new(),
    )
    // Zero backoff keeps the test fast and deterministic; attempts is what matters.
    .with_retry_policy(RetryPolicy {
        max_attempts: 3,
        base_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    })
}

async fn ingest(
    p: &Pipeline<TenancyRouter<FlakyTenancy>, MemorySink>,
) -> Result<PipelineResponse, RequestError> {
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers: Vec<(String, String)> = vec![];
    let body = serde_json::to_vec(&json!({ "tenant_id": "acme", "id": 7, "msg": "hi" })).unwrap();
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "orders-logical",
        HeaderView::new(&headers),
        &body,
    );
    p.handle(&ctx).await
}

#[tokio::test]
async fn a_transient_backend_blip_is_retried_and_the_write_succeeds() {
    // Two failures then success — within the 3-attempt budget.
    let p = pipeline(2);
    let resp = ingest(&p).await.unwrap();
    assert!(resp.status >= 200 && resp.status < 300);
    assert_eq!(
        p.sink().recorded().len(),
        1,
        "the write committed after retry"
    );
}

#[tokio::test]
async fn a_persistently_unavailable_backend_surfaces_a_retryable_error() {
    // More failures than the attempt budget: the request fails, but retryably,
    // so the client (not the proxy) decides whether to try again later.
    let p = pipeline(u32::MAX);
    let err = ingest(&p).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::PlacementBackendUnavailable);
    assert!(err.retryable());
    assert!(p.sink().recorded().is_empty(), "nothing committed");
}

#[tokio::test]
async fn the_mget_per_item_resolve_retries_a_transient_blip() {
    // The demux read paths resolve each item's placement, and that per-item resolve
    // retries a transient backend blip too (symmetry with single-doc/bulk; `_msearch`
    // uses the identical `with_retry` construct). Two failures then success: the doc
    // resolves (no positioned `placement_missing`) rather than failing on the blip.
    let p = pipeline(2);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let body = serde_json::to_vec(&json!({ "docs": [{ "_id": "7" }] })).unwrap();
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::MultiGet,
        Protocol::Http1,
        "orders-logical",
        HeaderView::new(&headers),
        &body,
    );
    let resp = p.handle(&ctx).await.unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let entry = &doc["docs"][0];
    assert_ne!(
        entry["error"]["type"], "placement_missing",
        "the per-item resolve retried the blip rather than failing the item: {entry}"
    );
}
