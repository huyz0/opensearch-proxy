//! Bulk demux through the pipeline (`docs/04` §3): a mixed-partition `_bulk`
//! routes each document to its partition's target, preserves the original item
//! order in the response, isolates the per-target writes, and positions a
//! per-item failure in place while the rest proceed (200 + `errors:true`).

// Test scaffolding (helpers + a tenancy impl, not `#[test]` fns).
#![allow(clippy::unwrap_used)]
// JUSTIFY(file-length): the `_bulk` verb matrix (index/create/update/delete,
// mixed-partition demux, order preservation, per-item + upstream failures) shares
// one ~95-line tenancy/pipeline scaffold. Splitting would duplicate that scaffold
// per file; the behaviours read better proven side by side against it.

use std::sync::Arc;

use osproxy_core::{
    ClusterId, EndpointKind, FieldName, IndexName, PartitionId, PrincipalId, RequestId, Target,
};
use osproxy_engine::{Pipeline, PipelineResponse};
use osproxy_sink::{MemorySink, ReadOp, Reader, Sink, SinkError};
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter};
use proptest::prelude::*;
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

/// Two partitions on two **different** physical indices, so a mixed bulk demuxes
/// into two targets.
fn pipeline() -> Pipeline<TenancyRouter<SharedTenancy>, MemorySink> {
    let table = Arc::new(PlacementTable::new());
    for (partition, index) in [("acme", "acme-idx"), ("globex", "globex-idx")] {
        table.set(
            PartitionId::from(partition),
            Placement::SharedIndex {
                cluster: ClusterId::from("eu-1"),
                index: IndexName::from(index),
                inject: vec![InjectedField::new(
                    FieldName::from("_tenant"),
                    InjectedValue::PartitionId,
                )],
            },
        );
    }
    Pipeline::new(
        TenancyRouter::new(SharedTenancy { table }),
        MemorySink::new(),
    )
}

async fn bulk(
    p: &Pipeline<TenancyRouter<SharedTenancy>, MemorySink>,
    body: &[u8],
) -> PipelineResponse {
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

fn target(index: &str) -> Target {
    Target::new(ClusterId::from("eu-1"), IndexName::from(index))
}

#[tokio::test]
async fn mixed_partition_bulk_demuxes_preserves_order_and_isolates() {
    let p = pipeline();
    // Interleaved partitions, with an unresolvable doc (no tenant_id) at index 2.
    let body = concat!(
        "{\"index\":{}}\n{\"tenant_id\":\"acme\",\"id\":1,\"msg\":\"a1\"}\n",
        "{\"index\":{}}\n{\"tenant_id\":\"globex\",\"id\":2,\"msg\":\"g2\"}\n",
        "{\"index\":{}}\n{\"id\":3,\"msg\":\"orphan\"}\n",
        "{\"index\":{}}\n{\"tenant_id\":\"acme\",\"id\":4,\"msg\":\"a4\"}\n",
    );
    let resp = bulk(&p, body.as_bytes()).await;
    assert_eq!(resp.status, 200);
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["errors"], true, "{doc}");
    let items = doc["items"].as_array().unwrap();
    assert_eq!(items.len(), 4);

    // Order preserved; logical ids echoed; the orphan is a positioned error.
    assert_eq!(items[0]["index"]["_id"], "1");
    assert_eq!(items[0]["index"]["status"], 201);
    assert_eq!(items[1]["index"]["_id"], "2");
    assert_eq!(items[1]["index"]["status"], 201);
    assert_eq!(items[2]["index"]["status"], 400);
    assert_eq!(items[2]["index"]["error"]["type"], "partition_unresolved");
    assert_eq!(items[3]["index"]["_id"], "4");
    assert_eq!(items[3]["index"]["status"], 201);

    // Demux landed each partition's docs in its own physical index, isolated.
    let sink = p.sink();
    let a1 = sink
        .get(ReadOp::new(
            target("acme-idx"),
            "acme:1",
            Some("acme".into()),
        ))
        .await
        .unwrap();
    assert!(a1.found, "acme:1 should be in acme-idx");
    let g2 = sink
        .get(ReadOp::new(
            target("globex-idx"),
            "globex:2",
            Some("globex".into()),
        ))
        .await
        .unwrap();
    assert!(g2.found, "globex:2 should be in globex-idx");
    // The acme doc is NOT in the globex index (no cross-partition leakage).
    let cross = sink
        .get(ReadOp::new(target("globex-idx"), "acme:1", None))
        .await
        .unwrap();
    assert!(!cross.found, "acme:1 must not be in globex-idx");
}

#[tokio::test]
async fn all_succeed_reports_no_errors() {
    let p = pipeline();
    let body = concat!(
        "{\"index\":{}}\n{\"tenant_id\":\"acme\",\"id\":1}\n",
        "{\"delete\":{\"_id\":\"1\"}}\n",
    );
    // Use a header to route the delete (it has no body to carry the partition).
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("b");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestBulk,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        body.as_bytes(),
    );
    let resp = p.handle(&ctx).await.unwrap();
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["errors"], false, "{doc}");
    let items = doc["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["index"]["result"], "created");
    assert_eq!(items[1]["delete"]["_id"], "1");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Order preservation: for any interleaving of acme/globex index ops, the
    /// response `items[]` are in the input order, each echoing its own id — the
    /// re-interleave must not reorder across the per-target demux (docs/09).
    #[test]
    fn bulk_preserves_item_order(ops in prop::collection::vec(
        (prop_oneof![Just("acme"), Just("globex")], 0u32..1000),
        0..12,
    )) {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            use std::fmt::Write as _;
            let p = pipeline();
            let mut body = String::new();
            for (tenant, id) in &ops {
                let _ = write!(
                    body,
                    "{{\"index\":{{}}}}\n{{\"tenant_id\":\"{tenant}\",\"id\":{id}}}\n"
                );
            }
            let resp = bulk(&p, body.as_bytes()).await;
            let doc: Value = serde_json::from_slice(&resp.body).unwrap();
            let items = doc["items"].as_array().unwrap();
            prop_assert_eq!(items.len(), ops.len());
            for (item, (_tenant, id)) in items.iter().zip(&ops) {
                prop_assert_eq!(&item["index"]["_id"], &Value::from(id.to_string()));
                prop_assert_eq!(&item["index"]["status"], &Value::from(201));
            }
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn per_item_errors_are_positioned_and_typed() {
    let p = pipeline();
    let body = concat!(
        "{\"update\":{}}\n{\"doc\":{}}\n", // update with no id
        "{\"delete\":{}}\n",               // delete with no id
        "{\"index\":{}}\n{\"tenant_id\":\"ghost\",\"id\":9}\n", // no placement
    );
    // An x-tenant header so the delete (no body) resolves a partition and fails
    // specifically on the missing id; the ghost index doc resolves via its body.
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("b");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestBulk,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        body.as_bytes(),
    );
    let resp = p.handle(&ctx).await.unwrap();
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["errors"], true);
    let items = doc["items"].as_array().unwrap();
    assert_eq!(items[0]["update"]["error"]["type"], "update_without_id");
    assert_eq!(items[1]["delete"]["error"]["type"], "delete_without_id");
    assert_eq!(items[2]["index"]["error"]["type"], "placement_missing");
    assert_eq!(items[2]["index"]["status"], 404);
}

#[tokio::test]
async fn action_line_id_is_mapped_logical_to_physical() {
    let p = pipeline();
    // The action line supplies an explicit _id (the logical/natural key).
    let body = "{\"index\":{\"_id\":\"99\"}}\n{\"tenant_id\":\"acme\",\"id\":1}\n";
    let resp = bulk(&p, body.as_bytes()).await;
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["items"][0]["index"]["_id"], "99"); // logical id echoed

    // Stored under the partition-prefixed physical id.
    let hit = p
        .sink()
        .get(ReadOp::new(
            target("acme-idx"),
            "acme:99",
            Some("acme".into()),
        ))
        .await
        .unwrap();
    assert!(hit.found, "acme:99 should be stored");
}

#[tokio::test]
async fn large_bulk_flushes_mid_stream_without_dropping_or_reordering() {
    use std::fmt::Write as _;
    // More than the engine's FLUSH_THRESHOLD (256) acme ops, so at least one
    // sub-batch is flushed mid-stream before the body is fully parsed. The
    // re-interleave and storage must survive that flush boundary intact.
    const COUNT: usize = 600;
    let p = pipeline();
    let mut body = String::new();
    for id in 0..COUNT {
        let _ = write!(
            body,
            "{{\"index\":{{}}}}\n{{\"tenant_id\":\"acme\",\"id\":{id},\"msg\":\"m{id}\"}}\n"
        );
    }
    let resp = bulk(&p, body.as_bytes()).await;
    assert_eq!(resp.status, 200);
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["errors"], false, "no errors expected");
    let items = doc["items"].as_array().unwrap();
    assert_eq!(items.len(), COUNT);
    // Every item is in input order, each echoing its own id, all created.
    for (id, item) in items.iter().enumerate() {
        assert_eq!(item["index"]["_id"], id.to_string(), "order preserved");
        assert_eq!(item["index"]["status"], 201);
    }

    // A doc from the first (mid-stream) flush and one from the final flush are
    // both stored — nothing was dropped at the boundary.
    for id in ["0", "599"] {
        let hit = p
            .sink()
            .get(ReadOp::new(
                target("acme-idx"),
                format!("acme:{id}"),
                Some("acme".into()),
            ))
            .await
            .unwrap();
        assert!(hit.found, "acme:{id} should be stored");
    }
}

#[tokio::test]
async fn create_action_routes_through_the_create_op() {
    let p = pipeline();
    // A `create` carries the same tenancy rewrite as `index`, but the demuxed op
    // targets `_create` upstream (fail-if-exists). Against the memory sink it
    // succeeds and stores under the partition-prefixed physical id.
    let body = "{\"create\":{}}\n{\"tenant_id\":\"acme\",\"id\":5,\"msg\":\"c5\"}\n";
    let resp = bulk(&p, body.as_bytes()).await;
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["errors"], false, "{doc}");
    assert_eq!(doc["items"][0]["create"]["_id"], "5");
    assert_eq!(doc["items"][0]["create"]["status"], 201);

    let hit = p
        .sink()
        .get(ReadOp::new(
            target("acme-idx"),
            "acme:5",
            Some("acme".into()),
        ))
        .await
        .unwrap();
    assert!(hit.found, "acme:5 should be stored by the create op");
}

#[tokio::test]
async fn update_upsert_injects_tenancy_and_round_trips() {
    let p = pipeline();
    // An upsert for a not-yet-existing doc: the engine maps the logical id to the
    // physical id and injects `_tenant` into the upsert, so the created document
    // is isolated. (The header carries the partition; the update body does not.)
    let body = concat!(
        "{\"update\":{\"_id\":\"5\"}}\n",
        "{\"doc\":{\"msg\":\"patched\"},\"upsert\":{\"msg\":\"made\"}}\n",
    );
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("b");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestBulk,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        body.as_bytes(),
    );
    let resp = p.handle(&ctx).await.unwrap();
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["errors"], false, "{doc}");
    assert_eq!(doc["items"][0]["update"]["_id"], "5");

    // The upserted document landed under the physical id, carrying `_tenant`.
    let hit = p
        .sink()
        .get(ReadOp::new(
            target("acme-idx"),
            "acme:5",
            Some("acme".into()),
        ))
        .await
        .unwrap();
    assert!(hit.found, "acme:5 should exist after the upsert");
    let stored: Value = serde_json::from_slice(&hit.body).unwrap();
    assert_eq!(stored["_source"]["_tenant"], "acme");
    assert_eq!(stored["_source"]["msg"], "made");
}

#[tokio::test]
async fn upstream_failure_positions_502_for_that_target() {
    // A sink that always fails the write, so the dispatch error path is taken.
    struct FailSink;
    impl Sink for FailSink {
        async fn write(
            &self,
            _b: osproxy_sink::WriteBatch,
        ) -> Result<osproxy_sink::WriteAck, SinkError> {
            Err(SinkError::Transport { kind: "boom" })
        }
    }
    impl Reader for FailSink {
        async fn get(&self, _o: ReadOp) -> Result<osproxy_sink::ReadOutcome, SinkError> {
            unreachable!("reads not exercised here")
        }
        async fn search(
            &self,
            _o: osproxy_sink::SearchOp,
        ) -> Result<osproxy_sink::SearchOutcome, SinkError> {
            unreachable!("searches not exercised here")
        }
        async fn count(
            &self,
            _o: osproxy_sink::SearchOp,
        ) -> Result<osproxy_sink::CountOutcome, SinkError> {
            unreachable!("counts not exercised here")
        }
    }

    let table = Arc::new(PlacementTable::new());
    table.set(
        PartitionId::from("acme"),
        Placement::SharedIndex {
            cluster: ClusterId::from("eu-1"),
            index: IndexName::from("acme-idx"),
            inject: vec![InjectedField::new(
                FieldName::from("_tenant"),
                InjectedValue::PartitionId,
            )],
        },
    );
    let p = Pipeline::new(TenancyRouter::new(SharedTenancy { table }), FailSink);

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
        b"{\"index\":{}}\n{\"tenant_id\":\"acme\",\"id\":1}\n",
    );
    let resp = p.handle(&ctx).await.unwrap();
    let doc: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(doc["errors"], true);
    assert_eq!(doc["items"][0]["index"]["status"], 502);
    assert_eq!(doc["items"][0]["index"]["error"]["type"], "upstream_failed");
}

#[tokio::test]
async fn malformed_bulk_body_is_a_request_error() {
    let p = pipeline();
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("b");
    let headers = vec![];
    // An index action with no following source line: the whole body is rejected
    // as a request error, not a 200 with per-item errors.
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestBulk,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        b"{\"index\":{}}\n",
    );
    assert!(p.handle(&ctx).await.is_err());
}
