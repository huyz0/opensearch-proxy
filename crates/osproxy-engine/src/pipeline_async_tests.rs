//! Async fan-out write-mode tests (`docs/04` §9). Split from `pipeline_tests.rs`;
//! shares that module's `pipeline()`/`ctx()` harness via `use super::*`.
//
// JUSTIFY(file-length): one cohesive suite for the async write mode — single-doc,
// bulk, and delete-by-query all exercise the same `RecordingQueue` + `header`
// scaffolding defined here; splitting by sub-path would duplicate that harness
// across files (and the shared `pipeline()`/`ctx()` is reachable only as a
// sibling module). Kept together as the one place the mode's behavior is proven.

use super::*;
use crate::asyncwrite::{QueueError, QueuedWrite, WriteQueue};
use osproxy_core::{EndpointKind, PrincipalId, RequestId};
use osproxy_spi::Principal;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// A recording queue for tests: captures every enqueued write, and can be told
/// to refuse so the enqueue-failure path is exercised.
#[derive(Default)]
struct RecordingQueue {
    writes: Mutex<Vec<QueuedWrite>>,
    fail: bool,
}

impl WriteQueue for RecordingQueue {
    fn enabled(&self) -> bool {
        true
    }
    fn enqueue<'a>(
        &'a self,
        write: QueuedWrite,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + 'a>> {
        Box::pin(async move {
            if self.fail {
                return Err(QueueError {
                    reason: "broker unavailable",
                });
            }
            self.writes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(write);
            Ok(())
        })
    }
}

fn header(name: &str, value: &str) -> (String, String) {
    (name.to_owned(), value.to_owned())
}

#[tokio::test]
async fn async_ingest_enqueues_and_returns_202_without_touching_the_sink() {
    let queue = Arc::new(RecordingQueue::default());
    let p = pipeline().with_write_queue(Arc::clone(&queue) as Arc<dyn WriteQueue>);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![header("x-write-mode", "async")];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":7}"#,
    );
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 202);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(body.contains(r#""status":"accepted""#), "{body}");
    assert!(body.contains(r#""result":"queued""#), "{body}");
    // op id defaults to the request id when no X-Op-Id is supplied.
    assert!(body.contains(r#""op_id":"r""#), "{body}");

    // The op was durably enqueued — and never forwarded to the upstream sink.
    let writes = queue.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].partition_key, "acme");
    assert_eq!(writes[0].op_id, "r");
    assert!(
        p.sink().recorded().is_empty(),
        "sync sink must stay untouched"
    );
}

#[tokio::test]
async fn async_request_with_no_queue_is_refused_422() {
    // The default pipeline has NoQueue: async must be refused, never dropped.
    let p = pipeline();
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![header("x-write-mode", "async")];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":7}"#,
    );
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 422);
}

#[tokio::test]
async fn async_client_supplied_op_id_is_honored_and_invalid_falls_back() {
    let queue = Arc::new(RecordingQueue::default());
    let p = pipeline().with_write_queue(Arc::clone(&queue) as Arc<dyn WriteQueue>);
    let principal = Principal::new(PrincipalId::from("svc"));

    // A valid client op id rides through to the queued write.
    let rid = RequestId::from("r1");
    let headers = vec![
        header("x-write-mode", "async"),
        header("x-op-id", "client-key-1"),
    ];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":1}"#,
    );
    p.handle(&c).await.unwrap();

    // A malformed op id is ignored in favor of the proxy request id.
    let rid2 = RequestId::from("r2");
    let headers2 = vec![
        header("x-write-mode", "async"),
        header("x-op-id", "bad key"),
    ];
    let c2 = ctx(
        &principal,
        &rid2,
        &headers2,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":2}"#,
    );
    p.handle(&c2).await.unwrap();

    let writes = queue.writes.lock().unwrap();
    assert_eq!(writes[0].op_id, "client-key-1");
    assert_eq!(writes[1].op_id, "r2");
}

#[tokio::test]
async fn async_enqueue_failure_is_reported_503() {
    let queue = Arc::new(RecordingQueue {
        fail: true,
        ..Default::default()
    });
    let p = pipeline().with_write_queue(queue as Arc<dyn WriteQueue>);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![header("x-write-mode", "async")];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":7}"#,
    );
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 503);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(body.contains(r#""op_id":"r""#), "{body}");
}

#[tokio::test]
async fn sync_remains_the_default_without_a_header() {
    let queue = Arc::new(RecordingQueue::default());
    let p = pipeline().with_write_queue(Arc::clone(&queue) as Arc<dyn WriteQueue>);
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
    assert!(
        queue.writes.lock().unwrap().is_empty(),
        "sync must not enqueue"
    );
}

#[tokio::test]
async fn baseline_async_makes_fan_out_the_default() {
    let queue = Arc::new(RecordingQueue::default());
    let p = pipeline()
        .with_write_queue(Arc::clone(&queue) as Arc<dyn WriteQueue>)
        .with_baseline_write_mode(crate::asyncwrite::WriteMode::Async);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    // No header: the deployment baseline selects async.
    let headers = vec![];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":7}"#,
    );
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 202);
    assert_eq!(queue.writes.lock().unwrap().len(), 1);

    // ...and an explicit per-request sync header overrides the baseline.
    let rid2 = RequestId::from("r2");
    let headers2 = vec![header("x-write-mode", "sync")];
    let c2 = ctx(
        &principal,
        &rid2,
        &headers2,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":8}"#,
    );
    let resp2 = p.handle(&c2).await.unwrap();
    assert_eq!(resp2.status, 201);
    assert_eq!(
        queue.writes.lock().unwrap().len(),
        1,
        "sync override must not enqueue"
    );
}

#[tokio::test]
async fn async_rejects_optimistic_concurrency_with_400() {
    let queue = Arc::new(RecordingQueue::default());
    let p = pipeline().with_write_queue(Arc::clone(&queue) as Arc<dyn WriteQueue>);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![header("x-write-mode", "async")];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":7}"#,
    )
    .with_query(Some("if_seq_no=3&if_primary_term=1"));
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 400);
    let body = String::from_utf8(resp.body).unwrap();
    assert!(body.contains("optimistic concurrency"), "{body}");
    assert!(
        queue.writes.lock().unwrap().is_empty(),
        "rejected op must not enqueue"
    );
}

#[tokio::test]
async fn async_rejects_scripted_update_path_with_400() {
    let queue = Arc::new(RecordingQueue::default());
    let p = pipeline().with_write_queue(Arc::clone(&queue) as Arc<dyn WriteQueue>);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![header("x-write-mode", "async")];
    // The canonical OpenSearch update path is `/{index}/_update/{id}`.
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestDoc,
        br#"{"tenant_id":"acme","id":7,"doc":{"x":1}}"#,
    )
    .with_path("/orders/_update/7");
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 400);
    assert!(queue.writes.lock().unwrap().is_empty());
}

// --- async bulk (docs/04 §9) ----------------------------------------------

const ASYNC_BULK: &[u8] = b"{\"index\":{\"_id\":\"1\"}}\n{\"tenant_id\":\"acme\",\"id\":1}\n{\"update\":{\"_id\":\"acme:9\"}}\n{\"doc\":{\"x\":1}}\n{\"delete\":{\"_id\":\"acme:2\"}}\n";

#[tokio::test]
async fn async_bulk_enqueues_each_item_with_a_per_item_op_id() {
    let queue = Arc::new(RecordingQueue::default());
    let p = pipeline().with_write_queue(Arc::clone(&queue) as Arc<dyn WriteQueue>);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![header("x-write-mode", "async"), header("x-tenant", "acme")];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestBulk,
        ASYNC_BULK,
    );
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 200);

    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    // index → queued with op id "r:0"
    assert_eq!(items[0]["index"]["status"], 202);
    assert_eq!(items[0]["index"]["result"], "queued");
    assert_eq!(items[0]["index"]["op_id"], "r:0");
    // update → rejected 400, not enqueued
    assert_eq!(items[1]["update"]["status"], 400);
    // delete → queued with op id "r:2"
    assert_eq!(items[2]["delete"]["status"], 202);
    assert_eq!(items[2]["delete"]["op_id"], "r:2");
    assert_eq!(body["errors"], true); // the rejected update sets the flag

    // Only the two honorable ops were enqueued, keyed by their partition.
    let writes = queue.writes.lock().unwrap();
    assert_eq!(writes.len(), 2);
    assert!(writes.iter().all(|w| w.partition_key == "acme"));
    assert_eq!(writes[0].op_id, "r:0");
    assert_eq!(writes[1].op_id, "r:2");
}

#[tokio::test]
async fn async_bulk_with_no_queue_is_refused_422() {
    let p = pipeline(); // default NoQueue
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![header("x-write-mode", "async"), header("x-tenant", "acme")];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::IngestBulk,
        ASYNC_BULK,
    );
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 422);
}

// --- async _delete_by_query expansion (docs/04 §9) ------------------------

/// Stores two docs (sync), then runs an async DBQ that expands to one enqueued
/// delete per match — the sink is never asked to delete; the queue is.
#[tokio::test]
async fn async_delete_by_query_expands_to_one_enqueued_delete_per_match() {
    let queue = Arc::new(RecordingQueue::default());
    let p = pipeline()
        .with_write_queue(Arc::clone(&queue) as Arc<dyn WriteQueue>)
        .with_delete_by_query_expansion(true);
    let principal = Principal::new(PrincipalId::from("svc"));

    // Seed two docs synchronously (physical ids acme:1, acme:2).
    for id in [1, 2] {
        let rid = RequestId::from("seed");
        let body = format!(r#"{{"tenant_id":"acme","id":{id}}}"#);
        let c = ctx(
            &principal,
            &rid,
            &[],
            EndpointKind::IngestDoc,
            body.as_bytes(),
        );
        p.handle(&c).await.unwrap();
    }
    assert!(
        queue.writes.lock().unwrap().is_empty(),
        "sync seeds must not enqueue"
    );

    // Async DBQ.
    let rid = RequestId::from("r");
    let headers = vec![header("x-write-mode", "async"), header("x-tenant", "acme")];
    let c = ctx(
        &principal,
        &rid,
        &headers,
        EndpointKind::DeleteByQuery,
        br#"{"query":{"match_all":{}}}"#,
    );
    let resp = p.handle(&c).await.unwrap();
    assert_eq!(resp.status, 200);
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["total"], 2);
    assert_eq!(body["deleted"], 2);
    assert_eq!(body["version_conflicts"], 0);

    // Two concrete deletes were enqueued (not dispatched to the sink), keyed by
    // partition, with per-match op ids.
    let writes = queue.writes.lock().unwrap();
    assert_eq!(writes.len(), 2);
    let mut ids: Vec<String> = writes
        .iter()
        .filter_map(|w| match &w.batch.ops()[0].doc {
            osproxy_sink::DocOp::Delete { id, .. } => Some(id.clone()),
            _ => None,
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["acme:1".to_owned(), "acme:2".to_owned()]);
    assert_eq!(writes[0].op_id, "r:0");
    assert_eq!(writes[1].op_id, "r:1");
}

#[tokio::test]
async fn delete_by_query_is_rejected_unless_async_and_expansion_enabled() {
    let queue = Arc::new(RecordingQueue::default());
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let dbq = br#"{"query":{"match_all":{}}}"#;
    let tenant = vec![header("x-tenant", "acme")];
    let h = vec![header("x-write-mode", "async"), header("x-tenant", "acme")];
    let q = || Arc::clone(&queue) as Arc<dyn WriteQueue>;

    // Sync (no async header) though expansion is on → 400.
    let p = pipeline()
        .with_write_queue(q())
        .with_delete_by_query_expansion(true);
    let c = ctx(&principal, &rid, &tenant, EndpointKind::DeleteByQuery, dbq);
    assert_eq!(p.handle(&c).await.unwrap().status, 400);

    // Async but expansion off → 400.
    let p = pipeline().with_write_queue(q());
    let c = ctx(&principal, &rid, &h, EndpointKind::DeleteByQuery, dbq);
    assert_eq!(p.handle(&c).await.unwrap().status, 400);

    // Async + expansion on but no queue → 422.
    let p = pipeline().with_delete_by_query_expansion(true);
    let c = ctx(&principal, &rid, &h, EndpointKind::DeleteByQuery, dbq);
    assert_eq!(p.handle(&c).await.unwrap().status, 422);

    assert!(
        queue.writes.lock().unwrap().is_empty(),
        "no deletes enqueued on rejection"
    );
}
