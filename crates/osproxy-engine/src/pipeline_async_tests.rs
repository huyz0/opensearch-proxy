//! Async fan-out write-mode tests (`docs/04` §9). Split from `pipeline_tests.rs`
//! to keep each test file within the length budget; shares that module's
//! `pipeline()`/`ctx()` harness via `use super::*`.

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
