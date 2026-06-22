//! Graceful shutdown (NFR-R5): when the shutdown signal fires, a request already
//! in flight runs to completion (the connection is drained), and the serve future
//! then returns, it does not abandon the in-flight request.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_transport::{serve_with_shutdown, IngressHandler, IngressRequest, IngressResponse};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Notify};

/// A handler that parks each request until released, signalling when it has
/// arrived, so the test can hold a request *in flight* across the shutdown.
struct GatedHandler {
    arrived: Arc<Notify>,
    release: Arc<Notify>,
}

impl IngressHandler for GatedHandler {
    async fn handle(&self, _req: IngressRequest) -> IngressResponse {
        self.arrived.notify_one();
        self.release.notified().await;
        IngressResponse::json(200, br#"{"drained":true}"#.to_vec())
    }
}

#[tokio::test]
async fn an_in_flight_request_is_drained_before_shutdown_returns() {
    let arrived = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler = Arc::new(GatedHandler {
        arrived: arrived.clone(),
        release: release.clone(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        serve_with_shutdown(listener, handler, async {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // Fire a request and leave it parked in the handler (in flight).
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req_task = tokio::spawn(async move {
        client
            .request(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("http://{addr}/orders/_doc"))
                    .body(Full::new(Bytes::from_static(b"{}")))
                    .unwrap(),
            )
            .await
    });
    arrived.notified().await; // the request has reached the handler

    // Now signal shutdown while the request is still in flight, then release it.
    shutdown_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await; // let the drain begin
    release.notify_one();

    // The in-flight request still completes with its real response (not dropped).
    let resp = req_task.await.unwrap().unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], br#"{"drained":true}"#);

    // And the serve future returns promptly once the connection has drained
    // (well within the drain deadline, so the test isn't waiting it out).
    let served = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve should return after draining")
        .unwrap();
    assert!(served.is_ok(), "graceful shutdown returns Ok");
}

#[tokio::test]
async fn shutdown_with_no_traffic_returns_immediately() {
    let handler = Arc::new(GatedHandler {
        arrived: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        serve_with_shutdown(listener, handler, async {
            let _ = shutdown_rx.await;
        })
        .await
    });
    shutdown_tx.send(()).unwrap();
    let served = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("idle shutdown returns at once")
        .unwrap();
    assert!(served.is_ok());
}
