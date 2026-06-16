//! Drives the [`serve`] loop with a real HTTP client: a request goes over a TCP
//! socket, is parsed and classified, reaches a handler, and the response comes
//! back. Proves the wire round-trip end to end without the engine.

// Test scaffolding (helper fns and a spawned server task, not `#[test]` fns), so
// the test-only unwrap allowance does not reach it.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_core::EndpointKind;
use osproxy_transport::{
    serve, serve_with_limits, IngressHandler, IngressLimits, IngressRequest, IngressResponse,
};
use tokio::net::TcpListener;

/// Echoes the parsed classification back as JSON so the test can assert on it.
struct EchoHandler;

impl IngressHandler for EchoHandler {
    async fn handle(&self, req: IngressRequest) -> IngressResponse {
        let ingest = req.endpoint == EndpointKind::IngestDoc;
        let body = format!(
            r#"{{"index":"{}","doc_id":"{}","body_len":{},"ingest":{ingest},"protocol":"{:?}"}}"#,
            req.logical_index,
            req.doc_id.unwrap_or_default(),
            req.body.len(),
            req.protocol,
        );
        IngressResponse::json(201, body.into_bytes())
    }
}

#[tokio::test]
async fn put_doc_round_trips_through_the_ingress() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(listener, Arc::new(EchoHandler)).await;
    });

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::PUT)
        .uri(format!("http://{addr}/orders/_doc/acme:1"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(br#"{"tenant_id":"acme"}"#)))
        .unwrap();

    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 201);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();

    assert!(text.contains(r#""index":"orders""#), "{text}");
    assert!(text.contains(r#""doc_id":"acme:1""#), "{text}");
    assert!(text.contains(r#""ingest":true"#), "{text}");
}

#[tokio::test]
async fn body_over_the_per_request_cap_is_413() {
    let limits = IngressLimits {
        max_body_bytes: 16,
        inflight_ceiling: 1024,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve_with_limits(listener, Arc::new(EchoHandler), limits).await;
    });

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{addr}/orders/_bulk"))
        // 20 bytes > the 16-byte per-request cap.
        .body(Full::new(Bytes::from_static(b"01234567890123456789")))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 413);
}

#[tokio::test]
async fn body_over_the_inflight_ceiling_is_shed_with_429() {
    // A ceiling smaller than the request's declared size: admission cannot make
    // room, so the request is shed with 429 + retry guidance (NFR-R3).
    let limits = IngressLimits {
        max_body_bytes: 1024,
        inflight_ceiling: 8,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve_with_limits(listener, Arc::new(EchoHandler), limits).await;
    });

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{addr}/orders/_bulk"))
        // 20 bytes > the 8-byte in-flight ceiling.
        .body(Full::new(Bytes::from_static(b"01234567890123456789")))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 429);
    assert_eq!(resp.headers().get("retry-after").unwrap(), "1");
}

#[tokio::test]
async fn request_round_trips_over_http2() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(listener, Arc::new(EchoHandler)).await;
    });

    // A prior-knowledge h2c client: no h1, no upgrade — the request must travel
    // over HTTP/2, which the auto ingress builder serves on the same listener.
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build_http();
    let req = Request::builder()
        .method(Method::PUT)
        .uri(format!("http://{addr}/orders/_doc/acme:1"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(br#"{"tenant_id":"acme"}"#)))
        .unwrap();

    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 201);
    assert_eq!(
        resp.version(),
        hyper::Version::HTTP_2,
        "must be served over h2"
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains(r#""index":"orders""#), "{text}");
    assert!(text.contains(r#""ingest":true"#), "{text}");
    // The engine sees the true ingress protocol, not an assumed h1.
    assert!(text.contains(r#""protocol":"Http2""#), "{text}");
}

#[tokio::test]
async fn unsupported_method_gets_405() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve(listener, Arc::new(EchoHandler)).await;
    });

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri(format!("http://{addr}/orders/_doc"))
        .body(Full::new(Bytes::new()))
        .unwrap();

    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 405);
}
