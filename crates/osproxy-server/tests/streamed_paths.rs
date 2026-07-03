//! The streamed fast paths (ADR-014): `_bulk` demuxed without buffering the
//! whole batch, and `_search` with a streamed response. Full-stack, a real
//! client through the real ingress into a mock upstream (no Docker), the
//! counterpart to `end_to_end.rs`'s buffered `_doc` round trip.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use osproxy_core::{ClusterId, IndexName};
use osproxy_engine::Pipeline;
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_tenancy::TenancyRouter;
use tokio::net::TcpListener;

/// A mock upstream returning a fixed body for every request, capturing the
/// last request's method/URI for assertions.
async fn start_upstream(response_body: &'static str) -> (String, Arc<Mutex<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let last_uri = Arc::new(Mutex::new(String::new()));
    let cap = Arc::clone(&last_uri);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let io = TokioIo::new(stream);
            let cap = Arc::clone(&cap);
            let svc = service_fn(move |req: Request<Incoming>| {
                let cap = Arc::clone(&cap);
                async move {
                    *cap.lock().unwrap() = req.uri().to_string();
                    let _ = req.into_body().collect().await;
                    let resp = Response::builder()
                        .status(200)
                        .body(Full::new(Bytes::from(response_body)))
                        .unwrap();
                    Ok::<_, std::convert::Infallible>(resp)
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await;
        }
    });
    (format!("http://{addr}"), last_uri)
}

async fn spawn_proxy(upstream: String) -> std::net::SocketAddr {
    let cluster = ClusterId::from("default");
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from("osproxy-shared"), upstream);
    let handler = Arc::new(
        AppHandler::new(
            Pipeline::new(TenancyRouter::new(tenancy), sink),
            ReferenceAuthenticator::dev(),
        )
        .with_require_tls_for_mutation(false),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });
    proxy_addr
}

#[tokio::test]
async fn a_sync_bulk_request_is_stream_demuxed_and_reaches_the_upstream() {
    let (upstream, last_uri) = start_upstream(
        r#"{"took":1,"errors":false,"items":[{"index":{"_id":"acme:7","status":201}}]}"#,
    )
    .await;
    let proxy_addr = spawn_proxy(upstream).await;

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let ndjson = concat!(
        "{\"index\":{\"_index\":\"orders\"}}\n",
        "{\"tenant_id\":\"acme\",\"id\":7,\"msg\":\"hi\"}\n",
    );
    let req = Request::builder()
        .method("POST")
        .uri(format!("http://{proxy_addr}/_bulk"))
        .header("content-type", "application/x-ndjson")
        .body(Full::new(Bytes::from_static(ndjson.as_bytes())))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["items"].is_array(), "{v}");

    // The demux forwarded the transformed op to the physical shared index.
    assert!(
        last_uri.lock().unwrap().contains("osproxy-shared"),
        "{}",
        *last_uri.lock().unwrap()
    );
}

#[tokio::test]
async fn a_plain_search_streams_its_response_back() {
    let (upstream, last_uri) = start_upstream(r#"{"hits":{"hits":[]}}"#).await;
    let proxy_addr = spawn_proxy(upstream).await;

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method("GET")
        .uri(format!("http://{proxy_addr}/orders/_search"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"query":{"match_all":{}}}"#,
        )))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["hits"]["hits"].is_array(), "{v}");
    assert!(last_uri.lock().unwrap().contains("_search"));
}

#[tokio::test]
async fn a_scroll_opening_search_keeps_the_buffered_path() {
    // `scroll=1m` returns a `_scroll_id` that must be affinity-wrapped against
    // the whole response body, so it must NOT take the streamed path; it still
    // succeeds, just via `handle` rather than `handle_search_stream`.
    let (upstream, _last_uri) = start_upstream(r#"{"_scroll_id":"abc","hits":{"hits":[]}}"#).await;
    let proxy_addr = spawn_proxy(upstream).await;

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method("GET")
        .uri(format!("http://{proxy_addr}/orders/_search?scroll=1m"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"query":{"match_all":{}}}"#,
        )))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 200);
}
