//! Proves the OTLP exporter ships a payload to a collector's `/v1/traces` over
//! HTTP, off the caller's thread: a real in-process mock collector captures the
//! POST and we assert its target, content type, and body.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use osproxy_observe::SpanExporter;
use osproxy_otlp::OtlpHttpExporter;
use tokio::net::TcpListener;

/// What the mock collector captured: the request URI, content type, and body.
#[derive(Clone, Debug)]
struct Captured {
    uri: String,
    content_type: String,
    body: String,
}

/// Starts a one-shot mock collector; returns its base URL and the captured POST.
async fn start_collector() -> (String, Arc<Mutex<Option<Captured>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(None));
    let captured_for_task = Arc::clone(&captured);

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        let service = service_fn(move |req: Request<Incoming>| {
            let captured = Arc::clone(&captured_for_task);
            async move {
                let uri = req.uri().to_string();
                let content_type = req
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                let body = req.into_body().collect().await.unwrap().to_bytes();
                *captured.lock().unwrap() = Some(Captured {
                    uri,
                    content_type,
                    body: String::from_utf8_lossy(&body).into_owned(),
                });
                Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from_static(
                    b"{}",
                ))))
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(io, service)
            .await;
    });

    (format!("http://{addr}"), captured)
}

/// Polls the capture slot until the background POST lands (or times out).
async fn await_captured(slot: &Arc<Mutex<Option<Captured>>>) -> Option<Captured> {
    for _ in 0..100 {
        if let Some(c) = slot.lock().unwrap().clone() {
            return Some(c);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

#[tokio::test]
async fn posts_otlp_json_spans_to_v1_traces() {
    let (base, captured) = start_collector().await;
    let exporter = OtlpHttpExporter::new(&base);

    // The collector base URL gains the standard OTLP traces path, and the
    // exporter reports itself enabled (so the pipeline will encode + ship).
    assert!(exporter.endpoint().ends_with("/v1/traces"));
    assert!(exporter.enabled());

    let payload = serde_json::json!({
        "resourceSpans": [{ "scopeSpans": [{ "spans": [{ "traceId": "4bf9" }] }] }]
    });
    // Returns immediately; the POST is delivered in the background.
    exporter.export(payload);

    let got = await_captured(&captured)
        .await
        .expect("collector should receive the exported span");
    assert!(got.uri.ends_with("/v1/traces"), "uri: {}", got.uri);
    assert_eq!(got.content_type, "application/json");
    assert!(got.body.contains("resourceSpans"), "body: {}", got.body);
    assert!(
        got.body.contains("4bf9"),
        "body carries the span: {}",
        got.body
    );
}

#[tokio::test]
async fn a_trailing_slash_on_the_base_url_is_not_doubled() {
    let exporter = OtlpHttpExporter::new("http://collector:4318/");
    assert_eq!(exporter.endpoint(), "http://collector:4318/v1/traces");
}

#[tokio::test]
async fn exporting_to_a_down_collector_neither_blocks_nor_panics() {
    // Port 9 (discard) refuses/blackholes: the central promise is that this never
    // reaches the caller. `export` must return immediately and not panic; the
    // failed POST is absorbed by the background task under its deadline.
    let exporter = OtlpHttpExporter::new("http://127.0.0.1:9");
    let payload = serde_json::json!({ "resourceSpans": [] });
    // Many calls in a row: must not block, panic, or deadlock the caller even
    // though none can succeed.
    for _ in 0..10 {
        exporter.export(payload.clone());
    }
    // Give the background tasks a moment to fail; the test reaching here is the
    // assertion (the caller was never blocked by the dead collector).
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}
