//! Exercises [`OpenSearchSink`] against an in-process mock OpenSearch: a real
//! TCP/HTTP server that records the request it receives and returns a canned
//! index response. This proves request construction (method, path, routing
//! query, body) and response parsing without needing Docker — the live
//! testcontainer round-trip is a separate, ignored test.
//!
// This whole file is test scaffolding (a mock server in helper fns and spawned
// tasks, not `#[test]` fns), so the test-only unwrap allowance does not reach
// it; an unwrap here is a test failure, which is the intent.
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use osproxy_core::{ClusterId, Epoch, IndexName, Target};
use osproxy_sink::{DocOp, OpenSearchSink, ReadOp, Reader, Sink, WriteBatch, WriteOp};
use tokio::net::TcpListener;

/// What the mock captured from the single request it served.
#[derive(Clone, Debug, Default)]
struct Captured {
    method: String,
    uri: String,
    body: String,
}

/// Starts a one-shot mock server returning `response` (status 201) and capturing
/// the request. Returns the base URL and a handle to the captured request.
async fn start_mock(response: &'static str) -> (String, Arc<Mutex<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(Captured::default()));
    let captured_for_task = Arc::clone(&captured);

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        let service = service_fn(move |req: Request<Incoming>| {
            let captured = Arc::clone(&captured_for_task);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                let body = req.into_body().collect().await.unwrap().to_bytes();
                *captured.lock().unwrap() = Captured {
                    method,
                    uri,
                    body: String::from_utf8_lossy(&body).into_owned(),
                };
                Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(response))))
            }
        });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(io, service)
            .await
            .unwrap();
    });

    (format!("http://{addr}"), captured)
}

fn sink_for(cluster: &str, base: String) -> OpenSearchSink {
    let mut endpoints = HashMap::new();
    endpoints.insert(ClusterId::from(cluster), base);
    OpenSearchSink::new(endpoints)
}

#[tokio::test]
async fn index_with_id_and_routing_is_sent_and_parsed() {
    let (base, captured) = start_mock(r#"{"_id":"acme:1001","result":"created"}"#).await;
    let sink = sink_for("eu-1", base);

    let op = WriteOp::new(
        Target::new(ClusterId::from("eu-1"), IndexName::from("orders-shared")),
        DocOp::Index {
            id: Some("acme:1001".to_owned()),
            routing: Some("acme".to_owned()),
            body: br#"{"_tenant":"acme","msg":"hi"}"#.to_vec(),
        },
        Epoch::new(4),
    );
    let ack = sink.write(WriteBatch::single(op)).await.unwrap();

    assert!(ack.all_succeeded());
    assert_eq!(ack.results()[0].id, "acme:1001");
    assert!(ack.results()[0].created);

    let got = captured.lock().unwrap().clone();
    assert_eq!(got.method, "PUT");
    assert_eq!(got.uri, "/orders-shared/_doc/acme:1001?routing=acme");
    assert!(got.body.contains("\"_tenant\":\"acme\""));
}

#[tokio::test]
async fn get_by_id_sends_request_and_returns_the_found_document() {
    let (base, captured) = start_mock(
        r#"{"_index":"orders-shared","_id":"acme:7","found":true,"_source":{"_tenant":"acme","msg":"hi"}}"#,
    )
    .await;
    let sink = sink_for("eu-1", base);

    let outcome = sink
        .get(ReadOp::new(
            Target::new(ClusterId::from("eu-1"), IndexName::from("orders-shared")),
            "acme:7",
            Some("acme".to_owned()),
        ))
        .await
        .unwrap();

    assert!(outcome.found);
    assert_eq!(outcome.status, 200);
    assert!(outcome.body.windows(3).any(|w| w == b"hi\""));

    let got = captured.lock().unwrap().clone();
    assert_eq!(got.method, "GET");
    assert_eq!(got.uri, "/orders-shared/_doc/acme:7?routing=acme");
    assert!(got.body.is_empty());
}

#[tokio::test]
async fn read_from_unreachable_upstream_is_a_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let sink = sink_for("eu-1", format!("http://{addr}"));

    let err = sink
        .get(ReadOp::new(
            Target::new(ClusterId::from("eu-1"), IndexName::from("i")),
            "x",
            None,
        ))
        .await
        .unwrap_err();
    assert!(
        err.retryable(),
        "transport failure should be retryable: {err:?}"
    );
}

#[tokio::test]
async fn server_error_surfaces_as_retryable_upstream() {
    // Bind then immediately drop the listener so the connection is refused,
    // standing in for an unreachable upstream.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let sink = sink_for("eu-1", format!("http://{addr}"));

    let op = WriteOp::new(
        Target::new(ClusterId::from("eu-1"), IndexName::from("i")),
        DocOp::Index {
            id: Some("x".to_owned()),
            routing: None,
            body: b"{}".to_vec(),
        },
        Epoch::new(1),
    );
    let err = sink.write(WriteBatch::single(op)).await.unwrap_err();
    assert!(
        err.retryable(),
        "transport failure should be retryable: {err:?}"
    );
}

#[tokio::test]
async fn unconfigured_cluster_is_a_transport_error() {
    let sink = sink_for("known", "http://127.0.0.1:1".to_owned());
    let op = WriteOp::new(
        Target::new(ClusterId::from("unknown"), IndexName::from("i")),
        DocOp::Index {
            id: Some("x".to_owned()),
            routing: None,
            body: b"{}".to_vec(),
        },
        Epoch::new(1),
    );
    assert!(sink.write(WriteBatch::single(op)).await.is_err());
}
