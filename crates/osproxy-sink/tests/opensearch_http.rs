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
    version: String,
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
                let version = format!("{:?}", req.version());
                let body = req.into_body().collect().await.unwrap().to_bytes();
                *captured.lock().unwrap() = Captured {
                    method,
                    uri,
                    body: String::from_utf8_lossy(&body).into_owned(),
                    version,
                };
                Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(response))))
            }
        });
        // The protocol-auto builder serves whichever protocol the sink's client
        // speaks (h1 by default; h2 prior-knowledge when the op selects it).
        let _ = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(io, service)
            .await;
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
async fn an_http2_op_is_dispatched_over_http2() {
    let (base, captured) = start_mock(r#"{"_id":"acme:1","result":"created"}"#).await;
    let sink = sink_for("eu-1", base);

    // The op's resolved upstream protocol is HTTP/2 — the sink must dispatch it
    // over its h2 client, not the default h1 one (per-request selection).
    let op = WriteOp::new(
        Target::new(ClusterId::from("eu-1"), IndexName::from("orders")),
        DocOp::Index {
            id: Some("acme:1".to_owned()),
            routing: None,
            body: b"{}".to_vec(),
        },
        Epoch::new(1),
    )
    .with_protocol(osproxy_spi::Protocol::Http2);
    let ack = sink.write(WriteBatch::single(op)).await.unwrap();
    assert!(ack.all_succeeded());

    let got = captured.lock().unwrap().clone();
    assert_eq!(got.version, "HTTP/2.0", "must travel over h2: {got:?}");
    assert_eq!(got.method, "PUT");
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
async fn each_cluster_routes_to_its_own_sharded_pool() {
    // Two clusters, two upstreams: each op must reach the endpoint of its own
    // cluster's pool (sharded per cluster, docs/01 §7).
    let (base_a, cap_a) = start_mock(r#"{"_id":"a:1","result":"created"}"#).await;
    let (base_b, cap_b) = start_mock(r#"{"_id":"b:1","result":"created"}"#).await;
    let mut endpoints = HashMap::new();
    endpoints.insert(ClusterId::from("eu-1"), base_a);
    endpoints.insert(ClusterId::from("us-1"), base_b);
    let sink = OpenSearchSink::new(endpoints);

    let op = |cluster: &str| {
        WriteOp::new(
            Target::new(ClusterId::from(cluster), IndexName::from("orders")),
            DocOp::Index {
                id: Some("1".to_owned()),
                routing: None,
                body: b"{}".to_vec(),
            },
            Epoch::new(1),
        )
    };
    sink.write(WriteBatch::single(op("eu-1"))).await.unwrap();
    sink.write(WriteBatch::single(op("us-1"))).await.unwrap();

    // Each mock saw exactly its cluster's request — no cross-routing.
    assert_eq!(cap_a.lock().unwrap().method, "PUT");
    assert_eq!(cap_b.lock().unwrap().method, "PUT");
    assert!(cap_a.lock().unwrap().uri.contains("/orders/_doc/1"));
    assert!(cap_b.lock().unwrap().uri.contains("/orders/_doc/1"));
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
