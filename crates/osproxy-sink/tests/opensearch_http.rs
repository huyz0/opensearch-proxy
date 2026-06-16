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
// JUSTIFY(file-length): a cohesive suite of mock-upstream integration tests, each
// a self-contained scenario (request construction, h2 selection, sharded pools,
// breaker eviction, pool reuse) sharing one mock-server harness; splitting it
// would scatter the shared scaffolding without adding clarity.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use osproxy_core::{ClusterId, Epoch, IndexName, RequestId, Target, TraceContext};
use osproxy_sink::{
    CursorOp, DocOp, OpenSearchSink, ReadOp, Reader, SearchOp, Sink, WriteBatch, WriteOp,
};
use osproxy_spi::HttpMethod;
use tokio::net::TcpListener;

/// What the mock captured from the single request it served.
#[derive(Clone, Debug, Default)]
struct Captured {
    method: String,
    uri: String,
    body: String,
    version: String,
    traceparent: Option<String>,
    tracestate: Option<String>,
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
                let header = |name: &str| {
                    req.headers()
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned)
                };
                let traceparent = header("traceparent");
                let tracestate = header("tracestate");
                let body = req.into_body().collect().await.unwrap().to_bytes();
                *captured.lock().unwrap() = Captured {
                    method,
                    uri,
                    body: String::from_utf8_lossy(&body).into_owned(),
                    version,
                    traceparent,
                    tracestate,
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

/// Starts a long-lived mock that accepts *many* connections and serves every
/// request on each, counting how many TCP connections it accepted. Lets a test
/// prove the sink's pool reuses one connection across many requests rather than
/// reconnecting per request.
async fn start_pooled_mock(response: &'static str) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_for_task = Arc::clone(&accepts);

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            accepts_for_task.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let service = service_fn(move |_req: Request<Incoming>| async move {
                    Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(
                        response,
                    ))))
                });
                let _ = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection(TokioIo::new(stream), service)
                .await;
            });
        }
    });

    (format!("http://{addr}"), accepts)
}

fn sink_for(cluster: &str, base: String) -> OpenSearchSink {
    let mut endpoints = HashMap::new();
    endpoints.insert(ClusterId::from(cluster), base);
    OpenSearchSink::new(endpoints)
}

#[tokio::test]
async fn the_trace_context_is_propagated_to_the_upstream() {
    let (base, captured) = start_mock(r#"{"_id":"acme:1","result":"created"}"#).await;
    let sink = sink_for("eu-1", base);

    // A client request arrives carrying an upstream traceparent and tracestate.
    let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let ctx = TraceContext::propagate(
        Some(incoming),
        Some("vendor1=abc,congo=t61rcWkgMzE"),
        &RequestId::from("req-42"),
    );
    let op = WriteOp::new(
        Target::new(ClusterId::from("eu-1"), IndexName::from("orders-shared")),
        DocOp::Index {
            id: Some("acme:1".to_owned()),
            routing: Some("acme".to_owned()),
            body: br#"{"_tenant":"acme"}"#.to_vec(),
        },
        Epoch::new(1),
    )
    .with_trace(Some(ctx));
    sink.write(WriteBatch::single(op)).await.unwrap();

    let got = captured.lock().unwrap().clone();
    let traceparent = got
        .traceparent
        .expect("upstream must receive a traceparent");
    // Same trace id: the upstream span joins the client's distributed trace.
    assert!(
        traceparent.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
        "trace id must be preserved end to end: {traceparent}"
    );
    // New span id: the upstream is a child of the proxy, not of the client.
    assert!(
        !traceparent.contains("00f067aa0ba902b7"),
        "proxy must present its own span id downstream: {traceparent}"
    );
    // tracestate is forwarded verbatim — the proxy adds no entry of its own.
    assert_eq!(
        got.tracestate.as_deref(),
        Some("vendor1=abc,congo=t61rcWkgMzE"),
        "the caller's tracestate must pass through unchanged"
    );
}

#[tokio::test]
async fn cursor_passthrough_forwards_method_path_and_body_to_the_pinned_cluster() {
    // The engine has already recovered the cluster + real id from the envelope;
    // the sink forwards the raw op (method, path, body) verbatim to that cluster.
    let (base, captured) = start_mock(r#"{"_scroll_id":"X","hits":{"hits":[]}}"#).await;
    let sink = sink_for("eu-1", base);

    let op = CursorOp::new(
        ClusterId::from("eu-1"),
        HttpMethod::Post,
        "/_search/scroll",
        br#"{"scroll":"1m","scroll_id":"REALID"}"#.to_vec(),
    );
    let outcome = sink.cursor(op).await.unwrap();

    let got = captured.lock().unwrap().clone();
    assert_eq!(got.method, "POST");
    assert_eq!(got.uri, "/_search/scroll");
    assert!(
        got.body.contains("REALID"),
        "real id forwarded: {}",
        got.body
    );
    assert_eq!(outcome.status, 200, "the upstream status is forwarded");
    assert!(
        outcome.body.starts_with(br#"{"_scroll_id""#),
        "the upstream response is forwarded back verbatim"
    );
}

#[tokio::test]
async fn a_passthrough_path_with_a_traversal_segment_is_refused_without_dispatch() {
    // Defense in depth at the one choke point that concatenates a passthrough
    // path verbatim into the upstream URI: a `..` segment is refused before any
    // request is built, so it can never resolve past an allow-listed prefix.
    let (base, captured) = start_mock(r"{}").await;
    let sink = sink_for("eu-1", base);

    let op = CursorOp::new(
        ClusterId::from("eu-1"),
        HttpMethod::Get,
        "/_cat/../_cluster/settings",
        Vec::new(),
    );
    let err = sink.cursor(op).await.expect_err("a `..` path is refused");
    assert_eq!(err.code(), osproxy_core::ErrorCode::UpstreamFailed);
    assert_eq!(
        captured.lock().unwrap().method,
        "",
        "a refused path never reaches the upstream"
    );
}

#[tokio::test]
async fn a_search_appends_its_allow_listed_query_to_the_upstream_url() {
    // The engine forwards only `scroll`/`keep_alive`; the sink appends it so a
    // scroll-opening search actually opens a scroll upstream.
    let (base, captured) = start_mock(r#"{"_scroll_id":"X","hits":{"hits":[]}}"#).await;
    let sink = sink_for("eu-1", base);

    let op = SearchOp::new(
        Target::new(ClusterId::from("eu-1"), IndexName::from("orders-shared")),
        br#"{"query":{"match_all":{}}}"#.to_vec(),
    )
    .with_query(Some("scroll=1m".to_owned()));
    let _ = sink.search(op).await.unwrap();

    let got = captured.lock().unwrap().clone();
    assert_eq!(got.method, "POST");
    assert_eq!(
        got.uri, "/orders-shared/_search?scroll=1m",
        "the scroll param must reach the upstream"
    );
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
async fn a_failing_cluster_is_evicted_then_retried_after_cooldown() {
    use osproxy_core::ManualClock;
    use osproxy_sink::SinkError;

    // A dead endpoint: every dispatch is a fast connection failure.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let clock = Arc::new(ManualClock::new());
    let mut endpoints = HashMap::new();
    endpoints.insert(ClusterId::from("eu-1"), format!("http://{addr}"));
    let sink = OpenSearchSink::new(endpoints)
        .with_clock(clock.clone())
        .with_breaker(2, std::time::Duration::from_secs(5));

    let write = || async {
        sink.write(WriteBatch::single(WriteOp::new(
            Target::new(ClusterId::from("eu-1"), IndexName::from("i")),
            DocOp::Index {
                id: Some("x".to_owned()),
                routing: None,
                body: b"{}".to_vec(),
            },
            Epoch::new(1),
        )))
        .await
        .unwrap_err()
    };

    // Two real connection failures trip the breaker (threshold 2).
    let kind = |e: SinkError| match e {
        SinkError::Transport { kind } => kind,
        other => unreachable!("expected transport error, got {other:?}"),
    };
    assert!(
        !kind(write().await).contains("circuit"),
        "1st is a real attempt"
    );
    assert!(
        !kind(write().await).contains("circuit"),
        "2nd is a real attempt"
    );

    // The cluster is now shed — the next request fails fast without attempting.
    assert!(
        kind(write().await).contains("circuit"),
        "evicted cluster must be shed"
    );

    // After the cooldown a half-open trial is attempted again (it still fails,
    // since the endpoint is dead — but it is no longer shed outright).
    clock.advance(std::time::Duration::from_secs(6));
    assert!(
        !kind(write().await).contains("circuit"),
        "after cooldown the cluster is retried"
    );
}

#[tokio::test]
async fn a_stuck_upstream_times_out_and_is_retryable() {
    // A server that accepts the connection but never sends a response — the
    // request must not hang forever; the per-request timeout fails it fast
    // (NFR-R7) as a retryable transport error.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        // Hold the connection open without ever replying.
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    });

    let sink = sink_for("eu-1", format!("http://{addr}"))
        .with_timeout(std::time::Duration::from_millis(50));
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
        "an upstream timeout should be retryable: {err:?}"
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

#[tokio::test]
async fn repeated_writes_reuse_one_pooled_connection() {
    // The M4 "pool reuse rates verified" exit gate (docs/11): many sequential
    // writes to one cluster must ride a single pooled connection, not reconnect
    // each time — proven from both ends (server accepts) and the sink's own
    // connection-open counter.
    const WRITES: u64 = 5;
    let (base, accepts) = start_pooled_mock(r#"{"_id":"a:1","result":"created"}"#).await;
    let sink = sink_for("eu-1", base);

    for i in 0..WRITES {
        let op = WriteOp::new(
            Target::new(ClusterId::from("eu-1"), IndexName::from("orders")),
            DocOp::Index {
                id: Some("1".to_owned()),
                routing: None,
                body: b"{}".to_vec(),
            },
            Epoch::new(1),
        );
        let ack = sink.write(WriteBatch::single(op)).await.unwrap();
        // The ack's pool-reuse flag (which feeds the dispatch span) is false for
        // the first, cold write and true once the pool is warm.
        assert_eq!(
            ack.pool_reuse(),
            i > 0,
            "write {i} reuse flag must reflect a warm pool"
        );
    }

    // The server accepted exactly one TCP connection for all the writes.
    assert_eq!(
        accepts.load(Ordering::Relaxed),
        1,
        "all writes must share one pooled connection"
    );

    // The sink's own counters agree: one connection opened, every write but the
    // first rode a reused connection.
    let stats = sink.pool_stats(&ClusterId::from("eu-1")).unwrap();
    assert_eq!(stats.opened, 1, "pool opened exactly one connection");
    assert_eq!(stats.dispatched, WRITES);
    assert_eq!(stats.reused(), WRITES - 1, "pool reuse rate verified");
}
