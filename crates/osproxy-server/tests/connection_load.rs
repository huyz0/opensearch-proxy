//! No-Docker connection-load test.
//!
//! Drives **many concurrent downstream client connections** through the real proxy
//! (ingress → pipeline → reference tenancy → `OpenSearchSink`) against an
//! in-process mock upstream. It proves two connection-handling properties without
//! a real OpenSearch or Docker:
//!
//! - **Downstream**: the accept loop + per-connection task model sustains hundreds
//!   of simultaneous client connections and serves every request — none dropped,
//!   no error, under independent (un-pooled) connections.
//! - **Upstream**: the per-cluster pool **reuses** connections under that load —
//!   it opens far fewer upstream sockets than it dispatches requests (NFR-P4/P5),
//!   rather than churning one per downstream connection.
//!
//! This is a deterministic regression guard, not a saturation/ceiling benchmark —
//! the absolute ceiling sweep lives in the Docker-gated `perf_harness`. Counts are
//! held to a CI-safe level (well under the typical 1024 file-descriptor limit).
// Measurement/test-harness code (same posture as `perf_harness`): the latency
// arithmetic casts to f64 and the two driver fns run a little long.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::semicolon_if_nothing_returned
)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use osproxy_bench::LatencySummary;
use osproxy_core::{Clock, ClusterId, IndexName, SystemClock};
use osproxy_engine::Pipeline;
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_tenancy::TenancyRouter;
use tokio::net::TcpListener;

/// Simultaneous downstream connections (each its own client, so they are not
/// multiplexed over one pool — every worker holds a distinct connection).
const CONNECTIONS: u64 = 200;
/// Requests each connection issues in sequence (keep-alive reuse on that conn).
const REQUESTS_PER_CONNECTION: u64 = 8;

/// A mock OpenSearch that accepts **arbitrarily many** connections, each serving
/// any number of keep-alive requests with a fixed `created` response.
async fn start_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            // One task per accepted connection; it serves every request on that
            // connection (HTTP/1.1 keep-alive) until the peer closes it.
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(|_req: Request<Incoming>| async move {
                    let resp = Response::builder()
                        .status(201)
                        .body(Full::new(Bytes::from(
                            r#"{"_id":"acme:1","result":"created"}"#,
                        )))
                        .unwrap();
                    Ok::<_, std::convert::Infallible>(resp)
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

/// Spawns the proxy (the exact wiring the binary builds) against `upstream`,
/// returning its address.
async fn spawn_proxy(upstream: String) -> std::net::SocketAddr {
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(
        ClusterId::from("default"),
        IndexName::from("osproxy-shared"),
        upstream,
    );
    let handler = Arc::new(
        AppHandler::new(
            Pipeline::new(TenancyRouter::new(tenancy), sink),
            ReferenceAuthenticator::dev(),
        )
        .with_require_tls_for_mutation(false), // cleartext loopback harness
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_proxy_serves_many_concurrent_downstream_connections() {
    let upstream = start_upstream().await;
    let proxy = spawn_proxy(upstream).await;

    let ok = Arc::new(AtomicU64::new(0));
    // Cold = the first request on a connection (pays TCP setup + the connect-storm
    // queueing of CONNECTIONS arriving at once). Warm = the rest, on an established
    // keep-alive connection — the steady-state per-request cost under concurrency.
    let cold = Arc::new(Mutex::new(Vec::<u64>::new()));
    let warm = Arc::new(Mutex::new(Vec::<u64>::new()));
    let wall_start = SystemClock.now();
    let mut workers = Vec::new();
    for _ in 0..CONNECTIONS {
        let ok = Arc::clone(&ok);
        let cold = Arc::clone(&cold);
        let warm = Arc::clone(&warm);
        workers.push(tokio::spawn(async move {
            // Each worker gets its OWN client (independent pool), so CONNECTIONS
            // distinct downstream connections are live at once rather than all
            // sharing one pool.
            let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
            let mut local_cold: Vec<u64> = Vec::new();
            let mut local_warm: Vec<u64> = Vec::new();
            for n in 0..REQUESTS_PER_CONNECTION {
                let req = Request::builder()
                    .method(Method::POST)
                    .uri(format!("http://{proxy}/orders/_doc"))
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from_static(
                        br#"{"tenant_id":"acme","id":1,"msg":"hi"}"#,
                    )))
                    .unwrap();
                let t0 = SystemClock.now();
                if let Ok(resp) = client.request(req).await {
                    let ok_status = resp.status() == 201;
                    let _ = resp.into_body().collect().await;
                    if ok_status {
                        ok.fetch_add(1, Ordering::Relaxed);
                        let dt = u64::try_from(
                            SystemClock.now().saturating_duration_since(t0).as_nanos(),
                        )
                        .unwrap_or(u64::MAX);
                        if n == 0 {
                            local_cold.push(dt)
                        } else {
                            local_warm.push(dt)
                        }
                    }
                }
            }
            cold.lock().unwrap().extend(local_cold);
            warm.lock().unwrap().extend(local_warm);
        }));
    }
    for w in workers {
        w.await.unwrap();
    }
    let wall = SystemClock.now().saturating_duration_since(wall_start);

    let total = CONNECTIONS * REQUESTS_PER_CONNECTION;
    assert_eq!(
        ok.load(Ordering::Relaxed),
        total,
        "every request across {CONNECTIONS} concurrent connections must succeed"
    );

    // Report latency under the concurrent load. NOTE these are *not* the proxy's
    // isolated overhead: the load generator, the proxy, and the mock upstream all
    // run in this one process, so the numbers are inflated by co-located CPU/scheduler
    // contention (and the cold bucket additionally by the connect storm). For the
    // proxy's true added latency (measured proxy-vs-direct against a real upstream)
    // see the Docker `perf_harness`. Cold vs warm are split so the connect-storm tail
    // doesn't masquerade as per-request cost. Host-bound, so reported — never asserted.
    let cold = LatencySummary::from_nanos(&cold.lock().unwrap()).expect("a cold sample");
    let warm = LatencySummary::from_nanos(&warm.lock().unwrap()).expect("a warm sample");
    let rps = (total as f64) / wall.as_secs_f64();
    println!(
        "connection-load: {CONNECTIONS} conns x {REQUESTS_PER_CONNECTION} = {total} reqs in {:.3}s ({rps:.0} rps)\n  \
         cold (1st req/conn, incl. connect storm): p50={:.3}ms p99={:.3}ms max={:.3}ms\n  \
         warm (keep-alive, steady state):          p50={:.3}ms p99={:.3}ms max={:.3}ms",
        wall.as_secs_f64(),
        ms(cold.p50_ns), ms(cold.p99_ns), ms(cold.max_ns),
        ms(warm.p50_ns), ms(warm.p99_ns), ms(warm.max_ns),
    );

    assert_upstream_pooled(proxy, total).await;
}

/// Nanoseconds to milliseconds as an `f64`, for the human-readable report line.
fn ms(ns: u64) -> f64 {
    ns as f64 / 1.0e6
}

/// Microbenchmark: per-request round-trip latency on a **single warm keep-alive
/// connection**, sequential. This isolates the proxy's per-request path from the
/// connect storm and from concurrency contention, so it cleanly surfaces socket-
/// level stalls (e.g. Nagle's algorithm). Reported, never asserted (host-bound).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "microbenchmark; run with --ignored --nocapture"]
async fn single_connection_request_latency_microbench() {
    let upstream = start_upstream().await;
    let proxy = spawn_proxy(upstream).await;
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();

    let one = || {
        let client = client.clone();
        async move {
            let req = Request::builder()
                .method(Method::POST)
                .uri(format!("http://{proxy}/orders/_doc"))
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from_static(
                    br#"{"tenant_id":"acme","id":1,"msg":"hi"}"#,
                )))
                .unwrap();
            let resp = client.request(req).await.unwrap();
            assert_eq!(resp.status(), 201);
            let _ = resp.into_body().collect().await;
        }
    };

    for _ in 0..100 {
        one().await; // warm the connection + pool
    }
    let mut samples = Vec::with_capacity(2000);
    for _ in 0..2000 {
        let t0 = SystemClock.now();
        one().await;
        samples.push(
            u64::try_from(SystemClock.now().saturating_duration_since(t0).as_nanos())
                .unwrap_or(u64::MAX),
        );
    }
    let s = LatencySummary::from_nanos(&samples).expect("samples");
    println!(
        "single-conn warm round-trip: p50={:.3}ms p90={:.3}ms p99={:.3}ms mean={:.3}ms max={:.3}ms",
        ms(s.p50_ns),
        ms(s.p90_ns),
        ms(s.p99_ns),
        ms(s.mean_ns),
        ms(s.max_ns),
    );

    // True connection-establishment cost, in isolation: a fresh client (so a fresh
    // downstream connection) each iteration, timed including connect + first
    // request, sequential (no connect storm, no concurrency contention).
    let mut cold = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let fresh: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("http://{proxy}/orders/_doc"))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from_static(
                br#"{"tenant_id":"acme","id":1,"msg":"hi"}"#,
            )))
            .unwrap();
        let t0 = SystemClock.now();
        let resp = fresh.request(req).await.unwrap();
        assert_eq!(resp.status(), 201);
        let _ = resp.into_body().collect().await;
        cold.push(
            u64::try_from(SystemClock.now().saturating_duration_since(t0).as_nanos())
                .unwrap_or(u64::MAX),
        );
    }
    let c = LatencySummary::from_nanos(&cold).expect("samples");
    println!(
        "fresh-conn establish+1st req (sequential): p50={:.3}ms p90={:.3}ms p99={:.3}ms mean={:.3}ms max={:.3}ms",
        ms(c.p50_ns),
        ms(c.p90_ns),
        ms(c.p99_ns),
        ms(c.mean_ns),
        ms(c.max_ns),
    );
}

/// Scrapes `/metrics` and asserts the upstream pool **reused** connections under
/// the load: it dispatched every request but opened far fewer sockets than it
/// dispatched (NFR-P4/P5) — proving the upstream side does not churn a connection
/// per downstream connection.
async fn assert_upstream_pooled(proxy: std::net::SocketAddr, total: u64) {
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let resp = client
        .request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("http://{proxy}/metrics"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let snap: serde_json::Value = serde_json::from_str(&String::from_utf8_lossy(&body)).unwrap();

    assert_eq!(
        snap["requests_total"].as_u64(),
        Some(total),
        "every data-plane request was counted: {snap}"
    );
    assert_eq!(
        snap["requests_error"].as_u64(),
        Some(0),
        "no errors: {snap}"
    );

    let pool = &snap["pools"][0];
    let opened = pool["opened"].as_u64().unwrap();
    let dispatched = pool["dispatched"].as_u64().unwrap();
    assert_eq!(
        dispatched, total,
        "the pool dispatched every request: {snap}"
    );
    assert!(
        opened < dispatched,
        "the upstream pool must reuse connections (opened {opened} < dispatched {dispatched}): {snap}"
    );
}
