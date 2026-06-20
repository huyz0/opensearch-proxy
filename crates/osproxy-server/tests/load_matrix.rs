//! Parameterized load matrix (no Docker): end-to-end throughput + latency across
//! **payload size × concurrent connections × write mode** (sync forward vs async
//! fan-out enqueue). This is the "realistic load profile" view — what you actually
//! get at a given payload/concurrency/mode — as opposed to the isolated hot-path
//! micro-numbers in `osproxy-observe`'s contention bench.
//!
//! In-process mock upstream + co-located load generator, so absolute numbers are
//! host-bound and the generator competes with the proxy for cores; reported, never
//! asserted. `#[ignore]` — run on demand or in the CI integration lane. See
//! `docs/guide/11-performance.md`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::future::Future;
use std::pin::Pin;
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
use osproxy_engine::{Pipeline, QueueError, QueuedWrite, WriteMode, WriteQueue};
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_tenancy::TenancyRouter;
use tokio::net::TcpListener;

/// (label, target body size in bytes).
const PAYLOADS: &[(&str, usize)] = &[("256B", 256), ("4KB", 4096), ("64KB", 65536)];
/// Concurrent connection counts (each its own client → a distinct connection).
const CONNS: &[usize] = &[16, 64, 256];
/// Requests each connection issues in the timed phase (after a warm-up request).
const REQS_PER_CONN: usize = 60;

/// An in-memory write queue that accepts every op (the downstream apply is out of
/// scope here): it isolates the proxy's async fan-out path — resolve + rewrite +
/// enqueue + `202` — from any broker, the mirror of the sync path's upstream call.
struct MemQueue;
impl WriteQueue for MemQueue {
    fn enabled(&self) -> bool {
        true
    }
    fn enqueue<'a>(
        &'a self,
        _write: QueuedWrite,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// A mock OpenSearch accepting arbitrarily many keep-alive connections, each
/// answering every request with a fixed `created` response.
async fn start_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(|_req: Request<Incoming>| async move {
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(201)
                            .body(Full::new(Bytes::from(
                                r#"{"_id":"acme:1","result":"created"}"#,
                            )))
                            .unwrap(),
                    )
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

/// Spawns the proxy against `upstream`; `async_mode` wires the in-memory fan-out
/// queue and makes async the baseline write mode. Returns its address.
async fn spawn_proxy(upstream: String, async_mode: bool) -> std::net::SocketAddr {
    let tenancy = ReferenceTenancy::new(
        ClusterId::from("default"),
        IndexName::from("osproxy-shared"),
        upstream,
    );
    let mut pipeline = Pipeline::new(TenancyRouter::new(tenancy), OpenSearchSink::new());
    if async_mode {
        pipeline = pipeline
            .with_baseline_write_mode(WriteMode::Async)
            .with_write_queue(Arc::new(MemQueue));
    }
    let handler = Arc::new(
        AppHandler::new(pipeline, ReferenceAuthenticator::dev())
            .with_require_tls_for_mutation(false),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });
    addr
}

/// A `tenant_id`+`id` document padded to ~`size` bytes with a `data` field.
fn payload(size: usize) -> Bytes {
    let fixed = r#"{"tenant_id":"acme","id":1,"data":""}"#.len();
    let pad = size.saturating_sub(fixed).max(1);
    Bytes::from(format!(
        r#"{{"tenant_id":"acme","id":1,"data":"{}"}}"#,
        "x".repeat(pad)
    ))
}

/// Drives `conns` concurrent connections, each sending one warm-up then
/// `REQS_PER_CONN` timed requests of `body`. Returns (rps, p50 ms, p99 ms) over
/// the timed requests; the cold first request per connection is excluded from the
/// latency percentiles but the steady-state rps is measured over the timed phase.
async fn run_cell(proxy: std::net::SocketAddr, body: Bytes, conns: usize) -> (f64, f64, f64) {
    let ok = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(Mutex::new(Vec::<u64>::new()));
    let clock = SystemClock;
    let t0 = clock.now();
    let mut workers = Vec::with_capacity(conns);
    for _ in 0..conns {
        let (ok, lat, body) = (Arc::clone(&ok), Arc::clone(&lat), body.clone());
        workers.push(tokio::spawn(async move {
            let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
            let send = |c: &Client<_, Full<Bytes>>, b: Bytes| {
                let c = c.clone();
                async move {
                    let req = Request::builder()
                        .method(Method::POST)
                        .uri(format!("http://{proxy}/orders/_doc"))
                        .header("content-type", "application/json")
                        .body(Full::new(b))
                        .unwrap();
                    match c.request(req).await {
                        Ok(resp) => {
                            let ok = resp.status().is_success();
                            let _ = resp.into_body().collect().await;
                            ok
                        }
                        Err(_) => false,
                    }
                }
            };
            let _ = send(&client, body.clone()).await; // warm-up (cold connect), untimed
            let mut local = Vec::with_capacity(REQS_PER_CONN);
            for _ in 0..REQS_PER_CONN {
                let r0 = SystemClock.now();
                if send(&client, body.clone()).await {
                    ok.fetch_add(1, Ordering::Relaxed);
                    local.push(
                        u64::try_from(SystemClock.now().saturating_duration_since(r0).as_nanos())
                            .unwrap_or(u64::MAX),
                    );
                }
            }
            lat.lock().unwrap().extend(local);
        }));
    }
    for w in workers {
        w.await.unwrap();
    }
    let wall = clock.now().saturating_duration_since(t0).as_secs_f64();
    let done = ok.load(Ordering::Relaxed);
    let s = LatencySummary::from_nanos(&lat.lock().unwrap()).expect("samples");
    (
        done as f64 / wall,
        s.p50_ns as f64 / 1.0e6,
        s.p99_ns as f64 / 1.0e6,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "load matrix; run with --ignored --nocapture"]
async fn load_matrix_payload_x_connections_x_mode() {
    let upstream = start_upstream().await;
    let sync = spawn_proxy(upstream.clone(), false).await;
    let r#async = spawn_proxy(upstream, true).await;

    println!("load matrix — rps / p50ms / p99ms (co-located harness, host-bound):");
    println!(
        "{:<6} {:>6} | {:>24} | {:>24}",
        "payload", "conns", "sync (forward upstream)", "async (fan-out enqueue)"
    );
    for &(label, size) in PAYLOADS {
        let body = payload(size);
        for &conns in CONNS {
            let (srps, sp50, sp99) = run_cell(sync, body.clone(), conns).await;
            let (arps, ap50, ap99) = run_cell(r#async, body.clone(), conns).await;
            println!(
                "{label:<6} {conns:>6} | {srps:>10.0} {sp50:>6.2} {sp99:>6.2} | {arps:>10.0} {ap50:>6.2} {ap99:>6.2}"
            );
        }
    }
}
