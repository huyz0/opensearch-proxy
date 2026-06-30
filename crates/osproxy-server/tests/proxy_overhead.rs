//! Proxy **overhead** by payload size — measured as a differential so the test
//! setup cancels out.
//!
//! The earlier `load_matrix` reports *absolute* end-to-end latency, which folds in
//! the co-located generator and mock upstream; that measures the harness as much
//! as the proxy (`docs/guide/11-performance`). This one isolates the proxy:
//!
//! - bodies are built **once** up front and reused (a `Bytes` clone is a refcount
//!   bump, never a re-allocation), so body construction is out of the timed loop,
//! - each cell is measured twice — **direct** client→upstream and **proxied**
//!   client→proxy→upstream — and the reported number is the *difference*. The
//!   generator, the loopback hops, and the upstream are present in both, so they
//!   subtract out; what remains is the proxy's added per-request cost,
//! - it runs at **low concurrency** (1 and 8), below the throughput knee, so we
//!   measure per-request overhead, not the queueing tail that dominates at 256,
//! - the mock upstream and the proxy live on a **dedicated** runtime, so the
//!   generator never steals their threads.
//!
//! The added cost should be ~flat across payload sizes if the proxy is not copying
//! the body, and should grow with size if it is — so the slope is the body-copy
//! overhead the gather/vectored-body change would remove. `#[ignore]`, host-bound,
//! reported never asserted — run with `--ignored --nocapture`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
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

/// (label, size in bytes), built once in `main` and reused for every cell.
const PAYLOADS: &[(&str, usize)] = &[("256B", 256), ("4KB", 4096), ("64KB", 65536)];
/// Low concurrency: we want per-request overhead, not the saturation tail.
const CONNS: &[usize] = &[1, 8];
const REQS_PER_CONN: usize = 100;

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

/// A dedicated runtime (own OS thread + workers) hosting the mock upstream and the
/// proxy in front of it. Returns `(upstream_addr, proxy_addr)` so the generator can
/// hit either the upstream **directly** (baseline) or **through** the proxy.
fn spawn_server_side() -> (SocketAddr, SocketAddr) {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let upstream = start_upstream().await;
            let upstream_addr: SocketAddr = upstream.trim_start_matches("http://").parse().unwrap();
            let tenancy = ReferenceTenancy::new(
                ClusterId::from("default"),
                IndexName::from("osproxy-shared"),
                upstream,
            );
            let pipeline = Pipeline::new(TenancyRouter::new(tenancy), OpenSearchSink::new());
            let handler = Arc::new(
                AppHandler::new(pipeline, ReferenceAuthenticator::dev())
                    .with_require_tls_for_mutation(false),
            );
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy_addr = listener.local_addr().unwrap();
            tx.send((upstream_addr, proxy_addr)).unwrap();
            let _ = osproxy_transport::serve(listener, handler).await;
        });
    });
    rx.recv().unwrap()
}

fn payload(size: usize) -> Bytes {
    let fixed = r#"{"tenant_id":"acme","id":1,"data":""}"#.len();
    let pad = size.saturating_sub(fixed).max(1);
    Bytes::from(format!(
        r#"{{"tenant_id":"acme","id":1,"data":"{}"}}"#,
        "x".repeat(pad)
    ))
}

/// Drives `conns` connections from the current (generator's) runtime against
/// `target` at `path`, reusing the pre-built `body`. Returns (p50 ms, p99 ms).
async fn run_cell(target: SocketAddr, path: &str, body: Bytes, conns: usize) -> (f64, f64) {
    let lat = Arc::new(Mutex::new(Vec::<u64>::new()));
    let done = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::with_capacity(conns);
    for _ in 0..conns {
        let (lat, done, body, path) = (
            Arc::clone(&lat),
            Arc::clone(&done),
            body.clone(),
            path.to_owned(),
        );
        workers.push(tokio::spawn(async move {
            let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
            let send = |b: Bytes| {
                let (c, uri) = (client.clone(), format!("http://{target}{path}"));
                async move {
                    let req = Request::builder()
                        .method(Method::POST)
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(Full::new(b))
                        .unwrap();
                    if let Ok(resp) = c.request(req).await {
                        let ok = resp.status().is_success();
                        let _ = resp.into_body().collect().await;
                        ok
                    } else {
                        false
                    }
                }
            };
            let _ = send(body.clone()).await; // warm-up, untimed
            let mut local = Vec::with_capacity(REQS_PER_CONN);
            for _ in 0..REQS_PER_CONN {
                let t0 = SystemClock.now();
                if send(body.clone()).await {
                    done.fetch_add(1, Ordering::Relaxed);
                    local.push(
                        u64::try_from(SystemClock.now().saturating_duration_since(t0).as_nanos())
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
    let s = LatencySummary::from_nanos(&lat.lock().unwrap()).expect("samples");
    (s.p50_ns as f64 / 1.0e6, s.p99_ns as f64 / 1.0e6)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "proxy-overhead differential; run with --ignored --nocapture"]
async fn proxy_added_latency_by_payload() {
    let (upstream, proxy) = spawn_server_side();
    // Build every body once, up front; the timed loops only clone (refcount bump).
    let bodies: Vec<(&str, Bytes)> = PAYLOADS.iter().map(|&(l, s)| (l, payload(s))).collect();

    println!("proxy ADDED latency = proxied − direct (ms); per-request overhead, harness cancels");
    println!(
        "{:<6} {:>5} | {:>14} | {:>14} | {:>14}",
        "size", "conns", "direct p50/p99", "proxied p50/p99", "ADDED p50/p99"
    );
    for (label, body) in &bodies {
        for &conns in CONNS {
            let (dp50, dp99) = run_cell(upstream, "/", body.clone(), conns).await;
            let (pp50, pp99) = run_cell(proxy, "/orders/_doc", body.clone(), conns).await;
            println!(
                "{label:<6} {conns:>5} | {dp50:>6.3} {dp99:>6.3} | {pp50:>6.3} {pp99:>6.3} | {:>6.3} {:>6.3}",
                pp50 - dp50,
                pp99 - dp99
            );
        }
    }
}
