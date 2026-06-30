//! Ablation: co-located vs isolated proxy, scaling connections at 64 KB.
//!
//! `load_matrix` runs the load generator, the proxy, and the mock upstream on
//! **one** tokio runtime, so as connections rise the generator competes with the
//! proxy for the same worker threads — the measured tail is dominated by that
//! co-location, not by the proxy's own cost (`docs/guide/11-performance`). This
//! test isolates that factor: it runs the same 64 KB connection sweep twice,
//!
//! - **co-located**: proxy spawned onto the test runtime (the generator's runtime),
//! - **isolated**: proxy (and its upstream) on a *dedicated* runtime with its own
//!   worker threads, so the generator and the proxy never share a thread,
//!
//! and prints both p50/p99 side by side. If the isolated tail is far lower, the
//! connection-scaling latency is the harness's core contention, not the proxy.
//! `#[ignore]`, host-bound, reported never asserted — run with `--ignored
//! --nocapture`.
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

const CONNS: &[usize] = &[16, 64, 256];
const REQS_PER_CONN: usize = 60;
const PAYLOAD: usize = 65536;
/// Worker threads for each runtime in the isolated case (so generator and proxy
/// run on disjoint threads). Kept modest so total threads stay under core count.
const PROXY_WORKERS: usize = 6;

/// A mock OpenSearch accepting keep-alive connections, answering `created`.
async fn upstream_on_current_rt() -> String {
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

/// Builds the proxy handler over `upstream` (shared by both spawn modes).
fn build_handler(upstream: String) -> Arc<AppHandler<ReferenceAuthenticator>> {
    let tenancy = ReferenceTenancy::new(
        ClusterId::from("default"),
        IndexName::from("osproxy-shared"),
        upstream,
    );
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy), OpenSearchSink::new());
    Arc::new(
        AppHandler::new(pipeline, ReferenceAuthenticator::dev())
            .with_require_tls_for_mutation(false),
    )
}

/// Co-located: upstream + proxy spawned onto the **current** (generator's) runtime.
async fn spawn_proxy_colocated() -> SocketAddr {
    let upstream = upstream_on_current_rt().await;
    let handler = build_handler(upstream);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });
    addr
}

/// Isolated: a **dedicated** multi-thread runtime on its own OS thread runs both
/// the upstream and the proxy, so they never share a worker thread with the
/// generator. The runtime is leaked (lives for the test) by holding it in the
/// spawned thread, which blocks forever serving.
fn spawn_proxy_isolated() -> SocketAddr {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(PROXY_WORKERS)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let upstream = upstream_on_current_rt().await;
            let handler = build_handler(upstream);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr).unwrap();
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

/// Drives `conns` connections from the **current** runtime against `proxy`.
/// Returns (rps, p50 ms, p99 ms) over the timed requests.
async fn run_cell(proxy: SocketAddr, body: Bytes, conns: usize) -> (f64, f64, f64) {
    let ok = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(Mutex::new(Vec::<u64>::new()));
    let t0 = SystemClock.now();
    let mut workers = Vec::with_capacity(conns);
    for _ in 0..conns {
        let (ok, lat, body) = (Arc::clone(&ok), Arc::clone(&lat), body.clone());
        workers.push(tokio::spawn(async move {
            let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
            let send = |b: Bytes| {
                let c = client.clone();
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
            let _ = send(body.clone()).await; // warm-up, untimed
            let mut local = Vec::with_capacity(REQS_PER_CONN);
            for _ in 0..REQS_PER_CONN {
                let r0 = SystemClock.now();
                if send(body.clone()).await {
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
    let wall = SystemClock
        .now()
        .saturating_duration_since(t0)
        .as_secs_f64();
    let done = ok.load(Ordering::Relaxed);
    let s = LatencySummary::from_nanos(&lat.lock().unwrap()).expect("samples");
    (
        done as f64 / wall,
        s.p50_ns as f64 / 1.0e6,
        s.p99_ns as f64 / 1.0e6,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "isolation ablation; run with --ignored --nocapture"]
async fn co_located_vs_isolated_64kb_connection_sweep() {
    let body = payload(PAYLOAD);
    let colocated = spawn_proxy_colocated().await;
    let isolated = spawn_proxy_isolated();

    println!("64KB connection sweep — co-located vs isolated proxy runtime");
    println!(
        "(generator on the test runtime; isolated proxy+upstream on a dedicated {PROXY_WORKERS}-thread runtime)"
    );
    println!(
        "{:>6} | {:>22} | {:>22}",
        "conns", "co-located rps/p50/p99", "isolated rps/p50/p99"
    );
    for &conns in CONNS {
        let (crps, cp50, cp99) = run_cell(colocated, body.clone(), conns).await;
        let (irps, ip50, ip99) = run_cell(isolated, body.clone(), conns).await;
        println!(
            "{conns:>6} | {crps:>8.0} {cp50:>6.2} {cp99:>6.2} | {irps:>8.0} {ip50:>6.2} {ip99:>6.2}"
        );
    }
}
