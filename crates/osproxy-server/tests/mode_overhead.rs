//! Comprehensive **mode** overhead: routing vs body-rewrite cost, per payload, so
//! an operator can choose a mode with numbers (`docs/guide/10-choosing-a-mode`).
//!
//! Measures the proxy's added per-request latency as a differential (proxied −
//! direct, so the generator/loopback/upstream cancel — see `proxy_overhead.rs`),
//! at low concurrency (below the throughput knee, so this is per-request overhead
//! not the queueing tail), across four modes that span the cost spectrum:
//!
//! - **passthrough** — streaming verbatim forward, no tenancy, no body touch;
//! - **ded-cluster** — tenant routing, isolate by cluster, **no body rewrite**;
//! - **ded-index** — tenant routing, isolate by index, **no body rewrite**;
//! - **shared** — tenant routing **plus body rewrite** (inject `_tenant` +
//!   construct a partition-scoped `_id`).
//!
//! The passthrough↔dedicated gap is the cost of *buffering* a routed write (the
//! dedicated modes still buffer + copy once even though they do not rewrite); the
//! dedicated↔shared gap is the cost of the *body rewrite* itself. Bodies are built
//! once up front and reused. `#[ignore]`, host-bound, reported never asserted —
//! run with `--ignored --nocapture`.
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
use osproxy_engine::{PassthroughPolicy, Pipeline};
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::{PlacementMode, ReferenceTenancy};
use osproxy_sink::OpenSearchSink;
use osproxy_tenancy::TenancyRouter;
use tokio::net::TcpListener;

const PAYLOADS: &[(&str, usize)] = &[("256B", 256), ("4KB", 4096), ("64KB", 65536)];
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

/// Builds one mode's pipeline over `upstream`. `None` mode ⇒ a whole-instance
/// passthrough (every request streamed verbatim); `Some(mode)` ⇒ tenancy routing
/// in that placement mode.
fn build_handler(
    upstream: &str,
    mode: Option<PlacementMode>,
) -> Arc<AppHandler<ReferenceAuthenticator>> {
    let cluster = ClusterId::from("default");
    let tenancy =
        ReferenceTenancy::new(cluster.clone(), IndexName::from("osproxy-shared"), upstream)
            .with_placement_mode(mode.unwrap_or_default());
    let mut pipeline = Pipeline::new(TenancyRouter::new(tenancy), OpenSearchSink::new());
    if mode.is_none() {
        pipeline = pipeline.with_passthrough(PassthroughPolicy::new(cluster, upstream));
    }
    Arc::new(
        AppHandler::new(pipeline, ReferenceAuthenticator::dev())
            .with_require_tls_for_mutation(false),
    )
}

async fn serve(handler: Arc<AppHandler<ReferenceAuthenticator>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });
    addr
}

/// The labelled modes, in increasing-cost order.
fn modes() -> [(&'static str, Option<PlacementMode>); 4] {
    [
        ("passthrough", None),
        ("ded-cluster", Some(PlacementMode::DedicatedCluster)),
        ("ded-index", Some(PlacementMode::DedicatedIndex)),
        ("shared", Some(PlacementMode::SharedIndex)),
    ]
}

/// Spawns the mock upstream and one proxy per mode on a dedicated runtime, so the
/// generator never shares their threads. Returns the upstream addr (the direct
/// baseline) and one addr per mode.
fn spawn_server_side() -> (SocketAddr, Vec<(&'static str, SocketAddr)>) {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(6)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let upstream = start_upstream().await;
            let upstream_addr: SocketAddr = upstream.trim_start_matches("http://").parse().unwrap();
            let mut proxies = Vec::new();
            for (label, mode) in modes() {
                proxies.push((label, serve(build_handler(&upstream, mode)).await));
            }
            tx.send((upstream_addr, proxies)).unwrap();
            std::future::pending::<()>().await;
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

/// Drives `conns` connections against `target` at `path`, reusing `body`. Returns
/// (p50 ms, p99 ms).
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
            let uri: hyper::Uri = format!("http://{target}{path}").parse().unwrap();
            let send = |b: Bytes| {
                let (c, uri) = (client.clone(), uri.clone());
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
#[ignore = "comprehensive mode overhead; run with --ignored --nocapture"]
async fn mode_overhead_routing_vs_rewrite() {
    let (upstream, proxies) = spawn_server_side();
    let bodies: Vec<(&str, Bytes)> = PAYLOADS.iter().map(|&(l, s)| (l, payload(s))).collect();

    println!("COMPREHENSIVE MODE OVERHEAD — proxy ADDED latency (proxied − direct), ms p50/p99");
    println!("passthrough=stream/no-rewrite · ded-*=route/no-rewrite · shared=route+body-rewrite");
    print!("{:<6} {:>5} | {:>13}", "size", "conns", "direct");
    for (label, _) in &proxies {
        print!(" | {label:>13}");
    }
    println!();
    for (label, body) in &bodies {
        for &conns in CONNS {
            let (dp50, dp99) = run_cell(upstream, "/", body.clone(), conns).await;
            print!("{label:<6} {conns:>5} | {dp50:>6.3} {dp99:>6.3}");
            for (_, addr) in &proxies {
                let (pp50, pp99) = run_cell(*addr, "/orders/_doc", body.clone(), conns).await;
                print!(" | {:>6.3} {:>6.3}", pp50 - dp50, pp99 - dp99);
            }
            println!();
        }
    }
}
