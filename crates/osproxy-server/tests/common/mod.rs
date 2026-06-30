//! Shared harness for the no-Docker performance tests (`load`/`overhead`/
//! `mode`/`isolation`/`profile`). One mock upstream, one body generator, one
//! differential driver, and the per-mode proxy builder — so each test file holds
//! only its distinct orchestration, not a copy of the plumbing.
//!
//! Each integration-test binary compiles this module and uses a subset of it, so
//! `dead_code` is expected and allowed here.
#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss
)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

/// The reference proxy handler type these harnesses serve.
pub(crate) type Handler = Arc<AppHandler<ReferenceAuthenticator>>;

/// A `{"tenant_id","id","data":"x…"}` document padded to ~`size` bytes; fixed
/// top-level shape, variable size. Build once and reuse (a `Bytes` clone is a
/// refcount bump, never a re-allocation).
#[must_use]
pub(crate) fn payload(size: usize) -> Bytes {
    let fixed = r#"{"tenant_id":"acme","id":1,"data":""}"#.len();
    let pad = size.saturating_sub(fixed).max(1);
    Bytes::from(format!(
        r#"{{"tenant_id":"acme","id":1,"data":"{}"}}"#,
        "x".repeat(pad)
    ))
}

/// A mock OpenSearch on the current runtime that accepts arbitrarily many
/// keep-alive connections and answers every request with a fixed `created`.
/// Returns its base URL.
pub(crate) async fn start_upstream() -> String {
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

/// Builds one proxy handler over `upstream`. `None` ⇒ a whole-instance passthrough
/// (every request streamed verbatim, no tenancy); `Some(mode)` ⇒ tenancy routing
/// in that placement mode (`SharedIndex` rewrites the body; the dedicated modes
/// route without touching it).
#[must_use]
pub(crate) fn build_handler(upstream: &str, mode: Option<PlacementMode>) -> Handler {
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

/// Serves `handler` on a fresh ephemeral port on the current runtime; returns the
/// bound address.
pub(crate) async fn serve(handler: Handler) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });
    addr
}

/// One connection's worth of load: an untimed warm-up then `reqs` timed POSTs of
/// `body` (reused, not re-allocated) to `target` at `path`. Returns the per-request
/// latencies in nanoseconds and tallies successes into `done`.
async fn one_connection(
    target: SocketAddr,
    path: String,
    body: Bytes,
    reqs: usize,
    done: Arc<AtomicU64>,
) -> Vec<u64> {
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    // Pre-build the URI once (a `hyper::Uri` clone is a refcount bump on its
    // Bytes-backed parts), so the timed loop allocates no request string.
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
    let mut local = Vec::with_capacity(reqs);
    for _ in 0..reqs {
        let r0 = SystemClock.now();
        if send(body.clone()).await {
            done.fetch_add(1, Ordering::Relaxed);
            local.push(
                u64::try_from(SystemClock.now().saturating_duration_since(r0).as_nanos())
                    .unwrap_or(u64::MAX),
            );
        }
    }
    local
}

/// Drives `conns` connections concurrently against `target` at `path`, each running
/// [`one_connection`]. Returns `(rps, p50_ms, p99_ms)` over the timed requests.
pub(crate) async fn run_cell(
    target: SocketAddr,
    path: &str,
    body: Bytes,
    conns: usize,
    reqs: usize,
) -> (f64, f64, f64) {
    let done = Arc::new(AtomicU64::new(0));
    let t0 = SystemClock.now();
    let workers: Vec<_> = (0..conns)
        .map(|_| {
            tokio::spawn(one_connection(
                target,
                path.to_owned(),
                body.clone(),
                reqs,
                Arc::clone(&done),
            ))
        })
        .collect();
    let mut lat = Vec::new();
    for w in workers {
        lat.extend(w.await.unwrap());
    }
    let wall = SystemClock
        .now()
        .saturating_duration_since(t0)
        .as_secs_f64();
    let s = LatencySummary::from_nanos(&lat).expect("samples");
    (
        done.load(Ordering::Relaxed) as f64 / wall,
        s.p50_ns as f64 / 1.0e6,
        s.p99_ns as f64 / 1.0e6,
    )
}
