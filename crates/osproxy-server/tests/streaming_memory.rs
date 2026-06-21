//! Memory invariant for the streaming paths (ADR-014): a **very large** message
//! flows through the proxy with **bounded** memory — the proxy never buffers the
//! whole body. It spawns the real `osproxy` binary in tenant-agnostic passthrough
//! mode pointed at an in-process mock upstream (no Docker), streams a large
//! request through it, and reads the proxy's own resident set from
//! `/proc/<pid>/statm` (so the figure is the proxy alone, not this harness).
//!
//! `#[ignore]`: needs Linux `/proc` and moves a lot of bytes.
//!   `cargo test -p osproxy-server --test streaming_memory -- --ignored --nocapture`

// Test scaffolding (a spawned child proxy + mock server, not `#[test]` fns).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]

use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

/// Linux page size assumed when converting `statm` resident pages to bytes.
const PAGE_BYTES: u64 = 4096;
/// The streamed request body size: 64 MiB — 8× the proxy's 8 MiB *buffered* cap,
/// so if the passthrough path buffered it the request would be refused (413) and,
/// were the cap lifted, the resident set would jump by ~64 MiB. Streaming both.
const BIG_BODY: usize = 64 * 1024 * 1024;
/// The most the proxy's resident set may grow while streaming the big body. Far
/// below `BIG_BODY`: streaming holds only an in-flight window, so a few MiB; full
/// buffering would be ~64 MiB. Generous margin over streaming, decisive vs buffering.
const MAX_GROWTH_BYTES: u64 = 16 * 1024 * 1024;

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

/// A spawned `osproxy` child, killed when the guard drops.
struct ProxyChild(Child);

impl Drop for ProxyChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A mock upstream that **drains** each request body frame by frame (never
/// buffering it) and returns a small response — so the proxy, not the mock, is
/// what the test measures, and backpressure is realistic.
async fn start_drain_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(|req: Request<Incoming>| async move {
                    let mut body = req.into_body();
                    // Discard each frame as it arrives — bounded memory.
                    while let Some(frame) = body.frame().await {
                        drop(frame);
                    }
                    Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from_static(
                        b"{\"result\":\"ok\"}",
                    ))))
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

/// Spawns the real `osproxy` binary in whole-instance passthrough mode pointed at
/// `upstream`. Returns the child guard, its base URL, and its pid.
async fn spawn_passthrough_proxy(upstream: &str) -> (ProxyChild, String, u32) {
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let bind = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_osproxy"))
        .env("OSPROXY_BIND", &bind)
        // The tenancy upstream/index are required to build the pipeline but never
        // consulted: whole-instance passthrough forwards every request verbatim.
        .env("OSPROXY_UPSTREAM", upstream)
        .env("OSPROXY_INDEX", "osproxy-shared")
        .env("OSPROXY_TOKENS", "") // dev (open) auth
        .env("OSPROXY_ALLOW_CLEARTEXT_MUTATION", "1")
        .env("OSPROXY_PASSTHROUGH_CLUSTER", "mock")
        .env("OSPROXY_PASSTHROUGH_ENDPOINT", upstream)
        .spawn()
        .expect("spawn osproxy binary");
    let pid = child.id();
    (ProxyChild(child), format!("http://{bind}"), pid)
}

/// A passthrough request of `size` bytes to an arbitrary index.
fn passthrough_request(base: &str, size: usize) -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::POST)
        .uri(format!("{base}/raw/_doc"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(vec![b'x'; size])))
        .unwrap()
}

/// The resident set of `pid` in bytes (`/proc/<pid>/statm` field 2), or `None`.
fn rss_bytes(pid: u32) -> Option<u64> {
    let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    Some(statm.split_whitespace().nth(1)?.parse::<u64>().ok()? * PAGE_BYTES)
}

/// Polls the proxy until it answers (any result means it's up), or gives up.
async fn wait_ready(client: &HttpClient, base: &str) -> bool {
    for _ in 0..60 {
        if client.request(passthrough_request(base, 1)).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux /proc; run with --ignored --nocapture"]
async fn a_large_passthrough_request_streams_with_bounded_memory() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let upstream = start_drain_upstream().await;
    let (proxy, base, pid) = spawn_passthrough_proxy(&upstream).await;
    assert!(
        wait_ready(&client, &base).await,
        "proxy did not become ready"
    );

    // Let startup allocations settle, then read the idle resident set.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = rss_bytes(pid).expect("read idle RSS");

    // Sample the proxy's RSS continuously while the big body streams through, so a
    // transient full-body buffer (held for the whole transfer) would be caught.
    let done = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(idle));
    let sampler = {
        let (done, peak) = (Arc::clone(&done), Arc::clone(&peak));
        tokio::spawn(async move {
            while !done.load(Ordering::Relaxed) {
                if let Some(r) = rss_bytes(pid) {
                    peak.fetch_max(r, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Drive several large requests so the sampler reliably overlaps the transfers.
    for _ in 0..4 {
        let resp = client
            .request(passthrough_request(&base, BIG_BODY))
            .await
            .expect("big passthrough request");
        assert!(
            resp.status().is_success(),
            "big passthrough streamed, not 413'd: {}",
            resp.status()
        );
        let _ = resp.into_body().collect().await;
    }

    done.store(true, Ordering::Relaxed);
    sampler.await.unwrap();
    let peak = peak.load(Ordering::Relaxed);
    let growth = peak.saturating_sub(idle);

    println!(
        "idle = {:.1} MiB, peak = {:.1} MiB, growth = {:.1} MiB over a {} MiB body",
        idle as f64 / 1_048_576.0,
        peak as f64 / 1_048_576.0,
        growth as f64 / 1_048_576.0,
        BIG_BODY / 1_048_576,
    );
    assert!(
        growth < MAX_GROWTH_BYTES,
        "passthrough must stream, not buffer: RSS grew {:.1} MiB for a {} MiB body (cap {:.0} MiB)",
        growth as f64 / 1_048_576.0,
        BIG_BODY / 1_048_576,
        MAX_GROWTH_BYTES as f64 / 1_048_576.0,
    );

    drop(proxy);
}
