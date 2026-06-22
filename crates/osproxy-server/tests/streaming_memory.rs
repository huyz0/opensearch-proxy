//! Memory invariant for the streaming paths (ADR-014): a **very large** message
//! flows through the proxy with **bounded** memory — the proxy never buffers the
//! whole body. It spawns the real `osproxy` binary pointed at an in-process mock
//! upstream (no Docker) — in tenant-agnostic passthrough mode for the verbatim
//! request/response cases, and in tenancy mode for the streamed **search
//! response** case (a huge `aggregations` sibling must pipe through the hit
//! transform without being buffered) — and reads the proxy's own resident set
//! from `/proc/<pid>/statm` (so the figure is the proxy alone, not this harness).
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
/// buffering it) and returns a response of `response_size` bytes — so the proxy,
/// not the mock, is what the test measures, and backpressure is realistic.
async fn start_drain_upstream(response_size: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = move |req: Request<Incoming>| async move {
                    let mut body = req.into_body();
                    // Discard each request frame as it arrives — bounded memory.
                    while let Some(frame) = body.frame().await {
                        drop(frame);
                    }
                    Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(vec![
                        b'y';
                        response_size
                    ]))))
                };
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service_fn(svc))
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

/// A mock upstream that drains each request and answers every call with a search
/// envelope whose `aggregations` blob is `agg_size` bytes — empty hits, so the
/// proxy's hit transform forwards the giant sibling verbatim without buffering.
async fn start_search_upstream(agg_size: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = Arc::new(search_envelope(agg_size));
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let body = Arc::clone(&body);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = move |req: Request<Incoming>| {
                    let body = Arc::clone(&body);
                    async move {
                        let mut b = req.into_body();
                        while let Some(frame) = b.frame().await {
                            drop(frame);
                        }
                        Ok::<_, std::convert::Infallible>(Response::new(Full::new(
                            body.as_ref().clone(),
                        )))
                    }
                };
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service_fn(svc))
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

/// A valid `_search` response envelope with empty hits and an `aggregations` blob
/// of `agg_size` bytes — the genuinely-unbounded part of a real search response.
fn search_envelope(agg_size: usize) -> Bytes {
    let mut v =
        br#"{"took":1,"hits":{"total":{"value":0},"hits":[]},"aggregations":{"blob":""#.to_vec();
    v.resize(v.len() + agg_size, b'a');
    v.extend_from_slice(br#""}}"#);
    Bytes::from(v)
}

/// Spawns the real `osproxy` binary in whole-instance passthrough mode pointed at
/// `upstream`. Returns the child guard, its base URL, and its pid.
async fn spawn_passthrough_proxy(upstream: &str) -> (ProxyChild, String, u32) {
    spawn_proxy(upstream, true).await
}

/// Spawns the real `osproxy` binary in tenancy mode (no passthrough), so a
/// `_search` is routed, query-wrapped, and its response streamed through the hit
/// transform.
async fn spawn_tenancy_proxy(upstream: &str) -> (ProxyChild, String, u32) {
    spawn_proxy(upstream, false).await
}

/// Spawns the binary against `upstream`, optionally in whole-instance passthrough.
async fn spawn_proxy(upstream: &str, passthrough: bool) -> (ProxyChild, String, u32) {
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let bind = format!("127.0.0.1:{port}");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_osproxy"));
    cmd.env("OSPROXY_BIND", &bind)
        .env("OSPROXY_UPSTREAM", upstream)
        .env("OSPROXY_INDEX", "osproxy-shared")
        .env("OSPROXY_TOKENS", "") // dev (open) auth
        .env("OSPROXY_ALLOW_CLEARTEXT_MUTATION", "1");
    if passthrough {
        cmd.env("OSPROXY_PASSTHROUGH_CLUSTER", "mock")
            .env("OSPROXY_PASSTHROUGH_ENDPOINT", upstream);
    }
    let child = cmd.spawn().expect("spawn osproxy binary");
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

/// A `_search` against a logical index, carrying the tenant header the reference
/// tenancy resolves the partition from. The query body is small; the response is
/// what streams.
fn search_request(base: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::POST)
        .uri(format!("{base}/orders/_search"))
        .header("content-type", "application/json")
        .header("x-tenant", "acme")
        .body(Full::new(Bytes::from_static(
            br#"{"query":{"match_all":{}}}"#,
        )))
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
    let upstream = start_drain_upstream(16).await;
    let (proxy, base, pid) = spawn_passthrough_proxy(&upstream).await;
    assert!(
        wait_ready(&client, &base).await,
        "proxy did not become ready"
    );

    // Let startup allocations settle, then read the idle resident set.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = rss_bytes(pid).expect("read idle RSS");

    // Drive several large requests, sampling the proxy's RSS throughout — a
    // transient full-body buffer (held for the whole transfer) would be caught.
    let peak = peak_rss_during(pid, async {
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
            drain(resp.into_body()).await;
        }
    })
    .await;

    assert_bounded("request", idle, peak);
    drop(proxy);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux /proc; run with --ignored --nocapture"]
async fn a_large_passthrough_response_streams_with_bounded_memory() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    // The upstream returns a 64 MiB response; the proxy must pipe it back without
    // buffering — so its resident set stays small even as the big body flows.
    let upstream = start_drain_upstream(BIG_BODY).await;
    let (proxy, base, pid) = spawn_passthrough_proxy(&upstream).await;
    assert!(
        wait_ready(&client, &base).await,
        "proxy did not become ready"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = rss_bytes(pid).expect("read idle RSS");

    let peak = peak_rss_during(pid, async {
        for _ in 0..4 {
            let resp = client
                .request(passthrough_request(&base, 1))
                .await
                .expect("small request, big response");
            assert!(resp.status().is_success(), "status {}", resp.status());
            // Drain the big response frame by frame, never collecting it whole.
            drain(resp.into_body()).await;
        }
    })
    .await;

    assert_bounded("response", idle, peak);
    drop(proxy);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux /proc; run with --ignored --nocapture"]
async fn a_large_search_response_streams_with_bounded_memory() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    // The upstream returns a search envelope with a 64 MiB `aggregations` blob —
    // the part of a real search response that is genuinely unbounded. The proxy
    // routes the search, then streams the response back through the hit transform;
    // the giant sibling must flow through verbatim without ever being buffered.
    let upstream = start_search_upstream(BIG_BODY).await;
    let (proxy, base, pid) = spawn_tenancy_proxy(&upstream).await;
    assert!(
        wait_ready(&client, &base).await,
        "proxy did not become ready"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = rss_bytes(pid).expect("read idle RSS");

    let peak = peak_rss_during(pid, async {
        for _ in 0..4 {
            let resp = client
                .request(search_request(&base))
                .await
                .expect("search request");
            assert!(
                resp.status().is_success(),
                "search status {}",
                resp.status()
            );
            drain(resp.into_body()).await;
        }
    })
    .await;

    assert_bounded("search response", idle, peak);
    drop(proxy);
}

/// Drains a response body frame by frame (bounded memory), discarding the bytes.
async fn drain(mut body: Incoming) {
    while let Some(frame) = body.frame().await {
        drop(frame);
    }
}

/// Runs `work` while continuously sampling `pid`'s resident set, returning the
/// peak observed — so a buffer held for the duration of a transfer is caught even
/// without hitting the exact instant of peak.
async fn peak_rss_during<F: std::future::Future<Output = ()>>(pid: u32, work: F) -> u64 {
    let done = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(0));
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
    work.await;
    done.store(true, Ordering::Relaxed);
    sampler.await.unwrap();
    peak.load(Ordering::Relaxed)
}

/// Asserts the proxy's RSS grew far less than a buffered body would have, printing
/// the figures either way.
fn assert_bounded(direction: &str, idle: u64, peak: u64) {
    let growth = peak.saturating_sub(idle);
    println!(
        "{direction}: idle = {:.1} MiB, peak = {:.1} MiB, growth = {:.1} MiB over a {} MiB body",
        idle as f64 / 1_048_576.0,
        peak as f64 / 1_048_576.0,
        growth as f64 / 1_048_576.0,
        BIG_BODY / 1_048_576,
    );
    assert!(
        growth < MAX_GROWTH_BYTES,
        "passthrough {direction} must stream, not buffer: RSS grew {:.1} MiB for a {} MiB body (cap {:.0} MiB)",
        growth as f64 / 1_048_576.0,
        BIG_BODY / 1_048_576,
        MAX_GROWTH_BYTES as f64 / 1_048_576.0,
    );
}
