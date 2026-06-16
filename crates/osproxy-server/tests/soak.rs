//! The NFR-P6 footprint runner (`docs/01` §NFR-P6, `docs/11` M4 calibration): how
//! much memory the proxy holds when idle, and whether that footprint stays
//! bounded under a soak (the unbounded-buffer/queue guard). It spawns the **real
//! `osproxy` binary as its own process** pointed at a testcontainer OpenSearch,
//! reads the process's resident set from `/proc/<pid>/statm` — so the figure is
//! the proxy's footprint alone, not the test harness's — drives a sustained
//! write load, and re-reads. It then fills an [`osproxy_bench::FootprintProfile`]
//! and judges it.
//!
//! Needs Docker *and* Linux `/proc`, so it is `#[ignore]`'d like the other
//! testcontainer gates:
//!   `cargo test -p osproxy-server --test soak -- --ignored --nocapture`

// Test scaffolding (helpers + a spawned child proxy/container, not `#[test]`).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
// JUSTIFY(file-length): one cohesive soak runner — container scaffold, the child
// process lifecycle, RSS reading, and the soak driver belong together; the
// ~40-line container scaffold is shared with the perf harness only by copy, which
// is cheaper than a cross-test-binary shared module for two `#[ignore]` gates.

use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_bench::{footprint_brief, judge_footprint, FootprintProfile, FootprintThresholds};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::net::TcpListener;

const INDEX: &str = "osproxy-shared";
/// Linux page size assumed when converting `statm` resident pages to bytes (4 KiB
/// on every platform this runs on).
const PAGE_BYTES: u64 = 4096;
/// Requests driven through the proxy during the soak — enough that an unbounded
/// per-request buffer would show as a climbing resident set.
const SOAK_REQUESTS: u64 = 50_000;
/// Concurrency the soak is driven at.
const SOAK_CONCURRENCY: u32 = 16;

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

/// Starts a single-node OpenSearch (security disabled) and returns its base URL.
async fn start_opensearch() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("opensearchproject/opensearch", "2.11.1")
        .with_exposed_port(ContainerPort::Tcp(9200))
        .with_wait_for(WaitFor::message_on_stdout("] started"))
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .with_env_var("bootstrap.memory_lock", "false")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(9200).await.unwrap();
    (container, format!("http://{host}:{port}"))
}

/// A spawned `osproxy` child that is killed when the guard drops, so a panicking
/// test never leaks the process.
struct ProxyChild(Child);

impl Drop for ProxyChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawns the real `osproxy` binary pointed at `upstream`, on a free port, with
/// open auth. Returns the child guard, its base URL, and its pid.
async fn spawn_proxy_process(upstream: &str) -> (ProxyChild, String, u32) {
    // Claim a free port, then release it for the child to bind (a small race the
    // readiness poll closes).
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };
    let bind = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_osproxy"))
        .env("OSPROXY_BIND", &bind)
        .env("OSPROXY_UPSTREAM", upstream)
        .env("OSPROXY_INDEX", INDEX)
        .env("OSPROXY_TOKENS", "") // dev (open) auth
        .env("OSPROXY_ALLOW_CLEARTEXT_MUTATION", "1") // cleartext soak harness
        .spawn()
        .expect("spawn osproxy binary");
    let pid = child.id();
    (ProxyChild(child), format!("http://{bind}"), pid)
}

/// One logical ingest request through the proxy.
fn ingest(base: &str, i: u64) -> Request<Full<Bytes>> {
    let body = format!(r#"{{"tenant_id":"soak","id":{i},"msg":"x"}}"#);
    Request::builder()
        .method(Method::POST)
        .uri(format!("{base}/orders/_doc"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

/// Polls the proxy until it answers an HTTP request (any status — it's up), or
/// gives up. Returns whether it became ready.
async fn wait_proxy_ready(client: &HttpClient, base: &str) -> bool {
    for _ in 0..60 {
        if client.request(ingest(base, 0)).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

/// The resident set size of `pid` in bytes, read from `/proc/<pid>/statm` (field
/// 2 is resident pages). `None` if the process or `/proc` can't be read.
fn rss_bytes(pid: u32) -> Option<u64> {
    let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(resident_pages * PAGE_BYTES)
}

/// Drives `SOAK_REQUESTS` ingests through the proxy at `SOAK_CONCURRENCY`,
/// returning how many succeeded (2xx).
async fn soak(client: &HttpClient, base: &str) -> u64 {
    let next = Arc::new(AtomicU64::new(0));
    let ok = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for _ in 0..SOAK_CONCURRENCY {
        let client = client.clone();
        let base = base.to_owned();
        let next = next.clone();
        let ok = ok.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= SOAK_REQUESTS {
                    break;
                }
                if let Ok(resp) = client.request(ingest(&base, i)).await {
                    let success = resp.status().is_success();
                    let _ = resp.into_body().collect().await;
                    if success {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for w in workers {
        w.await.unwrap();
    }
    ok.load(Ordering::Relaxed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + Linux /proc; run with --ignored --nocapture"]
async fn nfr_p6_footprint_under_soak() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    let (proxy, proxy_base, pid) = spawn_proxy_process(&os_base).await;
    assert!(
        wait_proxy_ready(&client, &proxy_base).await,
        "proxy process did not become ready"
    );

    // Let startup allocations settle, then read the idle footprint.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let idle_rss_bytes = rss_bytes(pid).expect("read idle RSS");

    let ok = soak(&client, &proxy_base).await;
    assert_eq!(ok, SOAK_REQUESTS, "every soak ingest should succeed");

    // Let post-soak transients drain before reading, so the figure reflects
    // retained memory, not in-flight buffers.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let soak_rss_bytes = rss_bytes(pid).expect("read post-soak RSS");

    let profile = FootprintProfile {
        idle_rss_bytes,
        soak_rss_bytes,
        soak_requests: SOAK_REQUESTS,
    };
    let verdict = judge_footprint(&profile, &FootprintThresholds::provisional());
    let dir = env!("CARGO_TARGET_TMPDIR");
    std::fs::write(format!("{dir}/nfr-footprint.json"), profile.to_json()).unwrap();
    std::fs::write(
        format!("{dir}/nfr-footprint-verdict.json"),
        verdict.to_json(),
    )
    .unwrap();
    std::fs::write(
        format!("{dir}/nfr-footprint.md"),
        footprint_brief(&profile, &verdict),
    )
    .unwrap();
    println!("NFR-P6 footprint:\n{}", profile.to_json());
    println!(
        "idle = {:.1} MiB, post-soak = {:.1} MiB, growth = {:.2}x over {} reqs\nverdict (provisional):\n{}",
        idle_rss_bytes as f64 / 1_048_576.0,
        soak_rss_bytes as f64 / 1_048_576.0,
        profile.growth_ratio(),
        SOAK_REQUESTS,
        verdict.to_json(),
    );

    // Host-independent invariant: the footprint must not run away under load —
    // the proxy holds no unbounded per-request buffer or queue (NFR-P6). The
    // *absolute* idle figure is build/host-bound (this is a debug binary), so it
    // is recorded and judged but not hard-asserted; the growth finding (ratio OR
    // absolute bytes) is the leak guard that survives a small idle footprint.
    assert!(idle_rss_bytes > 0, "idle RSS should be measurable");
    let growth = verdict
        .findings
        .iter()
        .find(|f| f.nfr == "NFR-P6-growth")
        .expect("growth finding present");
    assert!(
        growth.pass,
        "footprint should stay bounded under soak: {}",
        growth.detail
    );

    drop(proxy); // explicit: kill the child before the container tears down.
}
