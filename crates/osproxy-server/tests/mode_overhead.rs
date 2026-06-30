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
#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

mod common;

use std::net::SocketAddr;
use std::sync::mpsc;

use common::{build_handler, payload, run_cell, serve, start_upstream};
use osproxy_server::tenancy::PlacementMode;

const PAYLOADS: &[(&str, usize)] = &[("256B", 256), ("4KB", 4096), ("64KB", 65536)];
const CONNS: &[usize] = &[1, 8];
const REQS_PER_CONN: usize = 100;

/// The labelled modes, in increasing-cost order (`None` ⇒ passthrough).
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "comprehensive mode overhead; run with --ignored --nocapture"]
async fn mode_overhead_routing_vs_rewrite() {
    let (upstream, proxies) = spawn_server_side();
    let bodies: Vec<(&str, bytes::Bytes)> =
        PAYLOADS.iter().map(|&(l, s)| (l, payload(s))).collect();

    println!("COMPREHENSIVE MODE OVERHEAD — proxy ADDED latency (proxied − direct), ms p50/p99");
    println!("passthrough=stream/no-rewrite · ded-*=route/no-rewrite · shared=route+body-rewrite");
    print!("{:<6} {:>5} | {:>13}", "size", "conns", "direct");
    for (label, _) in &proxies {
        print!(" | {label:>13}");
    }
    println!();
    for (label, body) in &bodies {
        for &conns in CONNS {
            let (_, dp50, dp99) = run_cell(upstream, "/", body.clone(), conns, REQS_PER_CONN).await;
            print!("{label:<6} {conns:>5} | {dp50:>6.3} {dp99:>6.3}");
            for (_, addr) in &proxies {
                let (_, pp50, pp99) =
                    run_cell(*addr, "/orders/_doc", body.clone(), conns, REQS_PER_CONN).await;
                print!(" | {:>6.3} {:>6.3}", pp50 - dp50, pp99 - dp99);
            }
            println!();
        }
    }
}
