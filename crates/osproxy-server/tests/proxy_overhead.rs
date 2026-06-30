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
//! overhead. `#[ignore]`, host-bound, reported never asserted — run with
//! `--ignored --nocapture`.
#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

mod common;

use std::net::SocketAddr;
use std::sync::mpsc;

use common::{build_handler, payload, run_cell, serve, start_upstream};
use osproxy_server::tenancy::PlacementMode;

/// (label, size in bytes), built once and reused for every cell.
const PAYLOADS: &[(&str, usize)] = &[("256B", 256), ("4KB", 4096), ("64KB", 65536)];
/// Low concurrency: we want per-request overhead, not the saturation tail.
const CONNS: &[usize] = &[1, 8];
const REQS_PER_CONN: usize = 100;

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
            // Default mode = SharedIndex (the body-rewrite path).
            let proxy = serve(build_handler(&upstream, Some(PlacementMode::SharedIndex))).await;
            tx.send((upstream_addr, proxy)).unwrap();
            std::future::pending::<()>().await;
        });
    });
    rx.recv().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "proxy-overhead differential; run with --ignored --nocapture"]
async fn proxy_added_latency_by_payload() {
    let (upstream, proxy) = spawn_server_side();
    let bodies: Vec<(&str, bytes::Bytes)> =
        PAYLOADS.iter().map(|&(l, s)| (l, payload(s))).collect();

    println!("proxy ADDED latency = proxied − direct (ms); per-request overhead, harness cancels");
    println!(
        "{:<6} {:>5} | {:>14} | {:>14} | {:>14}",
        "size", "conns", "direct p50/p99", "proxied p50/p99", "ADDED p50/p99"
    );
    for (label, body) in &bodies {
        for &conns in CONNS {
            let (_, dp50, dp99) = run_cell(upstream, "/", body.clone(), conns, REQS_PER_CONN).await;
            let (_, pp50, pp99) =
                run_cell(proxy, "/orders/_doc", body.clone(), conns, REQS_PER_CONN).await;
            println!(
                "{label:<6} {conns:>5} | {dp50:>6.3} {dp99:>6.3} | {pp50:>6.3} {pp99:>6.3} | {:>6.3} {:>6.3}",
                pp50 - dp50,
                pp99 - dp99
            );
        }
    }
}
