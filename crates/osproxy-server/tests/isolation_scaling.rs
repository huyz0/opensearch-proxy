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
#![allow(clippy::unwrap_used)]

mod common;

use std::net::SocketAddr;
use std::sync::mpsc;

use common::{build_handler, payload, run_cell, serve, start_upstream};
use osproxy_server::tenancy::PlacementMode;

const CONNS: &[usize] = &[16, 64, 256];
const REQS_PER_CONN: usize = 60;
const PAYLOAD: usize = 64 * 1024;
/// Worker threads for the isolated proxy's dedicated runtime (so generator and
/// proxy run on disjoint threads). Modest so total threads stay near core count.
const PROXY_WORKERS: usize = 6;

/// Co-located: upstream + proxy spawned onto the **current** (generator's) runtime.
async fn spawn_proxy_colocated() -> SocketAddr {
    let upstream = start_upstream().await;
    serve(build_handler(&upstream, Some(PlacementMode::SharedIndex))).await
}

/// Isolated: a **dedicated** multi-thread runtime on its own OS thread runs both
/// the upstream and the proxy, so they never share a worker thread with the
/// generator. The runtime lives for the test (the thread parks on `pending`).
fn spawn_proxy_isolated() -> SocketAddr {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(PROXY_WORKERS)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let upstream = start_upstream().await;
            let addr = serve(build_handler(&upstream, Some(PlacementMode::SharedIndex))).await;
            tx.send(addr).unwrap();
            std::future::pending::<()>().await;
        });
    });
    rx.recv().unwrap()
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
        let (crps, cp50, cp99) = run_cell(
            colocated,
            "/orders/_doc",
            body.clone(),
            conns,
            REQS_PER_CONN,
        )
        .await;
        let (irps, ip50, ip99) =
            run_cell(isolated, "/orders/_doc", body.clone(), conns, REQS_PER_CONN).await;
        println!(
            "{conns:>6} | {crps:>8.0} {cp50:>6.2} {cp99:>6.2} | {irps:>8.0} {ip50:>6.2} {ip99:>6.2}"
        );
    }
}
