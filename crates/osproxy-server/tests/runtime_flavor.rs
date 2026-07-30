//! A/B comparison of the proxy's tokio runtime **flavor** (not a proxy
//! feature — infrastructure), to test a finding from `profile_64k.rs`'s
//! callgrind/strace profiling: at single-connection concurrency, `futex`
//! (the multi-thread scheduler's cross-thread work-stealing wakeup) is ~84%
//! of measured syscall time, dwarfing actual socket I/O. Switching that one
//! profiling target to `current_thread` cut futex calls from 1,241 to 4.
//!
//! That's a strong signal for *latency at low concurrency*, but a single OS
//! thread caps throughput at one core's worth of work — the question this
//! file answers is where the crossover is: does `current_thread` (or a small
//! fixed worker count) actually win in the concurrency range this proxy
//! really runs at, or does it just look good in the single-connection case?
//!
//! Same differential-avoiding-double-measurement methodology as
//! `mode_overhead.rs`: each flavor gets its own dedicated OS thread + runtime
//! serving a `SharedIndex` proxy (the mode `profile_64k.rs` profiles) over a
//! shared mock upstream, driven at several concurrency levels by the
//! generator on the outer test's own runtime. `#[ignore]`, host-bound,
//! reported never asserted — run with `--ignored --nocapture`.
#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

mod common;

use std::net::SocketAddr;
use std::sync::mpsc;

use common::{build_handler, payload, run_cell, serve, start_upstream};
use osproxy_server::tenancy::PlacementMode;

/// Matches production's allocator (see `profile_64k.rs`'s identical
/// declaration): each integration-test file is its own crate root, so
/// `main.rs`'s `#[global_allocator]` doesn't reach this binary without it.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// A tokio runtime configuration under test.
#[derive(Clone, Copy)]
enum RtFlavor {
    /// Everything (client-side tasks too, for the proxy's own accept/serve
    /// loop) cooperatively scheduled on one OS thread: no cross-thread
    /// wakeups, but no more than one core's worth of parallel work either.
    CurrentThread,
    /// The work-stealing multi-thread scheduler with a fixed worker count.
    /// `MultiThread(1)` isolates the scheduler-*flavor* cost (still pays for
    /// park/unpark machinery) from the worker-*count* cost that `CurrentThread`
    /// also removes.
    MultiThread(usize),
}

impl RtFlavor {
    fn label(self) -> String {
        match self {
            RtFlavor::CurrentThread => "current_thread".to_owned(),
            RtFlavor::MultiThread(n) => format!("multi_thread({n})"),
        }
    }

    fn build(self) -> tokio::runtime::Runtime {
        match self {
            RtFlavor::CurrentThread => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            RtFlavor::MultiThread(n) => tokio::runtime::Builder::new_multi_thread()
                .worker_threads(n)
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

/// Concurrency levels the generator drives each flavor at.
const CONNS: &[usize] = &[1, 4, 16, 64];
/// Requests per connection (so total load per cell scales with `conns`,
/// matching `mode_overhead.rs`'s convention).
const REQS_PER_CONN: usize = 100;

/// Spawns a `SharedIndex` proxy over `upstream` on its own dedicated OS thread
/// running `flavor`'s runtime, and returns its bound address. The thread parks
/// forever after startup (`pending::<()>`), so the proxy stays up for the
/// whole comparison; the process exiting at test end reclaims it.
fn spawn_proxy_with_flavor(flavor: RtFlavor, upstream: String) -> SocketAddr {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = flavor.build();
        rt.block_on(async move {
            let addr = serve(build_handler(&upstream, Some(PlacementMode::SharedIndex))).await;
            tx.send(addr).unwrap();
            std::future::pending::<()>().await;
        });
    });
    rx.recv().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "runtime-flavor A/B; run with --ignored --nocapture"]
async fn runtime_flavor_latency_and_throughput() {
    let upstream = start_upstream().await;
    let body = payload(256);

    // `available_parallelism` stands in for what `#[tokio::main]` (the real
    // `osproxy` binary's default) would pick on this box, the realistic
    // upper end of the comparison.
    let cores = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    let mut flavors = [
        RtFlavor::CurrentThread,
        RtFlavor::MultiThread(1),
        RtFlavor::MultiThread(2),
        RtFlavor::MultiThread(4),
        RtFlavor::MultiThread(cores),
    ];
    // Reverse iteration order half the time (by pid parity): a fixed order
    // can't distinguish a genuine flavor effect from thermal/frequency drift
    // across the run's wall-clock duration landing consistently on
    // later-tested flavors. Cheap ablation, not a full shuffle.
    if std::process::id().is_multiple_of(2) {
        flavors.reverse();
    }

    println!("RUNTIME FLAVOR A/B — SharedIndex proxy, 256B body, {cores} cores available");
    println!(
        "{:<18} {:>5} | {:>10} {:>9} {:>9}",
        "flavor", "conns", "rps", "p50 ms", "p99 ms"
    );
    for flavor in flavors {
        let addr = spawn_proxy_with_flavor(flavor, upstream.clone());
        for &conns in CONNS {
            let (rps, p50, p99) =
                run_cell(addr, "/orders/_doc", body.clone(), conns, REQS_PER_CONN).await;
            println!(
                "{:<18} {:>5} | {:>10.0} {:>9.3} {:>9.3}",
                flavor.label(),
                conns,
                rps,
                p50,
                p99
            );
        }
    }
}
