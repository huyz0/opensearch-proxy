//! Multicore contention benchmark for the two per-request hot-path operations:
//! `DirectiveStore::load()` (run once per request to evaluate directives) and
//! `ExplainStore::record()` (run once per request to retain the explain doc).
//!
//! It measures aggregate throughput (ops/sec) at rising thread counts, so a
//! lock that serializes cores shows up as throughput that *stops scaling*, while a
//! lock-free / cheaper path keeps climbing. Reported, never asserted (host-bound):
//! run with `--ignored --nocapture`. The same harness runs against the mutex
//! baseline and the optimized version, so the before/after is apples-to-apples.
#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use std::sync::{Arc, Barrier};
use std::thread;

use osproxy_core::{
    Clock, ClusterId, Epoch, FieldName, IndexName, PartitionId, RequestId, SystemClock,
};
use osproxy_observe::{
    ClassifyInfo, DirectiveStore, DispatchInfo, EgressInfo, ExplainStore, InMemoryDirectiveStore,
    RequestTrace, ResolveInfo,
};

/// Operations each thread performs per measurement, large enough that thread
/// spawn/join cost is negligible against the timed work.
const OPS_PER_THREAD: u64 = 200_000;

/// Thread counts to measure at (the "load" axis), capped at the machine's cores.
fn loads() -> Vec<usize> {
    let cores = thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get);
    [1usize, 2, 4, 8, 16]
        .into_iter()
        .filter(|&t| t <= cores.max(1))
        .collect()
}

/// A representative populated trace, so `record`'s cost reflects a real explain
/// document (classify + resolve + dispatch + egress), not an empty skeleton.
fn sample_trace() -> RequestTrace {
    let mut t = RequestTrace::new();
    t.record_classify(ClassifyInfo {
        endpoint: osproxy_core::EndpointKind::IngestDoc,
        logical_index: IndexName::from("orders"),
    });
    t.record_resolve(ResolveInfo {
        partition: PartitionId::from("acme"),
        placement_kind: "shared_index",
        cluster: ClusterId::from("eu-1"),
        index: IndexName::from("orders-shared"),
        epoch: Epoch::new(7),
        inject_fields: vec![FieldName::from("_tenant")],
        routing: true,
        migration: "settled",
    });
    t.record_dispatch(DispatchInfo {
        cluster: ClusterId::from("eu-1"),
        upstream_status: 201,
        pool_reuse: true,
    });
    t.record_egress(EgressInfo {
        status: 201,
        response_bytes: 48,
    });
    t
}

/// Runs `op` `OPS_PER_THREAD` times on each of `threads` threads, all released
/// together by a barrier, and returns aggregate ops/sec.
fn throughput<F>(threads: usize, op: F) -> f64
where
    F: Fn() + Send + Sync + 'static,
{
    let op = Arc::new(op);
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let op = Arc::clone(&op);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait(); // align starts so the cores truly contend
            for _ in 0..OPS_PER_THREAD {
                op();
            }
        }));
    }
    let clock = SystemClock;
    barrier.wait();
    let t0 = clock.now();
    for h in handles {
        h.join().unwrap();
    }
    let wall = clock.now().saturating_duration_since(t0);
    (threads as u64 * OPS_PER_THREAD) as f64 / wall.as_secs_f64()
}

#[test]
#[ignore = "contention benchmark; run with --ignored --nocapture"]
fn directive_store_load_scaling() {
    let store: Arc<dyn DirectiveStore> = Arc::new(InMemoryDirectiveStore::new());
    println!("DirectiveStore::load() throughput (Mops/s) by thread count:");
    for t in loads() {
        let store = Arc::clone(&store);
        let mops = throughput(t, move || {
            std::hint::black_box(store.load().len());
        }) / 1.0e6;
        println!("  threads={t:>2}: {mops:>8.2} Mops/s");
    }
}

#[test]
#[ignore = "contention benchmark; run with --ignored --nocapture"]
fn explain_store_record_scaling() {
    let store = Arc::new(ExplainStore::new(1024));
    let trace = Arc::new(sample_trace());
    let rid = RequestId::from("req-bench");
    println!("ExplainStore::record() throughput (Mops/s) by thread count:");
    for t in loads() {
        let store = Arc::clone(&store);
        let trace = Arc::clone(&trace);
        let rid = rid.clone();
        let mops = throughput(t, move || {
            store.record(rid.clone(), &trace);
        }) / 1.0e6;
        println!("  threads={t:>2}: {mops:>8.2} Mops/s");
    }
}
