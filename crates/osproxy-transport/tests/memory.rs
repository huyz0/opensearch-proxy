//! Deterministic allocation-budget test for the classifier (docs/12).
//!
//! `classify` runs on every ingress request, so its per-call allocation count is
//! a hot-path budget: for a fixed path it allocates exactly the same number of
//! blocks every run. dhat's testing mode pins those counts so a regression that
//! adds a per-request allocation fails CI (wall-clock timing would not).
//!
//! This binary installs the dhat allocator globally for itself only; production
//! binaries keep the system allocator.

use dhat::{HeapStats, Profiler};
use osproxy_spi::HttpMethod;
use osproxy_transport::classify;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Allocations made while running `f` (the global profiler must be live).
fn allocs(f: impl FnOnce()) -> u64 {
    let before = HeapStats::get().total_blocks;
    f();
    HeapStats::get().total_blocks - before
}

#[test]
fn classify_allocation_budgets() {
    let _profiler = Profiler::builder().testing().build();

    // Ingest-by-id: only the owned logical index and doc id — the path segments
    // are matched on a stack array, not a heap Vec. Pin the count so a future
    // matcher change can't quietly add allocations to the hottest path in the
    // proxy.
    let n = allocs(|| {
        let _ = std::hint::black_box(classify(HttpMethod::Put, "/orders/_doc/acme:1"));
    });
    assert_eq!(n, CLASSIFY_INGEST_ALLOCS, "classify ingest-by-id budget");

    // Search carries no doc id, so it owns one fewer allocation than ingest.
    let n = allocs(|| {
        let _ = std::hint::black_box(classify(HttpMethod::Post, "/orders/_search"));
    });
    assert_eq!(n, CLASSIFY_SEARCH_ALLOCS, "classify search budget");
}

/// Classifier per-call allocation budgets, discovered by measurement and pinned
/// so a change is a deliberate, reviewed edit.
const CLASSIFY_INGEST_ALLOCS: u64 = 2;
const CLASSIFY_SEARCH_ALLOCS: u64 = 1;
