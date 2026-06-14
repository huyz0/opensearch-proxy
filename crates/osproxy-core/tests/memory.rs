//! Deterministic memory-budget tests (docs/12).
//!
//! Wall-clock timing is noisy, but **allocation counts are deterministic**: for a
//! fixed input, a given code path makes exactly the same number of heap
//! allocations every run. `dhat`'s testing mode lets us assert those counts, so a
//! regression that adds an allocation to a hot path fails CI.
//!
//! This test binary installs the dhat allocator globally for itself only;
//! production binaries keep the system allocator.

use dhat::{HeapStats, Profiler};
use osproxy_core::{ErrorCode, PartitionId};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// All allocation budgets in one test: `dhat`'s profiler is process-global, so
/// only one may exist at a time (parallel `#[test]`s would collide). Each budget
/// is an independent before/after assertion within the single profiler.
#[test]
fn core_allocation_budgets() {
    let _profiler = Profiler::builder().testing().build();

    // Mapping a code to its static slug must not allocate (returns &'static).
    let before = HeapStats::get().total_blocks;
    let _ = std::hint::black_box(ErrorCode::StaleEpoch.as_slug());
    assert_eq!(
        HeapStats::get().total_blocks,
        before,
        "as_slug must not allocate"
    );

    // PartitionId::from allocates exactly once: the owned String backing the id.
    let before = HeapStats::get().total_blocks;
    let id = std::hint::black_box(PartitionId::from("tenant-42"));
    assert_eq!(
        HeapStats::get().total_blocks - before,
        1,
        "PartitionId::from should allocate exactly once"
    );
    assert_eq!(id.as_str(), "tenant-42");
}
