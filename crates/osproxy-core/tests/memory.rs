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
use osproxy_core::cursor::{self, CursorSigner};
use osproxy_core::{ClusterId, ErrorCode, PartitionId, RequestId, TraceContext};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Allocations made while running `f` (the global profiler must already be live).
fn allocs(f: impl FnOnce()) -> u64 {
    let before = HeapStats::get().total_blocks;
    f();
    HeapStats::get().total_blocks - before
}

/// Fixed-tag signer: the codec's framing allocations are ours to budget; the HMAC
/// behind the real signer lives in the binary and is measured separately.
struct StubSigner;
impl CursorSigner for StubSigner {
    fn tag(&self, _msg: &[u8]) -> Vec<u8> {
        vec![0xab; 32]
    }
}

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

    // cursor::wrap frames the token into one growing String plus the two hex
    // scratch buffers and the signer's tag Vec — a fixed, low budget that runs on
    // every scroll/PIT response. Pin it so a future framing change can't quietly
    // add per-request allocations to the cursor path.
    let cluster = ClusterId::from("eu-west-1");
    let cursor_id = "c2NhbjsxOzEyMzQ1Njc4OTA";
    let n = allocs(|| {
        let _ = std::hint::black_box(cursor::wrap(&StubSigner, &cluster, cursor_id));
    });
    assert_eq!(n, CURSOR_WRAP_ALLOCS, "cursor::wrap allocation budget");

    // cursor::unwrap verifies and splits the token back on every continue.
    let token = cursor::wrap(&StubSigner, &cluster, cursor_id);
    let n = allocs(|| {
        let _ = std::hint::black_box(cursor::unwrap(&StubSigner, &token));
    });
    assert_eq!(n, CURSOR_UNWRAP_ALLOCS, "cursor::unwrap allocation budget");

    // TraceContext::parse decodes a traceparent into fixed-size arrays — no heap.
    let n = allocs(|| {
        let _ = std::hint::black_box(TraceContext::parse(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));
    });
    assert_eq!(n, 0, "TraceContext::parse must not allocate");

    // TraceContext::propagate continues a trace without a tracestate to copy, so
    // the per-request span derivation stays allocation-free.
    let rid = RequestId::from("req-1");
    let n = allocs(|| {
        let _ = std::hint::black_box(TraceContext::propagate(
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            None,
            &rid,
        ));
    });
    assert_eq!(
        n, 0,
        "TraceContext::propagate (no tracestate) must not allocate"
    );
}

/// The cursor codec's per-call allocation budgets, discovered by measurement and
/// pinned. Named so a change here is a deliberate, reviewed edit, not a tweak.
const CURSOR_WRAP_ALLOCS: u64 = 3;
const CURSOR_UNWRAP_ALLOCS: u64 = 4;
