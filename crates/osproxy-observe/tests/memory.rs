//! Deterministic allocation-budget test for directive evaluation (docs/12).
//!
//! `DirectiveSet::evaluate` runs on every request when diagnostics directives are
//! active. It must stay on the cheap side of the hot path: a filtered scan plus
//! deterministic sampling that touches no heap. dhat's testing mode pins that to
//! zero so a regression which makes the per-request evaluation allocate fails CI.
//!
//! This binary installs the dhat allocator globally for itself only; production
//! binaries keep the system allocator.

use std::time::Duration;

use dhat::{HeapStats, Profiler};
use osproxy_core::{Clock, EndpointKind, ManualClock, PrincipalId, RequestId};
use osproxy_observe::{
    DiagLevel, DiagnosticsDirective, DirectiveMatch, DirectiveSet, RequestAttrs,
};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn allocs(f: impl FnOnce()) -> u64 {
    let before = HeapStats::get().total_blocks;
    f();
    HeapStats::get().total_blocks - before
}

fn directive(sample_per_mille: u16) -> DiagnosticsDirective {
    let clock = ManualClock::new();
    clock.advance(Duration::from_secs(3600));
    DiagnosticsDirective {
        id: "d".to_owned(),
        match_: DirectiveMatch::all(),
        level: DiagLevel::Shape,
        sample_per_mille,
        expires_at: clock.now(),
        ring_buffer: false,
        capture: false,
    }
}

#[test]
fn evaluate_does_not_allocate() {
    // Skip under coverage instrumentation: `cargo llvm-cov` rewrites the binary
    // with profiling counters that perturb heap-allocation counts, so these exact
    // budgets are meaningless and flaky there. The uninstrumented `performance`
    // gate enforces them for real.
    if std::env::var_os("LLVM_PROFILE_FILE").is_some() {
        return;
    }
    let _profiler = Profiler::builder().testing().build();

    let set = DirectiveSet::from_directives(vec![directive(100), directive(1000)]);
    let principal = PrincipalId::from("svc");
    let attrs = RequestAttrs {
        tenant: None,
        index: "orders",
        principal: &principal,
        endpoint: EndpointKind::Search,
    };
    let now = ManualClock::new().now();
    let rid = RequestId::from("req-1");

    let n = allocs(|| {
        let _ = std::hint::black_box(set.evaluate(&attrs, now, &rid));
    });
    assert_eq!(n, 0, "DirectiveSet::evaluate must not allocate per request");
}
