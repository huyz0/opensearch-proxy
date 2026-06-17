//! Wall-clock micro-benchmark of directive evaluation (`cargo xtask bench-local`).
//!
//! `DirectiveSet::evaluate` runs on every request whenever any diagnostics
//! directive is active: a filtered scan of the active set with per-request
//! deterministic sampling (FNV-1a, no RNG). Typically a handful of directives are
//! live, so this measures the steady-state cost the directive spine adds to the
//! hot path. Wall-clock numbers are host-specific; the deterministic gate is the
//! dhat allocation budget in `tests/memory.rs`.

use std::time::Duration;

use osproxy_core::{Clock, EndpointKind, ManualClock, PrincipalId, RequestId};
use osproxy_observe::{
    DiagLevel, DiagnosticsDirective, DirectiveMatch, DirectiveSet, RequestAttrs,
};

fn main() {
    divan::main();
}

fn directive(
    level: DiagLevel,
    match_: DirectiveMatch,
    sample_per_mille: u16,
) -> DiagnosticsDirective {
    let clock = ManualClock::new();
    clock.advance(Duration::from_secs(3600));
    DiagnosticsDirective {
        id: "d".to_owned(),
        match_,
        level,
        sample_per_mille,
        expires_at: clock.now(),
        ring_buffer: false,
        capture: false,
    }
}

/// Evaluate a realistic small set (a sampled tenant directive plus a catch-all)
/// against a request — the per-request cost of the directive spine.
#[divan::bench]
fn evaluate_active_set() -> DiagLevel {
    let set = DirectiveSet::from_directives(vec![
        directive(DiagLevel::ShapeTiming, DirectiveMatch::all(), 100),
        directive(DiagLevel::Shape, DirectiveMatch::all(), 1000),
    ]);
    let principal = PrincipalId::from("svc");
    let attrs = RequestAttrs {
        tenant: None,
        index: "orders",
        principal: &principal,
        endpoint: EndpointKind::Search,
    };
    let now = ManualClock::new().now();
    let rid = RequestId::from("req-1");
    set.evaluate(divan::black_box(&attrs), now, divan::black_box(&rid))
}
