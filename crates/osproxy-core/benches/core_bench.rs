//! Deterministic microbenchmarks (docs/12).
//!
//! Measured in **instruction counts** via callgrind, not wall-clock time, so the
//! numbers are reproducible run-to-run and machine-to-machine, a real perf
//! regression gate rather than noise. Run in CI under valgrind:
//! `cargo xtask bench`.

use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use osproxy_core::cursor::{self, CursorSigner};
use osproxy_core::{ClusterId, Epoch, PartitionId, RequestId, TraceContext};

#[library_benchmark]
fn construct_partition_id() -> PartitionId {
    black_box(PartitionId::from(black_box("tenant-42")))
}

#[library_benchmark]
fn advance_epoch() -> Epoch {
    black_box(black_box(Epoch::new(41)).next())
}

// A signer with a fixed-length tag: it isolates the codec's framing cost (hex
// encode/decode, allocation, constant-time compare) from the crypto provider's
// HMAC, which lives in the binary and is not ours to regress on.
struct StubSigner;
impl CursorSigner for StubSigner {
    fn tag(&self, _msg: &[u8]) -> Vec<u8> {
        vec![0xab; 32]
    }
}

// `cursor::wrap`: frame a pinned cluster + upstream cursor into a signed token,
// runs on every scroll/PIT response.
#[library_benchmark]
fn cursor_wrap() -> String {
    let cluster = ClusterId::from("eu-west-1");
    cursor::wrap(
        &StubSigner,
        black_box(&cluster),
        black_box("c2NhbjsxOzEyMzQ1Njc4OTA"),
    )
}

// `cursor::unwrap`: verify and split a token back to (cluster, cursor), runs on
// every scroll/PIT continue. Fail-closed verify is the per-request read cost.
#[library_benchmark]
fn cursor_unwrap() -> Option<(ClusterId, String)> {
    let cluster = ClusterId::from("eu-west-1");
    let token = cursor::wrap(&StubSigner, &cluster, "c2NhbjsxOzEyMzQ1Njc4OTA");
    cursor::unwrap(&StubSigner, black_box(&token))
}

// `TraceContext::parse`: parse a W3C `traceparent`, runs on every request that
// carries one.
#[library_benchmark]
fn trace_parse() -> Option<TraceContext> {
    TraceContext::parse(black_box(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    ))
}

// `TraceContext::propagate`: continue (or mint) a trace and derive this hop's
// span, runs on every request.
#[library_benchmark]
fn trace_propagate() -> TraceContext {
    let rid = RequestId::from("req-1");
    TraceContext::propagate(
        black_box(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )),
        black_box(None),
        &rid,
    )
}

library_benchmark_group!(
    name = core_hot_paths;
    benchmarks =
        construct_partition_id,
        advance_epoch,
        cursor_wrap,
        cursor_unwrap,
        trace_parse,
        trace_propagate
);

main!(library_benchmark_groups = core_hot_paths);
