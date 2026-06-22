//! Wall-clock comparison of the buffered vs streaming `_search` response
//! transform (divan; a local calibration tool, **not** a CI gate — the
//! deterministic gates stay dhat + iai-callgrind, docs/12).
//!
//! It makes the streaming trade measurable: the streamed path bounds peak memory
//! to one hit (proven by `osproxy-server/tests/streaming_memory.rs`), and this
//! bench shows the CPU cost of that — comparable-or-faster on hit-heavy
//! responses, and, after the `Passthrough` bulk-copy in `search_scan`, parity on
//! aggregation-heavy ones (where the buffered path memcpys an unparsed
//! `aggregations` `RawValue` and the streamed path forwards the same bytes).

use divan::Bencher;
use osproxy_engine::bench_support::{buffered, response, streaming};

fn main() {
    divan::main();
}

/// Builds the body once (outside the timed region) and benches `transform` over
/// it, black-boxing the input so the optimizer cannot hoist the work out.
fn run(bencher: Bencher, n_hits: usize, agg_bytes: usize, transform: fn(&[u8]) -> Vec<u8>) {
    let body = response(n_hits, agg_bytes);
    bencher.bench_local(|| transform(divan::black_box(&body)));
}

// Aggregation-heavy: a 4 MiB `aggregations` sibling dominates. Buffered memcpys
// its unparsed RawValue; streaming forwards the same bytes via the bulk-copy.
#[divan::bench]
fn agg_heavy_buffered(b: Bencher) {
    run(b, 100, 4 << 20, buffered);
}
#[divan::bench]
fn agg_heavy_streaming(b: Bencher) {
    run(b, 100, 4 << 20, streaming);
}

// Mixed: a moderate hit count with a moderate aggregations blob.
#[divan::bench]
fn mixed_buffered(b: Bencher) {
    run(b, 10_000, 256 << 10, buffered);
}
#[divan::bench]
fn mixed_streaming(b: Bencher) {
    run(b, 10_000, 256 << 10, streaming);
}

// Hit-heavy: many hits, no aggregations. Buffered materializes one large `Value`
// for the whole hits array; streaming parses one hit at a time.
#[divan::bench]
fn hits_heavy_buffered(b: Bencher) {
    run(b, 50_000, 0, buffered);
}
#[divan::bench]
fn hits_heavy_streaming(b: Bencher) {
    run(b, 50_000, 0, streaming);
}
