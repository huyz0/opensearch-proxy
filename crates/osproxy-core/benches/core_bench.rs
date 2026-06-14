//! Deterministic microbenchmarks (docs/12).
//!
//! Measured in **instruction counts** via callgrind, not wall-clock time, so the
//! numbers are reproducible run-to-run and machine-to-machine — a real perf
//! regression gate rather than noise. Run in CI under valgrind:
//! `cargo xtask bench`.

use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use osproxy_core::{Epoch, PartitionId};

#[library_benchmark]
fn construct_partition_id() -> PartitionId {
    black_box(PartitionId::from(black_box("tenant-42")))
}

#[library_benchmark]
fn advance_epoch() -> Epoch {
    black_box(black_box(Epoch::new(41)).next())
}

library_benchmark_group!(
    name = core_hot_paths;
    benchmarks = construct_partition_id, advance_epoch
);

main!(library_benchmark_groups = core_hot_paths);
