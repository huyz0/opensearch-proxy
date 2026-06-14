//! Pipeline orchestration.
//!
//! Drives a request through the stages — authenticate, authorize, classify,
//! resolve, transform, dispatch, reverse-transform, egress (`docs/04` §1) —
//! wiring the other crates together through `osproxy-core` types and
//! `osproxy-spi` traits. It owns no low-level wire or parsing detail. Lands in
//! M1 and grows each milestone.
