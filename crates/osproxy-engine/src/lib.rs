//! Pipeline orchestration.
//!
//! Drives a request through the stages — authenticate, authorize, classify,
//! resolve, transform, dispatch, reverse-transform, egress (`docs/04` §1) —
//! wiring the other crates together through `osproxy-core` types and
//! `osproxy-spi` traits. It owns no low-level wire or parsing detail.
//!
//! M1 lands the write-path core: [`build_write_batch`] turns a resolved routing
//! decision plus the request body into the epoch-stamped
//! [`WriteBatch`](osproxy_sink::WriteBatch) the sink delivers. The HTTP ingress,
//! upstream pool, and `/debug/explain` wiring attach to this core alongside the
//! transport layer.
#![deny(missing_docs)]

mod error;
mod plan;

pub use error::RequestError;
pub use plan::build_write_batch;
