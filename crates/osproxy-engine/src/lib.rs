//! Pipeline orchestration.
//!
//! Drives a request through the stages — authenticate, authorize, classify,
//! resolve, transform, dispatch, reverse-transform, egress (`docs/04` §1) —
//! wiring the other crates together through `osproxy-core` types and
//! `osproxy-spi` traits. It owns no low-level wire or parsing detail.
//!
//! M1 lands the write-path core: [`build_write_batch`] turns a resolved routing
//! decision plus the request body into the epoch-stamped
//! [`WriteBatch`](osproxy_sink::WriteBatch) the sink delivers. M2 adds the
//! get-by-id read path: the [`Pipeline`] maps a client's logical id to the
//! physical id, fetches it through the [`Reader`](osproxy_sink::Reader) seam,
//! and strips the injected tenancy fields so the client sees its logical
//! document — the write→read round-trip symmetry the model rests on.
#![deny(missing_docs)]

mod endpoints;
mod error;
mod observe;
mod pipeline;
mod plan;
mod read;

pub use error::RequestError;
pub use pipeline::{Pipeline, PipelineResponse};
pub use plan::build_write_batch;
