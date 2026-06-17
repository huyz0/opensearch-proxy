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

mod admin;
mod asyncwrite;
mod bulk;
mod bulkprep;
mod cursor;
mod dbq;
mod endpoints;
mod error;
mod mget;
mod msearch;
mod observe;
mod passthrough;
mod pipeline;
mod pit;
mod plan;
mod read;
mod retry;

pub use admin::AdminPolicy;
pub use asyncwrite::{
    op_id_for, unsupported_async, valid_op_id, NoQueue, QueueError, QueuedWrite, WriteMode,
    WriteQueue,
};
pub use error::RequestError;
pub use passthrough::PassthroughPolicy;
pub use pipeline::{Pipeline, PipelineResponse};
pub use plan::build_write_batch;
pub use retry::RetryPolicy;
