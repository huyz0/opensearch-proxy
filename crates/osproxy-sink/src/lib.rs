//! Write sink.
//!
//! Where writes go, isolated from how routing is decided
//! (`docs/decisions/008`). The [`Sink`] trait is the seam: `OpenSearchSink`
//! writes directly to a cluster, and the future queue-based redundancy mode is a
//! `QueueSink` drop-in behind the same trait. Epoch stamping is carried on every
//! [`WriteOp`] at this boundary (`docs/06` §2).
//!
//! M1 ships the trait, the [`WriteBatch`]/[`WriteAck`] vocabulary, and an
//! in-memory [`MemorySink`] for tests and dry-run. The real `OpenSearchSink`
//! (HTTP + connection pool) and its testcontainer round-trip land alongside the
//! transport layer.
#![deny(missing_docs)]

mod ack;
mod batch;
mod error;
mod memory;
mod sink;

pub use ack::{OpResult, WriteAck};
pub use batch::{DocOp, WriteBatch, WriteOp};
pub use error::SinkError;
pub use memory::MemorySink;
pub use sink::Sink;
