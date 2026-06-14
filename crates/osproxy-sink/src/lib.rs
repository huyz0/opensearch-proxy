//! Write sink.
//!
//! Where writes go, isolated from how routing is decided
//! (`docs/decisions/008`). The [`Sink`] trait is the seam: `OpenSearchSink`
//! writes directly to a cluster, and the future queue-based redundancy mode is a
//! `QueueSink` drop-in behind the same trait. Epoch stamping is carried on every
//! [`WriteOp`] at this boundary (`docs/06` §2).
//!
//! M1 ships the [`Sink`] trait, the [`WriteBatch`]/[`WriteAck`] vocabulary, an
//! in-memory [`MemorySink`] for tests and dry-run, and the [`OpenSearchSink`]
//! that writes directly to a cluster over a pooled HTTP connection. M2 adds the
//! [`Reader`] seam for get-by-id reads (kept separate because a read is always
//! direct-to-cluster — a redundancy `QueueSink` can absorb writes but cannot
//! answer a read); both `MemorySink` and `OpenSearchSink` implement it.
#![deny(missing_docs)]

mod ack;
mod batch;
mod error;
mod memory;
mod opensearch;
mod read;
mod sink;
mod wire;

pub use ack::{OpResult, WriteAck};
pub use batch::{DocOp, WriteBatch, WriteOp};
pub use error::SinkError;
pub use memory::MemorySink;
pub use opensearch::OpenSearchSink;
pub use read::{CountOutcome, ReadOp, ReadOutcome, Reader, SearchOp, SearchOutcome};
pub use sink::Sink;
