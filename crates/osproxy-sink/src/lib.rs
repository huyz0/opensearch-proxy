//! Write sink.
//!
//! Where writes go, isolated from how routing is decided
//! (`docs/decisions/008`). The [`Sink`] trait is the seam: `OpenSearchSink`
//! writes directly to a cluster, and the future queue-based redundancy mode is a
//! `QueueSink` drop-in behind the same trait. Epoch stamping is carried on every
//! [`WriteOp`] at this boundary (`docs/06` §2).
//!
//! M1 ships the trait, the [`WriteBatch`]/[`WriteAck`] vocabulary, an in-memory
//! [`MemorySink`] for tests and dry-run, and the [`OpenSearchSink`] that writes
//! directly to a cluster over a pooled HTTP connection. Upstream TLS via the
//! crypto seam attaches in the transport slice.
#![deny(missing_docs)]

mod ack;
mod batch;
mod error;
mod memory;
mod opensearch;
mod sink;

pub use ack::{OpResult, WriteAck};
pub use batch::{DocOp, WriteBatch, WriteOp};
pub use error::SinkError;
pub use memory::MemorySink;
pub use opensearch::OpenSearchSink;
pub use sink::Sink;
