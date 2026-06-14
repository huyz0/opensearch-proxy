//! The unit of work handed to a [`Sink`](crate::Sink): epoch-stamped writes
//! against a single target.

use osproxy_core::{Epoch, Target};

/// A single write operation against a resolved [`Target`].
///
/// Carries the epoch the routing decision was derived from, stamped here so the
/// sink (or a future migration-aware backend) can reject a stale-epoch write
/// (`docs/06` §2). For M1 the epoch is recorded and forwarded; stale-epoch
/// rejection arrives with migration in M5.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WriteOp {
    /// The physical destination of this operation.
    pub target: Target,
    /// The document operation to perform.
    pub doc: DocOp,
    /// The placement epoch this write was resolved against.
    pub epoch: Epoch,
}

impl WriteOp {
    /// Constructs a write operation.
    #[must_use]
    pub fn new(target: Target, doc: DocOp, epoch: Epoch) -> Self {
        Self { target, doc, epoch }
    }
}

/// A document-level operation: the already-transformed body plus the
/// constructed id/routing (the tenancy rewrite has already run, `docs/04`).
///
/// Not `#[non_exhaustive]`: every sink must handle every op kind, so adding one
/// should force sinks to be updated.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DocOp {
    /// Index (create-or-replace) a document.
    Index {
        /// The constructed document id, or `None` to let OpenSearch auto-assign.
        id: Option<String>,
        /// The `_routing` value (the partition id), if routing is enabled.
        routing: Option<String>,
        /// The transformed document body (injected fields applied).
        body: Vec<u8>,
    },
    /// Delete a document by id.
    Delete {
        /// The constructed document id to delete.
        id: String,
        /// The `_routing` value, if routing is enabled.
        routing: Option<String>,
    },
}

/// A batch of operations destined for one target.
///
/// M1 single-doc ingest produces a one-operation batch; the same type carries a
/// demultiplexed per-target slice of a `_bulk` request in M3 (`docs/04` §3).
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct WriteBatch {
    ops: Vec<WriteOp>,
}

impl WriteBatch {
    /// An empty batch.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A batch of a single operation (the M1 single-doc case).
    #[must_use]
    pub fn single(op: WriteOp) -> Self {
        Self { ops: vec![op] }
    }

    /// Appends an operation (builder style).
    #[must_use]
    pub fn with(mut self, op: WriteOp) -> Self {
        self.ops.push(op);
        self
    }

    /// The operations in this batch, in order.
    #[must_use]
    pub fn ops(&self) -> &[WriteOp] {
        &self.ops
    }

    /// Whether the batch has no operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// The number of operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{ClusterId, IndexName};

    fn op(id: &str) -> WriteOp {
        WriteOp::new(
            Target::new(ClusterId::from("c"), IndexName::from("i")),
            DocOp::Index {
                id: Some(id.to_owned()),
                routing: Some("p".to_owned()),
                body: b"{}".to_vec(),
            },
            Epoch::new(1),
        )
    }

    #[test]
    fn single_batch_has_one_op() {
        let b = WriteBatch::single(op("x"));
        assert_eq!(b.len(), 1);
        assert!(!b.is_empty());
        assert_eq!(b.ops()[0].epoch, Epoch::new(1));
    }

    #[test]
    fn empty_and_builder() {
        let b = WriteBatch::new();
        assert!(b.is_empty());
        let b = b.with(op("a")).with(op("b"));
        assert_eq!(b.len(), 2);
    }
}
