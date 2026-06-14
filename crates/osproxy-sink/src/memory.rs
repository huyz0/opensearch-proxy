//! An in-memory [`Sink`] for tests and dry-run routing.
//!
//! Records every batch it receives and acknowledges each operation as a
//! success, without any network. Not for production — it persists nothing — but
//! it lets the engine and routing logic be exercised end-to-end without a live
//! OpenSearch (the real `OpenSearchSink` is covered by a testcontainer
//! round-trip).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::ack::{OpResult, WriteAck};
use crate::batch::{DocOp, WriteBatch};
use crate::error::SinkError;
use crate::sink::Sink;

/// A non-persistent [`Sink`] that records batches and acknowledges success.
#[derive(Debug, Default)]
pub struct MemorySink {
    recorded: Mutex<Vec<WriteBatch>>,
    auto_id: AtomicU64,
}

impl MemorySink {
    /// Creates an empty recording sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The batches recorded so far, in arrival order.
    ///
    /// Recovers a poisoned lock: the recording is inert data with no invariant
    /// a panicking writer could tear, and a test asserting on it must not itself
    /// panic on poisoning.
    #[must_use]
    pub fn recorded(&self) -> Vec<WriteBatch> {
        self.recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Builds the success ack for a batch, assigning ids to auto-id operations.
    fn ack_for(&self, batch: &WriteBatch) -> WriteAck {
        let results = batch
            .ops()
            .iter()
            .map(|op| match &op.doc {
                DocOp::Index { id, .. } => {
                    let id = id.clone().unwrap_or_else(|| self.next_auto_id());
                    OpResult::new(id, 201, true)
                }
                DocOp::Delete { id, .. } => OpResult::new(id.clone(), 200, false),
            })
            .collect();
        WriteAck::new(results)
    }

    /// A deterministic id for an auto-id index op (`auto-1`, `auto-2`, …).
    fn next_auto_id(&self) -> String {
        let n = self.auto_id.fetch_add(1, Ordering::SeqCst) + 1;
        format!("auto-{n}")
    }
}

impl Sink for MemorySink {
    async fn write(&self, batch: WriteBatch) -> Result<WriteAck, SinkError> {
        let ack = self.ack_for(&batch);
        self.recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(batch);
        Ok(ack)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::WriteOp;
    use osproxy_core::{ClusterId, Epoch, IndexName, Target};

    fn index_op(id: Option<&str>) -> WriteOp {
        WriteOp::new(
            Target::new(ClusterId::from("c"), IndexName::from("i")),
            DocOp::Index {
                id: id.map(str::to_owned),
                routing: None,
                body: b"{}".to_vec(),
            },
            Epoch::new(1),
        )
    }

    #[tokio::test]
    async fn auto_ids_are_deterministic_and_increment() {
        let sink = MemorySink::new();
        let ack = sink
            .write(WriteBatch::new().with(index_op(None)).with(index_op(None)))
            .await
            .unwrap();
        assert_eq!(ack.results()[0].id, "auto-1");
        assert_eq!(ack.results()[1].id, "auto-2");
    }

    #[tokio::test]
    async fn explicit_id_is_preserved() {
        let sink = MemorySink::new();
        let ack = sink
            .write(WriteBatch::single(index_op(Some("p:7"))))
            .await
            .unwrap();
        assert_eq!(ack.results()[0].id, "p:7");
    }
}
