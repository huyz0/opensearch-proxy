//! The [`Sink`] trait: where writes go, decoupled from how routing is decided.

use crate::ack::WriteAck;
use crate::batch::WriteBatch;
use crate::error::SinkError;

/// Where writes go.
///
/// Isolating the destination behind a trait keeps routing decisions independent
/// of delivery (`docs/decisions/008`): `OpenSearchSink` writes directly to a
/// cluster today, and a future `QueueSink` (Kafka) can take the *same*
/// [`WriteBatch`] for the redundancy mode with no change to the engine.
///
/// # Invariants
///
/// - MUST NOT panic; return [`SinkError`] for every failure (NFR-R1).
/// - The returned [`WriteAck`] MUST carry one result per batch operation, in the
///   batch's original order, so a bulk response can be re-interleaved (M3).
///
/// # Examples
///
/// ```
/// use osproxy_core::{ClusterId, Epoch, IndexName, Target};
/// use osproxy_sink::{DocOp, MemorySink, Sink, WriteBatch, WriteOp};
///
/// # async fn demo() {
/// let sink = MemorySink::new();
/// let op = WriteOp::new(
///     Target::new(ClusterId::from("c"), IndexName::from("i")),
///     DocOp::Index { id: Some("p:1".into()), routing: Some("p".into()), body: bytes::Bytes::from_static(b"{}") },
///     Epoch::new(1),
/// );
/// let ack = sink.write(WriteBatch::single(op)).await.unwrap();
/// assert!(ack.all_succeeded());
/// assert_eq!(sink.recorded().len(), 1);
/// # }
/// ```
pub trait Sink: Send + Sync {
    /// Applies a batch of writes, returning a per-operation acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError`] if the upstream rejects the whole request, the
    /// write cannot be delivered, or the epoch is stale (M5).
    fn write(
        &self,
        batch: WriteBatch,
    ) -> impl std::future::Future<Output = Result<WriteAck, SinkError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{DocOp, WriteOp};
    use crate::MemorySink;
    use osproxy_core::{ClusterId, Epoch, IndexName, Target};

    #[tokio::test]
    async fn memory_sink_records_and_acks() {
        let sink = MemorySink::new();
        let op = WriteOp::new(
            Target::new(ClusterId::from("c"), IndexName::from("i")),
            DocOp::Index {
                id: Some("p:1".to_owned()),
                routing: Some("p".to_owned()),
                body: bytes::Bytes::from_static(b"{}"),
            },
            Epoch::new(3),
        );
        let ack = sink.write(WriteBatch::single(op)).await.unwrap();
        assert!(ack.all_succeeded());
        assert_eq!(ack.results()[0].id, "p:1");
        assert_eq!(sink.recorded().len(), 1);
        assert_eq!(sink.recorded()[0].ops()[0].epoch, Epoch::new(3));
    }
}
