//! The result of applying a [`WriteBatch`](crate::WriteBatch) at a sink.

/// The outcome of a single operation in a batch, positionally aligned with the
/// batch's operations (so a `_bulk` response can be re-interleaved in M3).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OpResult {
    /// The document id the operation acted on (constructed or auto-assigned).
    pub id: String,
    /// The upstream HTTP status for this operation.
    pub status: u16,
    /// Whether the document was newly created (vs. updated).
    pub created: bool,
}

impl OpResult {
    /// Constructs an operation result.
    #[must_use]
    pub fn new(id: impl Into<String>, status: u16, created: bool) -> Self {
        Self {
            id: id.into(),
            status,
            created,
        }
    }

    /// Whether the upstream status indicates success (2xx).
    #[must_use]
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// The acknowledgement for a whole batch: one [`OpResult`] per operation, in the
/// batch's original order.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct WriteAck {
    results: Vec<OpResult>,
    pool_reuse: bool,
}

impl WriteAck {
    /// An ack with the given per-operation results.
    #[must_use]
    pub fn new(results: Vec<OpResult>) -> Self {
        Self {
            results,
            pool_reuse: false,
        }
    }

    /// Records whether the dispatch(es) rode reused pooled connections, true
    /// only when every operation in the batch reused one (NFR-P telemetry).
    #[must_use]
    pub fn with_pool_reuse(mut self, reused: bool) -> Self {
        self.pool_reuse = reused;
        self
    }

    /// Whether this batch's dispatch rode reused pooled connection(s).
    #[must_use]
    pub fn pool_reuse(&self) -> bool {
        self.pool_reuse
    }

    /// The per-operation results, in batch order.
    #[must_use]
    pub fn results(&self) -> &[OpResult] {
        &self.results
    }

    /// Whether every operation in the batch succeeded.
    #[must_use]
    pub fn all_succeeded(&self) -> bool {
        self.results.iter().all(OpResult::is_success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_is_2xx() {
        assert!(OpResult::new("a", 201, true).is_success());
        assert!(OpResult::new("a", 200, false).is_success());
        assert!(!OpResult::new("a", 409, false).is_success());
    }

    #[test]
    fn ack_aggregates_success() {
        let ack = WriteAck::new(vec![
            OpResult::new("a", 201, true),
            OpResult::new("b", 200, false),
        ]);
        assert!(ack.all_succeeded());
        assert_eq!(ack.results().len(), 2);

        let mixed = WriteAck::new(vec![
            OpResult::new("a", 201, true),
            OpResult::new("b", 503, false),
        ]);
        assert!(!mixed.all_succeeded());
    }
}
