//! The top-level request-path error.
//!
//! Built from each stage's sub-error so the decision chain
//! (principal → partition → placement → epoch → upstream) is preserved for
//! diagnosis without source reading (NFR-T5, `docs/02` §4). Carries codes and
//! shapes only — never tenant values.

use osproxy_core::ErrorCode;
use osproxy_rewrite::RewriteError;
use osproxy_sink::SinkError;
use osproxy_spi::SpiError;
use thiserror::Error;

/// A failure anywhere on the request path.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RequestError {
    /// Routing (partition resolution / placement) failed.
    #[error("routing failed: {0}")]
    Spi(#[from] SpiError),

    /// A body transform failed (malformed document, reserved-field collision).
    #[error("rewrite failed: {0}")]
    Rewrite(#[from] RewriteError),

    /// The write could not be delivered or was rejected upstream.
    #[error("sink failed: {0}")]
    Sink(#[from] SinkError),

    /// The write resolved against a placement epoch no longer current for a
    /// migrating partition: the migration write gate held it (`docs/06` §2).
    /// Retryable — the client re-resolves against the new placement.
    #[error("stale placement epoch {stamped} for a migrating partition")]
    StaleEpoch {
        /// The epoch the rejected decision was stamped with (an id, not data).
        stamped: osproxy_core::Epoch,
    },

    /// An internal invariant was violated — a bug, not a client or upstream
    /// fault. Carries a static reason (never tenant data) for the operator/LLM.
    #[error("internal invariant violated: {reason}")]
    Internal {
        /// A short, value-free description of the violated invariant.
        reason: &'static str,
    },
}

impl RequestError {
    /// The stable [`ErrorCode`] for this failure, surfaced into the trace and
    /// `/debug/explain`.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Spi(e) => e.code(),
            Self::Sink(e) => e.code(),
            Self::StaleEpoch { .. } => ErrorCode::StaleEpoch,
            // A malformed body or reserved-field collision is an unsupported /
            // rejected request shape; reuse the unsupported-endpoint code until
            // a dedicated rewrite code is added (additive, docs/08 §7).
            Self::Rewrite(_) | Self::Internal { .. } => ErrorCode::UnsupportedEndpoint,
        }
    }

    /// Whether the caller may retry.
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self {
            Self::Spi(e) => e.retryable(),
            Self::Sink(e) => e.retryable(),
            // A stale epoch is retryable: the retry re-resolves the placement.
            Self::StaleEpoch { .. } => true,
            Self::Rewrite(_) | Self::Internal { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::PartitionId;

    #[test]
    fn spi_error_code_propagates() {
        let err: RequestError = SpiError::PlacementMissing {
            partition: PartitionId::from("p"),
        }
        .into();
        assert_eq!(err.code(), ErrorCode::PlacementMissing);
        assert!(!err.retryable());
    }

    #[test]
    fn sink_error_retryability_propagates() {
        let err: RequestError = SinkError::Transport { kind: "reset" }.into();
        assert_eq!(err.code(), ErrorCode::UpstreamFailed);
        assert!(err.retryable());
    }

    #[test]
    fn rewrite_and_internal_are_terminal() {
        let err: RequestError = RewriteError::NotAnObject.into();
        assert_eq!(err.code(), ErrorCode::UnsupportedEndpoint);
        assert!(!err.retryable());
        assert!(!RequestError::Internal { reason: "x" }.retryable());
    }
}
