//! Failures returned by a [`Sink`](crate::Sink).

use osproxy_core::{Epoch, ErrorCode};
use thiserror::Error;

/// A failure applying a write at the sink.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum SinkError {
    /// The upstream cluster returned an error status for the whole request
    /// (not a per-item failure, which is carried in the ack).
    #[error("upstream returned {status} (retryable={retryable})")]
    Upstream {
        /// The upstream HTTP status.
        status: u16,
        /// Whether the caller may retry.
        retryable: bool,
    },

    /// The write could not be delivered (connection reset, timeout, TLS). The
    /// message is a shape/category description, never tenant data.
    #[error("transport failure: {kind}")]
    Transport {
        /// A short, value-free description of the transport failure.
        kind: &'static str,
    },

    /// The write was resolved against an epoch that is stale for a migrating
    /// partition; the caller must re-resolve and retry (`docs/06` §2). Wired in
    /// M5; defined here so the sink contract is stable.
    #[error("stale epoch {stamped} (current {current})")]
    StaleEpoch {
        /// The epoch the rejected write carried.
        stamped: Epoch,
        /// The current epoch the sink expected.
        current: Epoch,
    },
}

impl SinkError {
    /// The stable [`ErrorCode`] for this failure.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Upstream { .. } | Self::Transport { .. } => ErrorCode::UpstreamFailed,
            Self::StaleEpoch { .. } => ErrorCode::StaleEpoch,
        }
    }

    /// Whether the caller may retry (possibly after re-resolving placement).
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self {
            Self::Upstream { retryable, .. } => *retryable,
            // A transport failure is transient; a stale epoch is retryable after
            // the client re-resolves the placement.
            Self::Transport { .. } | Self::StaleEpoch { .. } => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_and_retryability() {
        assert_eq!(
            SinkError::Upstream {
                status: 503,
                retryable: true
            }
            .code(),
            ErrorCode::UpstreamFailed
        );
        assert!(SinkError::Transport { kind: "reset" }.retryable());
        assert!(SinkError::StaleEpoch {
            stamped: Epoch::new(1),
            current: Epoch::new(2)
        }
        .retryable());
        assert!(!SinkError::Upstream {
            status: 400,
            retryable: false
        }
        .retryable());
    }
}
