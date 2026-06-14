//! The error type an SPI implementation returns.
//!
//! Every variant maps to a stable [`ErrorCode`] and carries shape-only context
//! (which sources were tried, which partition id) so a failure is diagnosable
//! from telemetry without reading source (NFR-T5, `docs/02` §4). It never
//! carries tenant *values*.

use osproxy_core::{ErrorCode, PartitionId};
use thiserror::Error;

use crate::rules::PartitionKeySpecKind;

/// A failure returned by [`RoutingSpi`] or [`TenancySpi`].
///
/// [`RoutingSpi`]: crate::RoutingSpi
/// [`TenancySpi`]: crate::TenancySpi
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum SpiError {
    /// The partition could not be resolved from the request. Reports which
    /// source kinds were attempted (shape only).
    #[error("partition could not be resolved (tried: {tried:?})")]
    PartitionUnresolved {
        /// The source kinds tried, in order, before giving up.
        tried: Vec<PartitionKeySpecKind>,
    },

    /// No placement exists for the resolved partition.
    #[error("no placement exists for partition")]
    PlacementMissing {
        /// The unresolved partition (an id, safe in telemetry).
        partition: PartitionId,
    },

    /// The placement-lookup backend was unavailable.
    #[error("placement lookup backend unavailable (retryable={retryable})")]
    PlacementBackend {
        /// Whether the caller may retry the lookup.
        retryable: bool,
    },

    /// The request endpoint is not supported for tenancy rewriting in this mode.
    #[error("endpoint not supported for tenancy rewrite")]
    UnsupportedEndpoint {
        /// The endpoint classification that was rejected.
        endpoint: osproxy_core::EndpointKind,
    },

    /// An injected field draws its value from a principal attribute that the
    /// authenticated principal does not carry. A configuration/identity
    /// mismatch, surfaced as a routing failure rather than silently injecting a
    /// null (which would corrupt isolation).
    #[error("principal is missing an attribute required by an injected field")]
    PrincipalAttrMissing {
        /// The missing attribute name.
        attr: String,
    },

    /// A `SharedIndex` placement was configured with a doc-id rule that does not
    /// include the partition id, which would allow cross-tenant id collisions
    /// (`docs/03`). A configuration error, surfaced as a routing failure.
    #[error("shared-index doc-id rule must reference the partition id")]
    IdRuleMissingPartition,
}

impl SpiError {
    /// The stable [`ErrorCode`] for this failure, for trace attributes and
    /// `/debug/explain`.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::PartitionUnresolved { .. } => ErrorCode::PartitionUnresolved,
            Self::PlacementMissing { .. } => ErrorCode::PlacementMissing,
            Self::PlacementBackend { .. } => ErrorCode::PlacementBackendUnavailable,
            // IdRuleMissingPartition is a misconfiguration that prevents safe
            // routing; reuse the unsupported-endpoint contract code until a
            // dedicated config code is added (additive, see docs/08 §7).
            Self::UnsupportedEndpoint { .. }
            | Self::IdRuleMissingPartition
            | Self::PrincipalAttrMissing { .. } => ErrorCode::UnsupportedEndpoint,
        }
    }

    /// Whether the caller may retry, possibly after re-resolving placement.
    #[must_use]
    pub fn retryable(&self) -> bool {
        matches!(self, Self::PlacementBackend { retryable: true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_map_to_core_taxonomy() {
        assert_eq!(
            SpiError::PartitionUnresolved { tried: vec![] }.code(),
            ErrorCode::PartitionUnresolved
        );
        assert_eq!(
            SpiError::PlacementMissing {
                partition: PartitionId::from("p")
            }
            .code(),
            ErrorCode::PlacementMissing
        );
    }

    #[test]
    fn only_backend_unavailable_is_retryable() {
        assert!(SpiError::PlacementBackend { retryable: true }.retryable());
        assert!(!SpiError::PlacementBackend { retryable: false }.retryable());
        assert!(!SpiError::PlacementMissing {
            partition: PartitionId::from("p")
        }
        .retryable());
    }
}
