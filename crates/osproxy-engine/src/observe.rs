//! Deriving shape-only trace spans from the pipeline's intermediate values.
//!
//! Bridges the engine's concrete types (a [`Resolved`] decision, a
//! [`WriteBatch`], an ack) into the observe crate's shape-only span structs
//! (`docs/05` §2). Every value here is an id, a name, a label, or a size — the
//! same no-value-leak discipline the trace API enforces.

use osproxy_core::{error::DecisionChain, ErrorContext, IndexName};
use osproxy_observe::{DispatchInfo, ResolveInfo, RewriteInfo};
use osproxy_sink::{DocOp, WriteAck, WriteBatch};
use osproxy_spi::{BodyTransform, SpiError};
use osproxy_tenancy::Resolved;

use crate::error::RequestError;

/// Builds the `spi.resolve` span from a resolved decision.
pub(crate) fn resolve_info(resolved: &Resolved) -> ResolveInfo {
    let decision = &resolved.decision;
    let transform = &decision.body_transform;
    ResolveInfo {
        partition: resolved.partition.clone(),
        placement_kind: if inject_field_names(transform).is_empty() {
            "dedicated"
        } else {
            "shared_index"
        },
        cluster: decision.target.cluster.clone(),
        index: decision.target.index.clone(),
        epoch: decision.epoch,
        inject_fields: inject_field_names(transform),
        routing: routing_enabled(transform),
    }
}

/// Builds the `rewrite` span from the transform and the produced batch.
pub(crate) fn rewrite_info(resolved: &Resolved, batch: &WriteBatch) -> RewriteInfo {
    RewriteInfo {
        transform_kind: transform_kind(&resolved.decision.body_transform),
        body_bytes: batch.ops().first().map_or(0, |op| match &op.doc {
            DocOp::Index { body, .. } | DocOp::Create { body, .. } | DocOp::Update { body, .. } => {
                body.len()
            }
            DocOp::Delete { .. } => 0,
        }),
    }
}

/// Builds the `dispatch` span from the resolved target and the upstream ack.
pub(crate) fn dispatch_info(resolved: &Resolved, ack: &WriteAck) -> DispatchInfo {
    DispatchInfo {
        cluster: resolved.decision.target.cluster.clone(),
        upstream_status: ack.results().first().map_or(0, |r| r.status),
        pool_reuse: ack.pool_reuse(),
    }
}

/// Builds the `dispatch` span for a read from the resolved target, the upstream
/// read status, and whether the read reused a pooled connection (a get-by-id or
/// query has no write ack).
pub(crate) fn read_dispatch_info(
    resolved: &Resolved,
    upstream_status: u16,
    pool_reuse: bool,
) -> DispatchInfo {
    DispatchInfo {
        cluster: resolved.decision.target.cluster.clone(),
        upstream_status,
        pool_reuse,
    }
}

/// Synthesizes an [`ErrorContext`] for a request-path failure, carrying the
/// stable code, retryability, a remediation hint, and whatever decision chain is
/// known at the point of failure (ids only).
pub(crate) fn error_context(err: &RequestError) -> ErrorContext {
    ErrorContext::new(err.code(), err.retryable(), remediation(err)).with_chain(chain_for(err))
}

/// The injected field *names* (never values) named by a transform.
fn inject_field_names(transform: &BodyTransform) -> Vec<osproxy_core::FieldName> {
    let fields = match transform {
        BodyTransform::Inject(fields) | BodyTransform::Both { inject: fields, .. } => {
            fields.as_slice()
        }
        BodyTransform::None | BodyTransform::ConstructId(_) => &[],
    };
    fields.iter().map(|f| f.name.clone()).collect()
}

/// Whether the transform sets `_routing`.
fn routing_enabled(transform: &BodyTransform) -> bool {
    match transform {
        BodyTransform::ConstructId(rule) | BodyTransform::Both { id: rule, .. } => rule.set_routing,
        BodyTransform::None | BodyTransform::Inject(_) => false,
    }
}

/// A compile-time label for the transform kind.
fn transform_kind(transform: &BodyTransform) -> &'static str {
    match transform {
        BodyTransform::None => "none",
        BodyTransform::Inject(_) => "inject",
        BodyTransform::ConstructId(_) => "construct_id",
        BodyTransform::Both { .. } => "inject+construct_id",
    }
}

/// A short, actionable remediation hint per failure, for the operator/LLM.
fn remediation(err: &RequestError) -> &'static str {
    match err {
        RequestError::Spi(SpiError::PartitionUnresolved { .. }) => {
            "ensure the request carries the configured partition key"
        }
        RequestError::Spi(SpiError::PlacementMissing { .. }) => {
            "register a placement for the partition"
        }
        RequestError::Spi(SpiError::PlacementBackend { .. }) => {
            "retry; the placement backend is unavailable"
        }
        RequestError::Spi(SpiError::UnsupportedEndpoint { .. }) => {
            "endpoint is not supported for tenancy rewrite in this version"
        }
        RequestError::Spi(
            SpiError::IdRuleMissingPartition | SpiError::PrincipalAttrMissing { .. },
        ) => "fix the tenancy configuration",
        // SpiError is non-exhaustive; a future variant gets a generic hint.
        RequestError::Spi(_) => "routing failed; consult the error code reference",
        RequestError::Rewrite(_) => {
            "the document body is malformed or collides with a reserved field"
        }
        RequestError::Sink(_) => "the upstream cluster failed; retry if retryable",
        RequestError::StaleEpoch { .. } => {
            "the partition is migrating; retry to re-resolve the new placement"
        }
        RequestError::Internal { .. } => "internal error; inspect logs",
    }
}

/// The decision chain known at the failure point (ids only).
fn chain_for(err: &RequestError) -> DecisionChain {
    match err {
        RequestError::Spi(SpiError::PlacementMissing { partition }) => DecisionChain {
            partition: Some(partition.clone()),
            ..DecisionChain::new()
        },
        _ => DecisionChain::new(),
    }
}

/// The logical index as a name (helper for the classify span).
pub(crate) fn logical_index(name: &str) -> IndexName {
    IndexName::from(name)
}
