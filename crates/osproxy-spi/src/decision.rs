//! What the SPI returns: where to send a request and how to transform it.

use osproxy_core::{Epoch, Target};

use crate::request::Protocol;
use crate::rules::{DocIdRule, InjectedField};

/// A mutation to apply to the request headers before forwarding upstream.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum HeaderOp {
    /// Add a header (does not remove an existing one of the same name).
    Add {
        /// Header name.
        name: String,
        /// Header value.
        value: String,
    },
    /// Remove all headers with this name.
    Remove {
        /// Header name to remove.
        name: String,
    },
    /// Replace (remove-then-add) a header.
    Replace {
        /// Header name.
        name: String,
        /// New value.
        value: String,
    },
}

/// How the request body must be transformed before it is forwarded.
///
/// For single-doc ingest the transform injects tenancy fields and/or constructs
/// the document `_id`. `osproxy-rewrite` performs the transform; this enum is
/// the instruction. Not `#[non_exhaustive]`: the engine must apply every
/// transform kind, so a new kind should force the plan builder to be updated.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum BodyTransform {
    /// Forward the body unchanged.
    None,
    /// Inject named fields into the document.
    Inject(Vec<InjectedField>),
    /// Construct the `_id` (and optionally `_routing`) from a rule.
    ConstructId(DocIdRule),
    /// Both inject fields and construct the id.
    Both {
        /// Fields to inject.
        inject: Vec<InjectedField>,
        /// Id-construction rule.
        id: DocIdRule,
    },
}

impl BodyTransform {
    /// Whether this transform leaves the body untouched.
    #[must_use]
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// The routing decision: the single destination plus the transforms to apply.
///
/// In v1 exactly one [`Target`] is resolved — no synchronous fan-out (ADR-002).
/// The [`Epoch`] is the placement-table generation the decision was derived
/// from; it is stamped onto the write so the sink can reject a stale-epoch write
/// during a migration (`docs/06` §2).
///
/// Read-path concerns (query filter, response strip, cursor affinity) arrive in
/// M2/M5 and will extend this struct additively (`docs/11`).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RouteDecision {
    /// The single physical destination.
    pub target: Target,
    /// The protocol to use upstream (may differ from ingress).
    pub upstream_protocol: Protocol,
    /// Header mutations to apply before forwarding.
    pub header_ops: Vec<HeaderOp>,
    /// The body transform to apply.
    pub body_transform: BodyTransform,
    /// The placement epoch this decision was derived from.
    pub epoch: Epoch,
}

impl RouteDecision {
    /// Constructs a decision with no header ops and no body transform.
    #[must_use]
    pub fn passthrough(target: Target, upstream_protocol: Protocol, epoch: Epoch) -> Self {
        Self {
            target,
            upstream_protocol,
            header_ops: Vec::new(),
            body_transform: BodyTransform::None,
            epoch,
        }
    }

    /// Sets the body transform (builder style).
    #[must_use]
    pub fn with_body_transform(mut self, transform: BodyTransform) -> Self {
        self.body_transform = transform;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{ClusterId, IndexName};

    fn target() -> Target {
        Target::new(ClusterId::from("c"), IndexName::from("i"))
    }

    #[test]
    fn passthrough_has_no_transform() {
        let d = RouteDecision::passthrough(target(), Protocol::Http1, Epoch::ZERO);
        assert!(d.body_transform.is_none());
        assert!(d.header_ops.is_empty());
        assert_eq!(d.epoch, Epoch::ZERO);
    }

    #[test]
    fn body_transform_can_be_attached() {
        let d = RouteDecision::passthrough(target(), Protocol::Http1, Epoch::new(2))
            .with_body_transform(BodyTransform::Inject(vec![]));
        assert!(!d.body_transform.is_none());
    }
}
