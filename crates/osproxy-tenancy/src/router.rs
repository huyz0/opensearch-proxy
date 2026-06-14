//! Adapts a high-level [`TenancySpi`] into the low-level [`RoutingSpi`].
//!
//! This is where declarative tenancy rules become a concrete routing decision:
//! resolve the partition, look up its placement, derive the physical target,
//! and assemble the body transform (with injected-field values already resolved
//! to constants, so downstream stages stay pure). The `SharedIndex`
//! partition-in-id invariant (`docs/03`) is enforced here.

use osproxy_core::{IndexName, PartitionId, Target};
use osproxy_spi::{
    BodyTransform, InjectedField, InjectedValue, Placement, RequestCtx, RouteDecision, RoutingSpi,
    SpiError, TenancySpi,
};
use serde_json::Value;

use crate::resolve::resolve_partition;

/// A fully resolved routing decision plus the partition it was resolved for.
///
/// The engine consumes this richer result directly (it needs the partition to
/// construct the document `_id` and `_routing`); the [`RoutingSpi`] impl exposes
/// just the [`RouteDecision`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Resolved {
    /// The resolved partition id.
    pub partition: PartitionId,
    /// The routing decision derived from the partition's placement.
    pub decision: RouteDecision,
}

/// Turns a [`TenancySpi`] implementation into a [`RoutingSpi`].
#[derive(Debug)]
pub struct TenancyRouter<T> {
    spi: T,
}

impl<T: TenancySpi> TenancyRouter<T> {
    /// Wraps a tenancy implementation.
    #[must_use]
    pub fn new(spi: T) -> Self {
        Self { spi }
    }

    /// The wrapped tenancy implementation.
    #[must_use]
    pub fn spi(&self) -> &T {
        &self.spi
    }

    /// Resolves the full routing plan for `ctx`.
    ///
    /// # Errors
    ///
    /// Returns [`SpiError`] if the endpoint is not tenancy-aware, the partition
    /// cannot be resolved, no placement exists, or the configured transforms are
    /// invalid (e.g. a shared-index id rule that omits the partition).
    pub async fn resolve(&self, ctx: &RequestCtx<'_>) -> Result<Resolved, SpiError> {
        if !ctx.endpoint().is_tenancy_aware() {
            return Err(SpiError::UnsupportedEndpoint {
                endpoint: ctx.endpoint(),
            });
        }
        // A single document parses cheaply; bulk streaming is M3 (docs/04 §3).
        let doc = serde_json::from_slice::<Value>(ctx.body()).ok();
        let partition = resolve_partition(&self.spi.partition_key(), ctx, doc.as_ref())?;
        let at = self.spi.placement_for(&partition).await?;
        let target = target_for(&at.placement, ctx.logical_index());
        let body_transform = self.build_transform(&at.placement, &partition, ctx)?;

        let decision = RouteDecision {
            target,
            upstream_protocol: ctx.protocol(),
            header_ops: Vec::new(),
            body_transform,
            epoch: at.epoch,
        };
        Ok(Resolved {
            partition,
            decision,
        })
    }

    /// Builds the body transform for a placement, resolving injected-field
    /// values and enforcing the shared-index partition-in-id invariant.
    fn build_transform(
        &self,
        placement: &Placement,
        partition: &PartitionId,
        ctx: &RequestCtx<'_>,
    ) -> Result<BodyTransform, SpiError> {
        let inject = match placement {
            Placement::SharedIndex { inject, .. } => resolve_inject(inject, partition, ctx)?,
            Placement::DedicatedCluster { .. } | Placement::DedicatedIndex { .. } => Vec::new(),
        };

        let id_rule = self.spi.doc_id_rule();
        if let (Placement::SharedIndex { .. }, Some(rule)) = (placement, id_rule.as_ref()) {
            if !rule.template.references_partition() {
                return Err(SpiError::IdRuleMissingPartition);
            }
        }

        Ok(match (inject.is_empty(), id_rule) {
            (true, None) => BodyTransform::None,
            (false, None) => BodyTransform::Inject(inject),
            (true, Some(id)) => BodyTransform::ConstructId(id),
            (false, Some(id)) => BodyTransform::Both { inject, id },
        })
    }
}

impl<T: TenancySpi> RoutingSpi for TenancyRouter<T> {
    async fn route(&self, ctx: &RequestCtx<'_>) -> Result<RouteDecision, SpiError> {
        Ok(self.resolve(ctx).await?.decision)
    }
}

/// Derives the physical [`Target`] from a placement and the request's logical
/// index. A dedicated cluster carries the logical index name unchanged; the
/// other modes pin a concrete physical index.
fn target_for(placement: &Placement, logical_index: &str) -> Target {
    match placement {
        Placement::DedicatedCluster { cluster } => {
            Target::new(cluster.clone(), IndexName::from(logical_index))
        }
        Placement::DedicatedIndex { cluster, index }
        | Placement::SharedIndex { cluster, index, .. } => {
            Target::new(cluster.clone(), index.clone())
        }
    }
}

/// Resolves each [`InjectedValue`] to a concrete constant, so downstream stages
/// inject literals and need no access to the principal or partition.
fn resolve_inject(
    fields: &[InjectedField],
    partition: &PartitionId,
    ctx: &RequestCtx<'_>,
) -> Result<Vec<InjectedField>, SpiError> {
    fields
        .iter()
        .map(|field| {
            let value = match &field.value {
                InjectedValue::PartitionId => Value::String(partition.as_str().to_owned()),
                InjectedValue::Constant(constant) => constant.clone(),
                InjectedValue::FromPrincipal(attr) => ctx
                    .principal()
                    .attr(attr)
                    .map(|v| Value::String(v.to_owned()))
                    .ok_or_else(|| SpiError::PrincipalAttrMissing { attr: attr.clone() })?,
            };
            Ok(InjectedField::new(
                field.name.clone(),
                InjectedValue::Constant(value),
            ))
        })
        .collect()
}
