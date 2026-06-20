//! Adapts a high-level [`TenancySpi`] into the low-level [`RoutingSpi`].
//!
//! This is where declarative tenancy rules become a concrete routing decision:
//! resolve the partition, look up its placement, derive the physical target,
//! and assemble the body transform (with injected-field values already resolved
//! to constants, so downstream stages stay pure). The `SharedIndex`
//! partition-in-id invariant (`docs/03`) is enforced here.

use osproxy_core::{ClusterId, Epoch, IndexName, PartitionId, Target};
use osproxy_spi::{
    BodyDoc, BodyTransform, InjectedField, InjectedValue, MigrationPhase, Placement, RequestCtx,
    RouteDecision, RoutingSpi, SpiError, TenancySpi,
};
use serde_json::Value;

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
    /// The partition's migration phase at resolve time (shape-only, for
    /// observability — `docs/06` §5).
    pub migration: MigrationPhase,
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

    /// The migration write gate for a resolved decision (`docs/06` §2): whether a
    /// write that resolved at `epoch` for `partition` may still commit. The
    /// engine calls this at dispatch; `false` is surfaced as a retryable
    /// stale-epoch error. Delegates to the [`TenancySpi`].
    pub async fn admit_write(&self, partition: &PartitionId, epoch: Epoch) -> bool {
        self.spi.admit_write(partition, epoch).await
    }

    /// Resolves the full routing plan for `ctx` (the single-document path).
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
        // The body is scanned on demand for the partition key — never parsed into
        // a JSON tree (ADR-014).
        let partition = self.resolve_partition(ctx, BodyDoc::new(ctx.body()))?;
        self.resolve_placement(ctx, partition, ctx.logical_index())
            .await
    }

    /// Resolves just the partition id for a request and document, without a
    /// placement lookup. The per-document entry point for bulk demux (`docs/04`
    /// §3), where each operation carries its own source as a [`BodyDoc`].
    ///
    /// # Errors
    ///
    /// Returns [`SpiError::PartitionUnresolved`] if no configured source yields
    /// the partition.
    pub fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        self.spi.resolve_partition(ctx, body)
    }

    /// Resolves a known partition to its placement and the routing plan for a
    /// given logical index. Separated from [`Self::resolve_partition`] so a bulk
    /// request can resolve the partition per document but cache the placement
    /// per partition.
    ///
    /// # Errors
    ///
    /// Returns [`SpiError`] if no placement exists or the configured transforms
    /// are invalid (e.g. a shared-index id rule that omits the partition).
    pub async fn resolve_placement(
        &self,
        ctx: &RequestCtx<'_>,
        partition: PartitionId,
        logical_index: &str,
    ) -> Result<Resolved, SpiError> {
        let at = self.spi.placement_for(&partition).await?;
        // Carry the cluster's endpoint (from the placement result) onto the
        // target so the sink can pool it — the tenancy is the source of truth for
        // where each cluster lives.
        let target = target_for(&at.placement, logical_index).with_endpoint(at.endpoint.clone());
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
            migration: at.phase,
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
        // In SharedIndex mode the partition id is MANDATORY in the doc-id template
        // (docs/03 §4): by-id reads/writes (`_doc/{id}`) bypass the query filter and
        // hit the physical id directly, so without a partition-scoped id two tenants
        // collide on the same `_id` — a cross-tenant overwrite on write and a
        // cross-tenant read on get. A *missing* rule is as unsafe as a partition-free
        // one, so reject both here rather than only validating a rule that happens to
        // be present.
        if let Placement::SharedIndex { .. } = placement {
            let partition_scoped = id_rule
                .as_ref()
                .is_some_and(|rule| rule.template.references_partition());
            if !partition_scoped {
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

/// The partition-aware routing seam the engine pipeline drives.
///
/// [`RoutingSpi`] yields only a [`RouteDecision`]; the engine needs more — the
/// resolved partition (to construct `_id`/`_routing` and to demux bulk per
/// document), the epoch and migration phase (the write gate), and a split
/// resolve so a bulk request can resolve the partition per document but cache the
/// placement per partition. This trait captures exactly that contract, so the
/// pipeline is generic over *any* router that can provide it, not nailed to the
/// concrete [`TenancyRouter`]. [`TenancyRouter`] is the in-tree implementation.
#[allow(
    async_fn_in_trait,
    reason = "consumed through generics in the engine, where Send is verified at \
              the spawn site, mirroring TenancySpi/RoutingSpi (docs/02 §2)"
)]
pub trait Router: Send + Sync + 'static {
    /// Resolves the full routing plan for a single-document request.
    ///
    /// # Errors
    ///
    /// Returns [`SpiError`] if the endpoint is not tenancy-aware, the partition
    /// cannot be resolved, no placement exists, or the transforms are invalid.
    async fn resolve(&self, ctx: &RequestCtx<'_>) -> Result<Resolved, SpiError>;

    /// Resolves just the partition id for a request and document (the bulk demux
    /// entry point).
    ///
    /// # Errors
    ///
    /// Returns [`SpiError::PartitionUnresolved`] if no source yields a partition.
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError>;

    /// Resolves a known partition to its placement and routing plan.
    ///
    /// # Errors
    ///
    /// Returns [`SpiError`] if no placement exists or the transforms are invalid.
    async fn resolve_placement(
        &self,
        ctx: &RequestCtx<'_>,
        partition: PartitionId,
        logical_index: &str,
    ) -> Result<Resolved, SpiError>;

    /// The migration write gate: may a write that resolved at `epoch` for
    /// `partition` still commit? `false` ⇒ reject as a retryable stale-epoch error.
    async fn admit_write(&self, partition: &PartitionId, epoch: Epoch) -> bool;

    /// The base URL of a cluster by id, for the cursor-affinity and admin paths
    /// that route by cluster without a placement. Default `None`.
    fn cluster_endpoint(&self, _cluster: &ClusterId) -> Option<String> {
        None
    }
}

impl<T: TenancySpi> Router for TenancyRouter<T> {
    async fn resolve(&self, ctx: &RequestCtx<'_>) -> Result<Resolved, SpiError> {
        TenancyRouter::resolve(self, ctx).await
    }

    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        TenancyRouter::resolve_partition(self, ctx, body)
    }

    async fn resolve_placement(
        &self,
        ctx: &RequestCtx<'_>,
        partition: PartitionId,
        logical_index: &str,
    ) -> Result<Resolved, SpiError> {
        TenancyRouter::resolve_placement(self, ctx, partition, logical_index).await
    }

    async fn admit_write(&self, partition: &PartitionId, epoch: Epoch) -> bool {
        TenancyRouter::admit_write(self, partition, epoch).await
    }

    fn cluster_endpoint(&self, cluster: &ClusterId) -> Option<String> {
        self.spi.cluster_endpoint(cluster)
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

/// Resolves the *context-derived* injected values to constants, using the
/// request. The `PartitionId` value is left as-is: it is the read-isolation key,
/// and downstream stages resolve it to the partition, so the read path can tell
/// the isolation field apart from the decorative (context-derived) ones.
fn resolve_inject(
    fields: &[InjectedField],
    _partition: &PartitionId,
    ctx: &RequestCtx<'_>,
) -> Result<Vec<InjectedField>, SpiError> {
    fields
        .iter()
        .map(|field| {
            let value = match &field.value {
                // The isolation field stays symbolic; never filtered on a
                // context-derived value (which would differ on read).
                InjectedValue::PartitionId => return Ok(field.clone()),
                InjectedValue::Constant(constant) => constant.clone(),
                InjectedValue::FromPrincipal(attr) => ctx
                    .principal()
                    .attr(attr)
                    .map(|v| Value::String(v.to_owned()))
                    .ok_or_else(|| SpiError::PrincipalAttrMissing { attr: attr.clone() })?,
                InjectedValue::FromHeader(name) => ctx
                    .headers()
                    .get(name)
                    .map(|v| Value::String(v.to_owned()))
                    .ok_or_else(|| SpiError::HeaderMissing {
                        header: name.clone(),
                    })?,
            };
            Ok(InjectedField::new(
                field.name.clone(),
                InjectedValue::Constant(value),
            ))
        })
        .collect()
}

#[cfg(test)]
#[path = "router_tests.rs"]
mod tests;
