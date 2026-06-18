//! The high-level tenancy contract — what most implementers provide.

use osproxy_core::{ClusterId, Epoch, PartitionId};

use crate::error::SpiError;
use crate::placement::PlacementAt;
use crate::request::RequestCtx;
use crate::rules::{DocIdRule, InjectedField, SensitivitySpec};

/// The tenancy-focused contract most implementers provide.
///
/// It declares tenancy *rules* — how to find the partition, how to build the
/// document `_id`, which fields to inject, which are sensitive — plus a
/// placement lookup. `osproxy-tenancy` turns this into a [`crate::RoutingSpi`],
/// so tenancy implementers never touch [`crate::RouteDecision`] plumbing
/// (`docs/02` §2).
///
/// # Invariants
///
/// - [`TenancySpi::resolve_partition`] MUST yield a partition id for every
///   routable request, or it returns [`SpiError::PartitionUnresolved`] and the
///   request is rejected.
/// - In `SharedIndex` mode the partition id MUST be part of the constructed
///   `_id` to prevent cross-tenant id collisions (`docs/03`); the adapter
///   enforces this.
/// - [`TenancySpi::injected_fields`] names and [`TenancySpi::sensitive_fields`]
///   MUST be stable for a given logical-index version, so the read-path
///   strip/filter stays symmetric with the write-path inject.
///
/// # Examples
///
/// ```
/// use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
/// use osproxy_spi::{
///     InjectedField, InjectedValue, Placement, PlacementAt, PartitionKeySpecKind,
///     RequestCtx, SensitivitySpec, SpiError, TenancySpi,
/// };
/// use serde_json::Value;
///
/// struct OneTenantPerHeader;
///
/// impl TenancySpi for OneTenantPerHeader {
///     fn resolve_partition(&self, ctx: &RequestCtx<'_>, _doc: Option<&Value>)
///         -> Result<PartitionId, SpiError>
///     {
///         // Real impls usually defer to `osproxy_tenancy::resolve_partition_spec`;
///         // here we resolve inline to keep the SPI crate self-contained.
///         ctx.headers().get("x-tenant").map(PartitionId::from).ok_or(
///             SpiError::PartitionUnresolved { tried: vec![PartitionKeySpecKind::Header] })
///     }
///     fn doc_id_rule(&self) -> Option<osproxy_spi::DocIdRule> { None }
///     fn injected_fields(&self) -> Vec<InjectedField> {
///         vec![InjectedField::new(FieldName::from("_tenant"), InjectedValue::PartitionId)]
///     }
///     fn sensitive_fields(&self) -> SensitivitySpec { SensitivitySpec::none() }
///     async fn placement_for(&self, p: &PartitionId) -> Result<PlacementAt, SpiError> {
///         Ok(PlacementAt::new(
///             Placement::SharedIndex {
///                 cluster: ClusterId::from("eu-1"),
///                 index: IndexName::from("logs-shared"),
///                 inject: self.injected_fields(),
///             },
///             Epoch::ZERO,
///         ))
///     }
/// }
/// ```
#[allow(
    async_fn_in_trait,
    reason = "consumed through generics in osproxy-tenancy's adapter; Send is \
              checked at the engine's spawn site (docs/02 §2)"
)]
pub trait TenancySpi: Send + Sync + 'static {
    /// Resolves the partition id for a request.
    ///
    /// `doc` is the request body parsed as JSON (or `None` if absent / not JSON);
    /// the adapter parses it once and shares it across a bulk request's items.
    ///
    /// Most implementations just defer to the declarative resolver
    /// `osproxy_tenancy::resolve_partition_spec`, naming the source(s) the
    /// partition id lives in (a body field, a header, a principal attribute):
    ///
    /// ```ignore
    /// fn resolve_partition(&self, ctx: &RequestCtx<'_>, doc: Option<&Value>)
    ///     -> Result<PartitionId, SpiError>
    /// {
    ///     osproxy_tenancy::resolve_partition_spec(
    ///         &PartitionKeySpec::BodyField(JsonPath::new("tenant_id")), ctx, doc)
    /// }
    /// ```
    ///
    /// Override the body freely for cases the declarative sources cannot express
    /// — decoding an encoded or signed header, parsing a structured token,
    /// combining several inputs — and fall back to the declarative resolver if you
    /// like. You choose the order; nothing is tried implicitly before you.
    ///
    /// # Errors
    ///
    /// Returns [`SpiError::PartitionUnresolved`] when no configured source yields a
    /// partition id; the request is then rejected.
    ///
    /// The no-value-leak rule holds (NFR-S2): whatever you decode here must not be
    /// logged. The id you return is treated as a partition id (an opaque routing
    /// key), never as a tenant *value* to capture.
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        doc: Option<&serde_json::Value>,
    ) -> Result<PartitionId, SpiError>;

    /// Optional rule to construct the document `_id` (and `_routing`).
    fn doc_id_rule(&self) -> Option<DocIdRule>;

    /// Fields injected on ingest and stripped on read. The field *names* are
    /// chosen here (the SPI decides them).
    fn injected_fields(&self) -> Vec<InjectedField>;

    /// Declares which field *values* observability may capture, driving
    /// value-suppression (NFR-S2). Deny-by-default: the standard implementation
    /// returns [`SensitivitySpec::all_sensitive`] (everything redacted) and
    /// allow-lists known-safe fields with [`SensitivitySpec::allowing`]. The
    /// default here is `all_sensitive`, so a tenancy that does not override it
    /// leaks nothing.
    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::all_sensitive()
    }

    /// Resolves a partition to its current placement and the epoch it was read
    /// at. NOT a pure function — migration mutates the placement state.
    ///
    /// # Errors
    ///
    /// Returns [`SpiError::PlacementMissing`] when the partition has no
    /// placement, or [`SpiError::PlacementBackend`] when the lookup backend is
    /// unavailable.
    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError>;

    /// The migration write gate (`docs/06` §2): may a write that resolved at
    /// `epoch` for `partition` still commit? Re-checked at dispatch, after the
    /// decision was stamped, so a placement that advanced (or entered cutover) in
    /// the meantime is caught. `false` means reject as a retryable stale-epoch
    /// error; the client re-resolves against the new placement.
    ///
    /// Defaults to always-admit: an implementation without live migration (a
    /// constant placement) never needs to hold a write.
    async fn admit_write(&self, _partition: &PartitionId, _epoch: Epoch) -> bool {
        true
    }

    /// The base URL of a cluster, by id. The data plane carries each cluster's
    /// endpoint on the placement result, but the cursor-affinity and admin
    /// pass-through paths route to a cluster by id with no placement to consult,
    /// so they resolve the endpoint through this lookup. Return `None` for an
    /// unknown cluster; the request then fails closed rather than route blind.
    ///
    /// Default `None`. A tenancy that runs cursor affinity or admin pass-through
    /// against `OpenSearchSink` must implement it for the clusters those paths
    /// reach (which is just its own cluster catalog by id).
    fn cluster_endpoint(&self, _cluster: &ClusterId) -> Option<String> {
        None
    }
}
