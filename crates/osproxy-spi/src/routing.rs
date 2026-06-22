//! The low-level routing contract.

use crate::decision::RouteDecision;
use crate::error::SpiError;
use crate::request::RequestCtx;

/// Decides where and how a single request is routed.
///
/// This is the low-level contract for a single routing *decision*: full control
/// over the destination and the transforms. Most implementers instead provide a
/// [`crate::TenancySpi`], which `osproxy-tenancy` adapts into a `RoutingSpi`.
///
/// Note this yields only a [`RouteDecision`]. The engine pipeline needs more than
/// a decision (the resolved partition, epoch, and migration phase, to construct
/// ids, demux bulk, and gate writes), so it is generic over the richer
/// `osproxy_tenancy::Router` seam rather than this trait. Implement `Router` to
/// drive the engine with custom routing; implement `RoutingSpi` where only a
/// `RouteDecision` is required.
///
/// # Invariants
///
/// - MUST resolve to exactly one [`Target`](osproxy_core::Target), no
///   synchronous fan-out in v1 (ADR-002).
/// - MUST NOT panic; return [`SpiError`] for every failure (NFR-R1).
/// - The returned [`RouteDecision::epoch`] MUST come from the placement state
///   the decision was derived from, so the sink can detect a stale-epoch write
///   during a migration (`docs/06` §2).
///
/// The engine drives implementations through generics (monomorphized, no dyn
/// dispatch on the hot path), so the future's `Send`-ness is checked at the
/// spawn site.
///
/// # Examples
///
/// ```
/// use osproxy_core::{ClusterId, Epoch, IndexName, Target};
/// use osproxy_spi::{Protocol, RequestCtx, RouteDecision, RoutingSpi, SpiError};
///
/// struct PinToOne;
///
/// impl RoutingSpi for PinToOne {
///     async fn route(&self, _ctx: &RequestCtx<'_>) -> Result<RouteDecision, SpiError> {
///         let target = Target::new(ClusterId::from("only"), IndexName::from("logs"));
///         Ok(RouteDecision::passthrough(target, Protocol::Http1, Epoch::ZERO))
///     }
/// }
/// ```
#[allow(
    async_fn_in_trait,
    reason = "implementations are consumed through generics in the engine, where \
              Send is verified at the spawn site; no public dyn boundary needs an \
              explicit Send bound in M1 (docs/02 §1)"
)]
pub trait RoutingSpi: Send + Sync + 'static {
    /// Resolves the routing decision for an authenticated request.
    ///
    /// # Errors
    ///
    /// Returns [`SpiError`] when the partition cannot be resolved, no placement
    /// exists, the placement backend is unavailable, or the endpoint is
    /// unsupported.
    async fn route(&self, ctx: &RequestCtx<'_>) -> Result<RouteDecision, SpiError>;
}
