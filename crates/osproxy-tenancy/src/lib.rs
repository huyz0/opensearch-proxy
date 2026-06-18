//! High-level tenancy layer.
//!
//! Translates declarative tenancy rules (partition key, doc-id construction,
//! injected/sensitive fields, placement lookup) into low-level routing
//! decisions, so most implementers never touch [`RouteDecision`] plumbing
//! (`docs/02`, `docs/03`).
//!
//! Two pieces:
//!
//! - [`PlacementTable`] — the in-memory, epoch-versioned partition→placement map
//!   that backs an implementer's `placement_for` lookup (M1; fleet store in M7).
//! - [`TenancyRouter`] — adapts a [`TenancySpi`] into a
//!   [`RoutingSpi`](osproxy_spi::RoutingSpi), resolving the partition, looking up
//!   placement, and assembling the body transform.
//!
//! [`RouteDecision`]: osproxy_spi::RouteDecision
//! [`TenancySpi`]: osproxy_spi::TenancySpi
#![deny(missing_docs)]

mod migration;
mod placement_table;
mod resolve;
mod router;

pub use migration::{MigrationError, PartitionState, Phase, WriteAdmission};
pub use placement_table::PlacementTable;
pub use resolve::resolve_partition_spec;
pub use router::{Resolved, Router, TenancyRouter};
