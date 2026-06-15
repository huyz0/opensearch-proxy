//! Control plane.
//!
//! The operator/automation-driven side of the proxy (`docs/06` §5): it owns the
//! **migration state transitions** and the fleet-safe protocol that flips a
//! partition's placement without a window where any instance writes to the wrong
//! cluster. It does not handle request traffic.
//!
//! Proxy instances poll the shared placement backend *fresh on every request*
//! (no cached migration decision), so the backend is the single synchronized
//! source of truth. The [`ControlPlane`] drives migrations through that backend
//! (the [`MigrationStore`] seam) and holds a **drain barrier** between cutover
//! and completion so in-flight writes cannot land after the flip.
//!
//! The in-memory backend is the M1
//! [`PlacementTable`](osproxy_tenancy::PlacementTable); distributed watched
//! stores (etcd/Consul/Redis/OS index) implement the same [`MigrationStore`]
//! contract in M7 without changing the control protocol.
//!
//! It also owns [`CursorAffinity`] — the bounded, TTL'd `cursor_id -> cluster`
//! map that pins scroll/PIT follow-ups to their creating cluster (`docs/03` §6).
#![deny(missing_docs)]

mod affinity;
mod migration;

pub use affinity::{Affinity, CursorAffinity, DEFAULT_CAPACITY, DEFAULT_CURSOR_TTL};
pub use migration::{ControlError, ControlPlane, MigrationStore, DEFAULT_DRAIN_BARRIER};
