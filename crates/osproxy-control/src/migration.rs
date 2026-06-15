//! The fleet-safe migration control plane (`docs/06` §5).
//!
//! The proxy runs as **many instances**, each resolving placement and the write
//! gate by polling the shared backend *fresh on every request* — nothing about a
//! migration is cached in an instance, so the backend is the single synchronized
//! source of truth (the in-memory [`PlacementTable`] here; a watched store such
//! as etcd/Consul in M7, behind [`MigrationStore`]).
//!
//! That makes the routing flip safe *except* for one residual window: a write
//! whose gate passed an instant **before** cutover was published may still be
//! committing upstream. So the controller does not flip the pointer immediately:
//! after publishing `Cutover` it holds a **drain barrier** — at least
//! [`DEFAULT_DRAIN_BARRIER`] (≥ the upstream write timeout) — before
//! `complete_migration` is allowed. By then every pre-cutover write has either
//! committed or hit its deadline, and no in-flight write can land in the old
//! placement after the flip (INV-M1, INV-M2 fleet-wide).
//!
//! Time comes from an injected [`Clock`], so the barrier is deterministic in
//! tests. One controller drives a given partition's migration (`docs/06` §5:
//! operator/automation-driven, never AI-mutated).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use osproxy_core::{Clock, Epoch, Instant, PartitionId, SystemClock};
use osproxy_spi::Placement;
use osproxy_tenancy::{MigrationError, PartitionState, Phase, PlacementTable};
use thiserror::Error;

/// The default drain barrier: how long the controller holds after publishing
/// `Cutover` before completing, so in-flight pre-cutover writes drain. Should be
/// ≥ the sink's upstream write timeout (30s, NFR-R7); set higher for safety.
pub const DEFAULT_DRAIN_BARRIER: Duration = Duration::from_secs(30);

/// The backend that holds and transitions the fleet's placement state — the seam
/// the proxy instances poll for reads and the controller drives for migration.
///
/// Implemented in-process by [`PlacementTable`] (and `Arc<PlacementTable>`); a
/// distributed watched store (etcd/Consul/Redis/OS index) implements the same
/// contract in M7 without changing the control protocol above it.
pub trait MigrationStore {
    /// Begins migrating `partition` toward `to` (`Active` → `Draining`).
    ///
    /// # Errors
    /// [`MigrationError`] if the partition is unknown or already migrating.
    fn begin_migration(
        &self,
        partition: &PartitionId,
        to: Placement,
    ) -> Result<Epoch, MigrationError>;

    /// Moves an in-flight migration into the cutover window (`Draining` →
    /// `Cutover`); writes are now rejected fleet-wide.
    ///
    /// # Errors
    /// [`MigrationError`] if the partition is not draining.
    fn enter_cutover(&self, partition: &PartitionId) -> Result<Epoch, MigrationError>;

    /// Completes the migration — the pointer flip (`Cutover` → `Active(to)`).
    ///
    /// # Errors
    /// [`MigrationError`] if the partition is not in cutover.
    fn complete_migration(&self, partition: &PartitionId) -> Result<Epoch, MigrationError>;

    /// Aborts an in-flight migration, returning it to its origin.
    ///
    /// # Errors
    /// [`MigrationError`] if the partition is not migrating.
    fn abort_migration(&self, partition: &PartitionId) -> Result<Epoch, MigrationError>;

    /// The partition's current migration state and stamped epoch, or `None`.
    fn state(&self, partition: &PartitionId) -> Option<(PartitionState, Epoch)>;
}

impl MigrationStore for PlacementTable {
    fn begin_migration(
        &self,
        partition: &PartitionId,
        to: Placement,
    ) -> Result<Epoch, MigrationError> {
        PlacementTable::begin_migration(self, partition, to)
    }
    fn enter_cutover(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        PlacementTable::enter_cutover(self, partition)
    }
    fn complete_migration(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        PlacementTable::complete_migration(self, partition)
    }
    fn abort_migration(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        PlacementTable::abort_migration(self, partition)
    }
    fn state(&self, partition: &PartitionId) -> Option<(PartitionState, Epoch)> {
        PlacementTable::state(self, partition)
    }
}

impl<T: MigrationStore + ?Sized> MigrationStore for Arc<T> {
    fn begin_migration(
        &self,
        partition: &PartitionId,
        to: Placement,
    ) -> Result<Epoch, MigrationError> {
        (**self).begin_migration(partition, to)
    }
    fn enter_cutover(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        (**self).enter_cutover(partition)
    }
    fn complete_migration(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        (**self).complete_migration(partition)
    }
    fn abort_migration(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        (**self).abort_migration(partition)
    }
    fn state(&self, partition: &PartitionId) -> Option<(PartitionState, Epoch)> {
        (**self).state(partition)
    }
}

/// Why a control-plane operation was refused.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum ControlError {
    /// The underlying state transition does not apply (wrong phase, unknown
    /// partition, …).
    #[error("transition refused: {0}")]
    Transition(#[from] MigrationError),

    /// `complete_migration` was called before the drain barrier elapsed; the
    /// controller must wait `remaining` longer so in-flight pre-cutover writes
    /// drain before the pointer flips.
    #[error("drain barrier not elapsed; wait {remaining:?} longer")]
    BarrierPending {
        /// How much of the barrier is left.
        remaining: Duration,
    },
}

/// Drives a partition through its migration phases against a [`MigrationStore`],
/// enforcing the drain barrier between cutover and completion (`docs/06` §5).
pub struct ControlPlane<S> {
    store: S,
    clock: Arc<dyn Clock>,
    barrier: Duration,
    /// When each partition entered cutover, to time the drain barrier.
    cutover_at: Mutex<HashMap<PartitionId, Instant>>,
}

impl<S: std::fmt::Debug> std::fmt::Debug for ControlPlane<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected `Clock` is not `Debug`; the rest is the useful shape.
        f.debug_struct("ControlPlane")
            .field("store", &self.store)
            .field("barrier", &self.barrier)
            .field("cutover_at", &self.cutover_at)
            .finish_non_exhaustive()
    }
}

impl<S: MigrationStore> ControlPlane<S> {
    /// Builds a controller over `store` with the default drain barrier and the
    /// system clock.
    #[must_use]
    pub fn new(store: S) -> Self {
        Self {
            store,
            clock: Arc::new(SystemClock),
            barrier: DEFAULT_DRAIN_BARRIER,
            cutover_at: Mutex::new(HashMap::new()),
        }
    }

    /// Sets the drain barrier (builder style).
    #[must_use]
    pub fn with_barrier(mut self, barrier: Duration) -> Self {
        self.barrier = barrier;
        self
    }

    /// Swaps the clock the barrier reads (tests inject a `ManualClock`).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Begins migrating `partition` toward `to`. Writes keep flowing to the
    /// origin during the ensuing drain phase.
    ///
    /// # Errors
    /// [`ControlError::Transition`] if the partition is unknown or migrating.
    pub fn begin_migration(
        &self,
        partition: &PartitionId,
        to: Placement,
    ) -> Result<Epoch, ControlError> {
        Ok(self.store.begin_migration(partition, to)?)
    }

    /// Enters the cutover window and starts the drain barrier clock. Writes are
    /// now rejected fleet-wide (every instance polls this fresh).
    ///
    /// # Errors
    /// [`ControlError::Transition`] if the partition is not draining.
    pub fn enter_cutover(&self, partition: &PartitionId) -> Result<Epoch, ControlError> {
        let epoch = self.store.enter_cutover(partition)?;
        self.lock().insert(partition.clone(), self.clock.now());
        Ok(epoch)
    }

    /// Completes the migration once the drain barrier has elapsed since cutover —
    /// the pointer flip. Refused (without mutating the store) while in-flight
    /// pre-cutover writes might still be committing.
    ///
    /// # Errors
    /// [`ControlError::BarrierPending`] if the barrier has not elapsed;
    /// [`ControlError::Transition`] if the partition is not in cutover.
    pub fn complete_migration(&self, partition: &PartitionId) -> Result<Epoch, ControlError> {
        let now = self.clock.now();
        let in_cutover = matches!(
            self.store.state(partition),
            Some((
                PartitionState::Migrating {
                    phase: Phase::Cutover,
                    ..
                },
                _
            ))
        );
        if in_cutover {
            // Start the barrier now if this controller did not record cutover
            // (errs toward waiting rather than flipping early).
            let started = *self.lock().entry(partition.clone()).or_insert(now);
            let elapsed = now.saturating_duration_since(started);
            if elapsed < self.barrier {
                return Err(ControlError::BarrierPending {
                    remaining: self.barrier.saturating_sub(elapsed),
                });
            }
        }
        let epoch = self.store.complete_migration(partition)?;
        self.lock().remove(partition);
        Ok(epoch)
    }

    /// Aborts an in-flight migration, returning the partition to its origin and
    /// clearing any pending barrier.
    ///
    /// # Errors
    /// [`ControlError::Transition`] if the partition is not migrating.
    pub fn abort_migration(&self, partition: &PartitionId) -> Result<Epoch, ControlError> {
        let epoch = self.store.abort_migration(partition)?;
        self.lock().remove(partition);
        Ok(epoch)
    }

    /// The partition's current migration state and epoch, or `None`. For
    /// operator/observability read-out (`docs/06` §5).
    #[must_use]
    pub fn state(&self, partition: &PartitionId) -> Option<(PartitionState, Epoch)> {
        self.store.state(partition)
    }

    /// Locks the cutover-time map, recovering a poisoned lock — it is plain
    /// timing data with no invariant a panicking holder could tear (NFR-R1).
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PartitionId, Instant>> {
        self.cutover_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
