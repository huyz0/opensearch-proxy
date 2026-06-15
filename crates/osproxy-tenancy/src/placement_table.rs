//! An in-memory, epoch-versioned placement table.
//!
//! Maps each partition to its current [`Placement`] and stamps every change
//! with a fresh, monotonically increasing [`Epoch`]. The epoch is a logical
//! generation counter (no wall-clock — keeps the table deterministic, `docs/12`)
//! that flows onto writes so the sink can reject a stale-epoch write during a
//! migration (`docs/06` §2).
//!
//! This is the M1 backend: a process-local table seeded by the operator. The
//! fleet-wide watched store (etcd/Consul/…) arrives in M7 behind the same
//! lookup shape (`docs/11`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use osproxy_core::{Epoch, PartitionId};
use osproxy_spi::{MigrationPhase, Placement, PlacementAt};

use crate::migration::{MigrationError, PartitionState, Phase, WriteAdmission};

/// Maps a partition's internal state to the shape-only [`MigrationPhase`] label
/// surfaced through [`PlacementAt`] for observability (`docs/06` §5).
fn migration_phase(state: &PartitionState) -> MigrationPhase {
    match state {
        PartitionState::Active(_) => MigrationPhase::Settled,
        PartitionState::Migrating {
            phase: Phase::Draining,
            ..
        } => MigrationPhase::Draining,
        PartitionState::Migrating {
            phase: Phase::Cutover,
            ..
        } => MigrationPhase::Cutover,
    }
}

/// One partition's migration state plus the epoch it was last stamped at. Every
/// `set`/transition advances the epoch, so a stamped decision can be recognized
/// as resolved against a superseded generation (`docs/06` §2).
#[derive(Clone, Debug)]
struct Entry {
    state: PartitionState,
    epoch: Epoch,
}

/// A concurrent, epoch-versioned map from partition to placement, carrying each
/// partition's migration state machine (`docs/06`).
///
/// Cloneable handles are not provided here; wrap in an `Arc` to share. All
/// methods are non-blocking beyond a short critical section. Transitions are
/// total: an inapplicable transition returns a [`MigrationError`] and leaves the
/// table unchanged.
#[derive(Debug)]
pub struct PlacementTable {
    // A read-mostly map: lookups vastly outnumber migrations. `RwLock` lets
    // concurrent routing reads proceed in parallel.
    entries: RwLock<HashMap<PartitionId, Entry>>,
    // The generation counter. Every `set`/transition pre-increments it, so the
    // first placement gets epoch 1 and `Epoch::ZERO` always means "never placed".
    generation: AtomicU64,
}

impl PlacementTable {
    /// Creates an empty table at generation zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    /// Registers (or replaces) the placement for `partition` as `Active`,
    /// stamping a fresh epoch and returning it. Initial registration; an
    /// in-flight migration uses the phase transitions below, not `set`.
    pub fn set(&self, partition: PartitionId, placement: Placement) -> Epoch {
        let epoch = self.next_epoch();
        self.write_lock().insert(
            partition,
            Entry::new(PartitionState::Active(placement), epoch),
        );
        epoch
    }

    /// Begins migrating `partition` to `to`: `Active(from)` → `Migrating`
    /// `Draining`. Writes still go to `from`; the epoch advances.
    ///
    /// # Errors
    /// [`MigrationError::AlreadyMigrating`] if a migration is already in flight,
    /// [`MigrationError::UnknownPartition`] if the partition has no placement.
    pub fn begin_migration(
        &self,
        partition: &PartitionId,
        to: Placement,
    ) -> Result<Epoch, MigrationError> {
        self.transition(partition, |state| match state {
            PartitionState::Active(from) => Ok(PartitionState::Migrating {
                from,
                to,
                phase: Phase::Draining,
            }),
            PartitionState::Migrating { .. } => Err(MigrationError::AlreadyMigrating),
        })
    }

    /// Enters the cutover window: `Draining` → `Cutover`. Writes are now rejected
    /// until [`complete_migration`](Self::complete_migration) flips the pointer.
    ///
    /// # Errors
    /// [`MigrationError::NotMigrating`] if settled, [`MigrationError::NotDraining`]
    /// if already past draining.
    pub fn enter_cutover(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        self.transition(partition, |state| match state {
            PartitionState::Migrating {
                from,
                to,
                phase: Phase::Draining,
            } => Ok(PartitionState::Migrating {
                from,
                to,
                phase: Phase::Cutover,
            }),
            PartitionState::Migrating { .. } => Err(MigrationError::NotDraining),
            PartitionState::Active(_) => Err(MigrationError::NotMigrating),
        })
    }

    /// Completes the migration — the pointer flip: `Cutover` → `Active(to)`.
    ///
    /// # Errors
    /// [`MigrationError::NotMigrating`] if settled, [`MigrationError::NotCutover`]
    /// if not yet in cutover.
    pub fn complete_migration(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        self.transition(partition, |state| match state {
            PartitionState::Migrating {
                to,
                phase: Phase::Cutover,
                ..
            } => Ok(PartitionState::Active(to)),
            PartitionState::Migrating { .. } => Err(MigrationError::NotCutover),
            PartitionState::Active(_) => Err(MigrationError::NotMigrating),
        })
    }

    /// Aborts an in-flight migration, returning the partition to `Active(from)`.
    /// Since writes never committed to `to` (Draining wrote to `from`, Cutover
    /// rejected), no rollback of data is needed (INV-M3).
    ///
    /// # Errors
    /// [`MigrationError::NotMigrating`] if the partition is settled.
    pub fn abort_migration(&self, partition: &PartitionId) -> Result<Epoch, MigrationError> {
        self.transition(partition, |state| match state {
            PartitionState::Migrating { from, .. } => Ok(PartitionState::Active(from)),
            PartitionState::Active(_) => Err(MigrationError::NotMigrating),
        })
    }

    /// The current migration state and the epoch it was stamped at, or `None`.
    /// For observability and the control plane (`docs/06` §5).
    #[must_use]
    pub fn state(&self, partition: &PartitionId) -> Option<(PartitionState, Epoch)> {
        self.read_lock()
            .get(partition)
            .map(|e| (e.state.clone(), e.epoch))
    }

    /// Resolves the placement reads go to (and its epoch), or `None`. The single
    /// read placement — `from` until a migration completes — so a read never
    /// sees a split view (INV-M4). The routing entry point.
    #[must_use]
    pub fn get(&self, partition: &PartitionId) -> Option<PlacementAt> {
        self.read_lock().get(partition).map(|e| {
            PlacementAt::new(e.state.read_placement().clone(), e.epoch)
                .with_phase(migration_phase(&e.state))
        })
    }

    /// The migration write gate (`docs/06` §2): may a write resolved at `epoch`
    /// for `partition` commit now? [`WriteAdmission::Admit`] only if writes are
    /// currently allowed (not in the `Cutover` window) *and* the partition's
    /// epoch is unchanged since the decision was resolved — otherwise
    /// [`WriteAdmission::Reject`], which the caller surfaces as a retryable
    /// stale-epoch error so the client re-resolves and retries.
    ///
    /// Epoch equality is the per-partition staleness check (`epoch` only advances
    /// on *this* partition's transitions), and the cutover gate handles the one
    /// window where a write resolved at the *current* epoch must still be held:
    /// together they give INV-M1 (no write in cutover) and INV-M2 (no write
    /// against a superseded placement after the flip).
    #[must_use]
    pub fn admit_write(&self, partition: &PartitionId, epoch: Epoch) -> WriteAdmission {
        let admit = self
            .read_lock()
            .get(partition)
            .is_some_and(|e| e.state.write_placement().is_some() && e.epoch == epoch);
        if admit {
            WriteAdmission::Admit
        } else {
            WriteAdmission::Reject
        }
    }

    /// The current generation of the table (the epoch the most recent change
    /// produced, or [`Epoch::ZERO`] if empty).
    #[must_use]
    pub fn current_epoch(&self) -> Epoch {
        Epoch::new(self.generation.load(Ordering::SeqCst))
    }

    /// Allocates the next monotonic epoch (generation counter pre-increment).
    fn next_epoch(&self) -> Epoch {
        Epoch::new(self.generation.fetch_add(1, Ordering::SeqCst) + 1)
    }

    /// Applies a state transition under the write lock: `f` maps the current
    /// state to the next one (or a [`MigrationError`]). On success the entry is
    /// replaced and stamped with a fresh epoch; on any error the table is
    /// untouched (transitions are atomic and side-effect-free on failure).
    fn transition(
        &self,
        partition: &PartitionId,
        f: impl FnOnce(PartitionState) -> Result<PartitionState, MigrationError>,
    ) -> Result<Epoch, MigrationError> {
        let mut entries = self.write_lock();
        let current = entries
            .get(partition)
            .ok_or(MigrationError::UnknownPartition)?;
        let next = f(current.state.clone())?;
        let epoch = self.next_epoch();
        entries.insert(partition.clone(), Entry::new(next, epoch));
        Ok(epoch)
    }

    /// Acquires the read lock, recovering from a poisoned lock.
    ///
    /// A poisoned lock means a writer panicked mid-update. The stored data is a
    /// plain map (no broken invariant a panic could leave torn), so recovering
    /// the guard is safe and keeps routing available — far better than
    /// propagating a panic onto every request path (NFR-R1).
    fn read_lock(&self) -> std::sync::RwLockReadGuard<'_, HashMap<PartitionId, Entry>> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Acquires the write lock, recovering from a poisoned lock (see
    /// [`PlacementTable::read_lock`]).
    fn write_lock(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<PartitionId, Entry>> {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Entry {
    fn new(state: PartitionState, epoch: Epoch) -> Self {
        Self { state, epoch }
    }
}

impl Default for PlacementTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{ClusterId, IndexName};

    fn shared(cluster: &str, index: &str) -> Placement {
        Placement::SharedIndex {
            cluster: ClusterId::from(cluster),
            index: IndexName::from(index),
            inject: Vec::new(),
        }
    }

    #[test]
    fn missing_partition_resolves_to_none() {
        let table = PlacementTable::new();
        assert!(table.get(&PartitionId::from("absent")).is_none());
        assert_eq!(table.current_epoch(), Epoch::ZERO);
    }

    #[test]
    fn set_assigns_monotonic_epochs() {
        let table = PlacementTable::new();
        let e1 = table.set(PartitionId::from("a"), shared("c", "i"));
        let e2 = table.set(PartitionId::from("b"), shared("c", "i"));
        assert_eq!(e1, Epoch::new(1));
        assert_eq!(e2, Epoch::new(2));
        assert!(e2 > e1);
        assert_eq!(table.current_epoch(), e2);
    }

    #[test]
    fn migration_replaces_placement_and_advances_epoch() {
        let table = PlacementTable::new();
        let p = PartitionId::from("t");
        table.set(p.clone(), shared("old", "i"));
        let before = table.get(&p).unwrap();
        assert_eq!(before.placement.cluster().as_str(), "old");

        let migrated = table.set(p.clone(), shared("new", "i"));
        let after = table.get(&p).unwrap();
        assert_eq!(after.placement.cluster().as_str(), "new");
        assert_eq!(after.epoch, migrated);
        assert!(after.epoch > before.epoch);
    }
}
