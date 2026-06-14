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
use osproxy_spi::{Placement, PlacementAt};

/// A concurrent, epoch-versioned map from partition to placement.
///
/// Cloneable handles are not provided here; wrap in an `Arc` to share. All
/// methods are non-blocking beyond a short critical section.
#[derive(Debug)]
pub struct PlacementTable {
    // A read-mostly map: lookups vastly outnumber migrations. `RwLock` lets
    // concurrent routing reads proceed in parallel.
    entries: RwLock<HashMap<PartitionId, PlacementAt>>,
    // The generation counter. `set` pre-increments it, so the first placement
    // gets epoch 1 and `Epoch::ZERO` always means "never placed".
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

    /// Sets (registers or migrates) the placement for `partition`, stamping it
    /// with a fresh epoch, and returns that epoch.
    ///
    /// Used both for initial registration and for migration cutover — they are
    /// the same operation: replace the placement and advance the generation.
    pub fn set(&self, partition: PartitionId, placement: Placement) -> Epoch {
        let epoch = Epoch::new(self.generation.fetch_add(1, Ordering::SeqCst) + 1);
        let mut entries = self.write_lock();
        entries.insert(partition, PlacementAt::new(placement, epoch));
        epoch
    }

    /// Resolves the current placement (and the epoch it was read at) for
    /// `partition`, or `None` if the partition has no placement.
    #[must_use]
    pub fn get(&self, partition: &PartitionId) -> Option<PlacementAt> {
        self.read_lock().get(partition).cloned()
    }

    /// The current generation of the table (the epoch the most recent `set`
    /// produced, or [`Epoch::ZERO`] if empty).
    #[must_use]
    pub fn current_epoch(&self) -> Epoch {
        Epoch::new(self.generation.load(Ordering::SeqCst))
    }

    /// Acquires the read lock, recovering from a poisoned lock.
    ///
    /// A poisoned lock means a writer panicked mid-update. The stored data is a
    /// plain map (no broken invariant a panic could leave torn), so recovering
    /// the guard is safe and keeps routing available — far better than
    /// propagating a panic onto every request path (NFR-R1).
    fn read_lock(&self) -> std::sync::RwLockReadGuard<'_, HashMap<PartitionId, PlacementAt>> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Acquires the write lock, recovering from a poisoned lock (see
    /// [`PlacementTable::read_lock`]).
    fn write_lock(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<PartitionId, PlacementAt>> {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
