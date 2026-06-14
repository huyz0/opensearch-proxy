//! The partition migration state machine (`docs/06`).
//!
//! A partition is either settled at one [`Placement`] (`Active`) or moving
//! between two (`Migrating`). The proxy never copies data — an external tool
//! does — so migration here is a *pointer flip guarded by phases*, designed so
//! the only window that rejects writes is the brief `Cutover`, and reads always
//! resolve to a single placement (never a split view).
//!
//! The two destination queries are the heart of the correctness argument:
//! - [`PartitionState::read_placement`] is always exactly one placement (INV-M4).
//! - [`PartitionState::write_placement`] is `None` only during `Cutover`, the
//!   one window where a write must be rejected and retried (INV-M1).

use osproxy_spi::Placement;
use thiserror::Error;

/// The phase of an in-flight migration (`docs/06` §3).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    /// Data is being copied `from -> to`; writes still go to `from` normally.
    Draining,
    /// The brief cutover: writes are rejected (stale-epoch retry) until the
    /// pointer flips to `to`.
    Cutover,
}

/// A partition's placement state: settled, or migrating between two placements.
///
/// Not `#[non_exhaustive]`: routing must interpret every state, so adding one
/// should force every match to be revisited (`docs/03`).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PartitionState {
    /// Settled at a single placement.
    Active(Placement),
    /// Moving from one placement to another; the phase gates writes.
    Migrating {
        /// Where the partition lives now (reads and Draining writes go here).
        from: Placement,
        /// Where the partition is moving to (live only after the flip).
        to: Placement,
        /// The current phase.
        phase: Phase,
    },
}

impl PartitionState {
    /// The single placement reads resolve to right now — `from` until the
    /// migration completes, never a split of both (INV-M4).
    #[must_use]
    pub fn read_placement(&self) -> &Placement {
        match self {
            Self::Active(p) | Self::Migrating { from: p, .. } => p,
        }
    }

    /// The placement a write may commit to right now, or `None` if writes are
    /// currently blocked — the `Cutover` window (INV-M1).
    #[must_use]
    pub fn write_placement(&self) -> Option<&Placement> {
        match self {
            Self::Active(p)
            | Self::Migrating {
                from: p,
                phase: Phase::Draining,
                ..
            } => Some(p),
            Self::Migrating {
                phase: Phase::Cutover,
                ..
            } => None,
        }
    }

    /// Whether a migration is in flight.
    #[must_use]
    pub fn is_migrating(&self) -> bool {
        matches!(self, Self::Migrating { .. })
    }
}

/// Whether a write resolved against a past placement may still commit.
///
/// The authoritative check is *destination match*, not epoch arithmetic: a write
/// commits only if its resolved placement equals the partition's current write
/// destination and writes are not blocked. This makes the migration invariants
/// hold regardless of how stale the in-flight decision is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WriteAdmission {
    /// The write may commit: its target is the current write destination.
    Admit,
    /// The write must be rejected and retried (stale-epoch / cutover): the
    /// destination changed or writes are blocked. Maps to a retryable error.
    Reject,
}

/// Why a migration state transition was refused: the transition does not apply
/// to the partition's current state. Transitions are total and side-effect-free
/// on failure, so a refused transition leaves the table unchanged.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum MigrationError {
    /// The partition has no placement to transition.
    #[error("partition has no placement")]
    UnknownPartition,
    /// `begin_migration` requires a settled (`Active`) partition.
    #[error("partition is already migrating")]
    AlreadyMigrating,
    /// `enter_cutover`/`complete`/`abort` require an in-flight migration.
    #[error("partition is not migrating")]
    NotMigrating,
    /// `enter_cutover` requires the `Draining` phase.
    #[error("migration is not draining")]
    NotDraining,
    /// `complete_migration` requires the `Cutover` phase.
    #[error("migration is not in cutover")]
    NotCutover,
}
