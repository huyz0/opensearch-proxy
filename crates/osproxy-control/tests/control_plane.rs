//! The fleet-safe migration control plane (`docs/06` §5): the drain barrier
//! between cutover and the pointer flip, and the property that every instance,
//! polling the shared backend fresh, never a cache, sees one consistent
//! migration state, so there is no window where two instances disagree on where
//! writes go.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use osproxy_control::{ControlError, ControlPlane};
use osproxy_core::{ClusterId, ManualClock, PartitionId};
use osproxy_spi::Placement;
use osproxy_tenancy::{PartitionState, PlacementTable, WriteAdmission};

const BARRIER: Duration = Duration::from_secs(30);

fn cluster(name: &str) -> Placement {
    Placement::DedicatedCluster {
        cluster: ClusterId::from(name),
    }
}

/// A backend (shared by the whole fleet) with one partition registered at A.
fn backend() -> (Arc<PlacementTable>, PartitionId) {
    let table = Arc::new(PlacementTable::new());
    let p = PartitionId::from("acme");
    table.set(p.clone(), cluster("a"));
    (table, p)
}

#[test]
fn complete_is_held_until_the_drain_barrier_elapses() {
    let (table, p) = backend();
    let clock = Arc::new(ManualClock::new());
    let cp = ControlPlane::new(Arc::clone(&table))
        .with_clock(clock.clone())
        .with_barrier(BARRIER);

    cp.begin_migration(&p, cluster("b")).unwrap();
    cp.enter_cutover(&p).unwrap();

    // Completing immediately is refused, in-flight pre-cutover writes may still
    // be committing, and the store is NOT flipped.
    assert!(matches!(
        cp.complete_migration(&p),
        Err(ControlError::BarrierPending { .. })
    ));
    assert!(matches!(
        table.state(&p).unwrap().0,
        PartitionState::Migrating { .. }
    ));

    // Still held part-way through the barrier.
    clock.advance(BARRIER.saturating_sub(Duration::from_secs(1)));
    assert!(matches!(
        cp.complete_migration(&p),
        Err(ControlError::BarrierPending { .. })
    ));

    // Once the barrier has elapsed, the pointer flips.
    clock.advance(Duration::from_secs(1));
    cp.complete_migration(&p).unwrap();
    assert_eq!(table.get(&p).unwrap().placement, cluster("b"));
}

#[test]
fn every_instance_polling_fresh_sees_one_consistent_state() {
    // Two proxy "instances" are two handles to the same backend, they poll it
    // fresh per request (no cached migration decision), so they never disagree.
    let (backend, p) = backend();
    let instance_a = Arc::clone(&backend);
    let instance_b = Arc::clone(&backend);
    let clock = Arc::new(ManualClock::new());
    let cp = ControlPlane::new(Arc::clone(&backend))
        .with_clock(clock.clone())
        .with_barrier(BARRIER);

    let epoch_active = backend.state(&p).unwrap().1;
    // Active: both instances admit a write resolved at the active epoch.
    assert_eq!(
        instance_a.admit_write(&p, epoch_active),
        WriteAdmission::Admit
    );
    assert_eq!(
        instance_b.admit_write(&p, epoch_active),
        WriteAdmission::Admit
    );

    cp.begin_migration(&p, cluster("b")).unwrap();
    let epoch_cutover = cp.enter_cutover(&p).unwrap();

    // Cutover: both instances reject, they read the same fresh state, so there
    // is no instance still writing to A (INV-M1 fleet-wide).
    assert_eq!(
        instance_a.admit_write(&p, epoch_cutover),
        WriteAdmission::Reject
    );
    assert_eq!(
        instance_b.admit_write(&p, epoch_cutover),
        WriteAdmission::Reject
    );

    // After the barrier and the flip, both instances resolve reads to B and admit
    // writes only at the new epoch.
    clock.advance(BARRIER);
    let epoch_b = cp.complete_migration(&p).unwrap();
    for instance in [&instance_a, &instance_b] {
        assert_eq!(instance.get(&p).unwrap().placement, cluster("b"));
        assert_eq!(instance.admit_write(&p, epoch_b), WriteAdmission::Admit);
        assert_eq!(
            instance.admit_write(&p, epoch_cutover),
            WriteAdmission::Reject
        );
    }
}

#[test]
fn abort_clears_the_barrier_and_returns_to_origin() {
    let (table, p) = backend();
    let clock = Arc::new(ManualClock::new());
    let cp = ControlPlane::new(Arc::clone(&table))
        .with_clock(clock.clone())
        .with_barrier(BARRIER);

    cp.begin_migration(&p, cluster("b")).unwrap();
    cp.enter_cutover(&p).unwrap();
    cp.abort_migration(&p).unwrap();

    assert_eq!(table.get(&p).unwrap().placement, cluster("a"));
    // No migration in flight: completing now is a transition error, not a barrier
    // wait (the barrier state was cleared by the abort).
    assert!(matches!(
        cp.complete_migration(&p),
        Err(ControlError::Transition(_))
    ));
}

#[test]
fn out_of_phase_transitions_surface_as_control_errors() {
    let (table, p) = backend();
    let cp = ControlPlane::new(Arc::clone(&table));

    // Cutover before begin: a transition error (not migrating).
    assert!(matches!(
        cp.enter_cutover(&p),
        Err(ControlError::Transition(_))
    ));
    cp.begin_migration(&p, cluster("b")).unwrap();
    // Begin twice: already migrating.
    assert!(matches!(
        cp.begin_migration(&p, cluster("b")),
        Err(ControlError::Transition(_))
    ));
}
