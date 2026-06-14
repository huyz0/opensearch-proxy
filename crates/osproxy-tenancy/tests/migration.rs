//! Migration correctness invariants (INV-M1..M4, `docs/06` §6), exercised as
//! deterministic state-machine simulations against the [`PlacementTable`].
//!
//! The table's epoch is a logical generation (no wall-clock), so these run
//! reproducibly with no time control needed — the interleavings are driven
//! explicitly. Each test names the invariant it pins.

#![allow(clippy::unwrap_used)]

use osproxy_core::PartitionId;
use osproxy_spi::Placement;
use osproxy_tenancy::{MigrationError, PartitionState, PlacementTable, WriteAdmission};

fn cluster(name: &str) -> Placement {
    Placement::DedicatedCluster {
        cluster: osproxy_core::ClusterId::from(name),
    }
}

/// A partition registered at A, migrating toward B.
fn registered() -> (PlacementTable, PartitionId, Placement, Placement) {
    let table = PlacementTable::new();
    let p = PartitionId::from("acme");
    let a = cluster("a");
    let b = cluster("b");
    table.set(p.clone(), a.clone());
    (table, p, a, b)
}

#[test]
fn inv_m1_no_write_commits_during_cutover() {
    let (table, p, a, b) = registered();
    table.begin_migration(&p, b.clone()).unwrap();
    table.enter_cutover(&p).unwrap();

    // During cutover, *every* resolved target is rejected — the one window with
    // write rejection (the client retries; the retry re-resolves after the flip).
    assert_eq!(table.admit_write(&p, &a), WriteAdmission::Reject);
    assert_eq!(table.admit_write(&p, &b), WriteAdmission::Reject);
    // Reads still resolve — to the old placement, a single view.
    assert_eq!(table.get(&p).unwrap().placement, a);
}

#[test]
fn inv_m2_after_cutover_old_placement_never_admits() {
    let (table, p, a, b) = registered();
    table.begin_migration(&p, b.clone()).unwrap();
    table.enter_cutover(&p).unwrap();
    table.complete_migration(&p).unwrap();

    // A write that resolved against the old placement A before the flip can
    // never commit afterward; only the new placement B does. Its retry will
    // re-resolve to B.
    assert_eq!(table.admit_write(&p, &a), WriteAdmission::Reject);
    assert_eq!(table.admit_write(&p, &b), WriteAdmission::Admit);
    assert_eq!(table.get(&p).unwrap().placement, b);
    assert!(matches!(
        table.state(&p).unwrap().0,
        PartitionState::Active(_)
    ));
}

#[test]
fn inv_m3_abort_returns_to_origin_with_no_writes_to_destination() {
    // Abort from each migrating phase returns to Active(A); a B-resolved write is
    // never admitted across the whole aborted attempt, so nothing landed in B.
    for enter_cutover in [false, true] {
        let (table, p, a, b) = registered();
        table.begin_migration(&p, b.clone()).unwrap();
        // Before abort: B never admits in either phase.
        assert_eq!(table.admit_write(&p, &b), WriteAdmission::Reject);
        if enter_cutover {
            table.enter_cutover(&p).unwrap();
            assert_eq!(table.admit_write(&p, &b), WriteAdmission::Reject);
        }
        table.abort_migration(&p).unwrap();

        // Back to the origin: A admits again, B still never does.
        assert_eq!(table.admit_write(&p, &a), WriteAdmission::Admit);
        assert_eq!(table.admit_write(&p, &b), WriteAdmission::Reject);
        assert_eq!(table.get(&p).unwrap().placement, a);
    }
}

#[test]
fn inv_m4_reads_are_always_a_single_placement() {
    // Through every phase the read placement is exactly one of {A, B}, never a
    // split — A until the flip, B after.
    let (table, p, a, b) = registered();
    assert_eq!(table.get(&p).unwrap().placement, a);
    table.begin_migration(&p, b.clone()).unwrap();
    assert_eq!(table.get(&p).unwrap().placement, a, "draining reads from A");
    table.enter_cutover(&p).unwrap();
    assert_eq!(table.get(&p).unwrap().placement, a, "cutover still reads A");
    table.complete_migration(&p).unwrap();
    assert_eq!(table.get(&p).unwrap().placement, b, "flip moves reads to B");
}

#[test]
fn draining_keeps_writes_flowing_to_the_origin() {
    // The whole point of Draining: writes continue to A normally while the
    // external tool copies, so only Cutover rejects (contrast INV-M1).
    let (table, p, a, b) = registered();
    table.begin_migration(&p, b.clone()).unwrap();
    assert_eq!(table.admit_write(&p, &a), WriteAdmission::Admit);
    // A write resolved for B can't commit yet — the pointer hasn't flipped.
    assert_eq!(table.admit_write(&p, &b), WriteAdmission::Reject);
}

#[test]
fn epoch_advances_monotonically_across_every_transition() {
    let (table, p, _a, b) = registered();
    let e0 = table.state(&p).unwrap().1;
    let e1 = table.begin_migration(&p, b).unwrap();
    let e2 = table.enter_cutover(&p).unwrap();
    let e3 = table.complete_migration(&p).unwrap();
    assert!(e0 < e1 && e1 < e2 && e2 < e3, "{e0:?}<{e1:?}<{e2:?}<{e3:?}");
}

#[test]
fn transitions_are_rejected_out_of_phase_and_leave_the_table_unchanged() {
    let (table, p, _a, b) = registered();

    // Cutover/complete/abort before a migration begins: not migrating.
    assert_eq!(table.enter_cutover(&p), Err(MigrationError::NotMigrating));
    assert_eq!(
        table.complete_migration(&p),
        Err(MigrationError::NotMigrating)
    );
    assert_eq!(table.abort_migration(&p), Err(MigrationError::NotMigrating));

    table.begin_migration(&p, b.clone()).unwrap();
    // Begin again while migrating: already migrating; complete before cutover.
    assert_eq!(
        table.begin_migration(&p, b),
        Err(MigrationError::AlreadyMigrating)
    );
    assert_eq!(
        table.complete_migration(&p),
        Err(MigrationError::NotCutover)
    );

    // A refused transition must not have advanced the state past Draining.
    assert!(table.state(&p).unwrap().0.is_migrating());
    let unknown = PartitionId::from("nobody");
    assert_eq!(
        table.begin_migration(&unknown, cluster("z")),
        Err(MigrationError::UnknownPartition)
    );
}

#[test]
fn interleaved_inflight_writes_never_admit_against_a_stale_destination() {
    // Simulation: capture writes "resolved" at each phase (carrying the target
    // they resolved to), then drive the full lifecycle and assert at every step
    // that a captured write is admitted *iff* its target equals the partition's
    // current write destination — the single rule the gate must enforce.
    let (table, p, a, b) = registered();

    let admitted_target = |t: &PlacementTable| -> Option<Placement> {
        // The only target that may currently be admitted, if any.
        for candidate in [&a, &b] {
            if t.admit_write(&p, candidate) == WriteAdmission::Admit {
                return Some(candidate.clone());
            }
        }
        None
    };

    // Active(A): writes to A admit, nothing else.
    assert_eq!(admitted_target(&table), Some(a.clone()));
    table.begin_migration(&p, b.clone()).unwrap();
    // Draining: still A.
    assert_eq!(admitted_target(&table), Some(a.clone()));
    table.enter_cutover(&p).unwrap();
    // Cutover: nothing admits.
    assert_eq!(admitted_target(&table), None);
    table.complete_migration(&p).unwrap();
    // Active(B): only B.
    assert_eq!(admitted_target(&table), Some(b.clone()));

    // At most one placement is ever admissible at a time — there is never a
    // window where both A and B writes could commit (no split-brain).
    assert!(
        !(table.admit_write(&p, &a) == WriteAdmission::Admit
            && table.admit_write(&p, &b) == WriteAdmission::Admit)
    );
}
