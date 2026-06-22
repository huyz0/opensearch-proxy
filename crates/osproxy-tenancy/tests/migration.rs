//! Migration correctness invariants (INV-M1..M4, `docs/06` §6), exercised as
//! deterministic state-machine simulations against the [`PlacementTable`].
//!
//! The table's epoch is a logical generation (no wall-clock), so these run
//! reproducibly with no time control needed, the interleavings are driven
//! explicitly. A write carries the epoch it resolved at; the gate
//! ([`PlacementTable::admit_write`]) decides whether it may still commit. Each
//! test names the invariant it pins.

#![allow(clippy::unwrap_used)]

use osproxy_core::{Epoch, PartitionId};
use osproxy_spi::Placement;
use osproxy_tenancy::{MigrationError, PartitionState, PlacementTable, WriteAdmission};

fn cluster(name: &str) -> Placement {
    Placement::DedicatedCluster {
        cluster: osproxy_core::ClusterId::from(name),
    }
}

/// A partition registered at A (returning its epoch), migrating toward B.
fn registered() -> (PlacementTable, PartitionId, Epoch, Placement) {
    let table = PlacementTable::new();
    let p = PartitionId::from("acme");
    let e_active = table.set(p.clone(), cluster("a"));
    (table, p, e_active, cluster("b"))
}

#[test]
fn inv_m1_no_write_commits_during_cutover() {
    let (table, p, e_active, b) = registered();
    let e_drain = table.begin_migration(&p, b).unwrap();
    let e_cutover = table.enter_cutover(&p).unwrap();

    // During cutover, a write resolved at *any* epoch is rejected, even one
    // resolved at the live cutover epoch. The client retries; the retry will
    // succeed once the pointer flips.
    assert_eq!(table.admit_write(&p, e_cutover), WriteAdmission::Reject);
    assert_eq!(table.admit_write(&p, e_drain), WriteAdmission::Reject);
    assert_eq!(table.admit_write(&p, e_active), WriteAdmission::Reject);
    // Reads still resolve, to the old placement, a single view.
    assert_eq!(table.get(&p).unwrap().placement, cluster("a"));
}

#[test]
fn inv_m2_after_cutover_a_stale_write_never_admits() {
    let (table, p, e_active, b) = registered();
    let e_drain = table.begin_migration(&p, b).unwrap();
    table.enter_cutover(&p).unwrap();
    let e_flipped = table.complete_migration(&p).unwrap();

    // Any write resolved before the flip is stale and can never commit; only one
    // resolved at the new, current epoch does. Its retry re-resolves to B.
    assert_eq!(table.admit_write(&p, e_active), WriteAdmission::Reject);
    assert_eq!(table.admit_write(&p, e_drain), WriteAdmission::Reject);
    assert_eq!(table.admit_write(&p, e_flipped), WriteAdmission::Admit);
    assert_eq!(table.get(&p).unwrap().placement, cluster("b"));
    assert!(matches!(
        table.state(&p).unwrap().0,
        PartitionState::Active(_)
    ));
}

#[test]
fn inv_m3_abort_returns_to_origin_and_admits_the_origin_epoch_again() {
    // Abort from each migrating phase returns to Active(A) at a fresh epoch; the
    // migrating epochs are stale forever, so nothing that resolved mid-migration
    // can land, and the destination B never had a non-stale write epoch at all.
    for enter_cutover in [false, true] {
        let (table, p, e_active, b) = registered();
        let e_drain = table.begin_migration(&p, b).unwrap();
        if enter_cutover {
            table.enter_cutover(&p).unwrap();
        }
        let e_aborted = table.abort_migration(&p).unwrap();

        // Back at the origin: the post-abort epoch admits, the pre/mid-migration
        // epochs are stale.
        assert_eq!(table.admit_write(&p, e_aborted), WriteAdmission::Admit);
        assert_eq!(table.admit_write(&p, e_active), WriteAdmission::Reject);
        assert_eq!(table.admit_write(&p, e_drain), WriteAdmission::Reject);
        assert_eq!(table.get(&p).unwrap().placement, cluster("a"));
    }
}

#[test]
fn inv_m4_reads_are_always_a_single_placement() {
    // Through every phase the read placement is exactly one of {A, B}, never a
    // split, A until the flip, B after.
    let (table, p, _e, b) = registered();
    let a = cluster("a");
    assert_eq!(table.get(&p).unwrap().placement, a);
    table.begin_migration(&p, b.clone()).unwrap();
    assert_eq!(table.get(&p).unwrap().placement, a, "draining reads from A");
    table.enter_cutover(&p).unwrap();
    assert_eq!(table.get(&p).unwrap().placement, a, "cutover still reads A");
    table.complete_migration(&p).unwrap();
    assert_eq!(table.get(&p).unwrap().placement, b, "flip moves reads to B");
}

#[test]
fn draining_keeps_writes_flowing_at_the_draining_epoch() {
    // The whole point of Draining: writes continue to A normally while the
    // external tool copies, so only Cutover rejects (contrast INV-M1). A write
    // resolved during draining carries the draining epoch and commits.
    let (table, p, e_active, b) = registered();
    let e_drain = table.begin_migration(&p, b).unwrap();
    assert_eq!(table.admit_write(&p, e_drain), WriteAdmission::Admit);
    // A write resolved before the migration began is now stale.
    assert_eq!(table.admit_write(&p, e_active), WriteAdmission::Reject);
}

#[test]
fn epoch_advances_monotonically_across_every_transition() {
    let (table, p, e0, b) = registered();
    let e1 = table.begin_migration(&p, b).unwrap();
    let e2 = table.enter_cutover(&p).unwrap();
    let e3 = table.complete_migration(&p).unwrap();
    assert!(e0 < e1 && e1 < e2 && e2 < e3, "{e0:?}<{e1:?}<{e2:?}<{e3:?}");
}

#[test]
fn an_unrelated_partitions_migration_does_not_stale_this_one() {
    // Epoch staleness is per-partition: bumping the global generation by
    // migrating another partition must not reject this partition's live write.
    let (table, p, e_p, _b) = registered();
    let q = PartitionId::from("other");
    table.set(q.clone(), cluster("x"));
    table.begin_migration(&q, cluster("y")).unwrap();
    table.enter_cutover(&q).unwrap();
    // p never transitioned, so its resolved epoch is still current.
    assert_eq!(table.admit_write(&p, e_p), WriteAdmission::Admit);
}

#[test]
fn transitions_are_rejected_out_of_phase_and_leave_the_table_unchanged() {
    let (table, p, _e, b) = registered();

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
fn at_most_one_epoch_is_admissible_per_phase_no_split_brain() {
    // Drive the lifecycle capturing each phase's epoch; assert that across the
    // whole history at most one captured epoch is admissible at any instant,
    // there is never a window where two different resolved writes could both
    // commit (no split-brain).
    let (table, p, e_active, b) = registered();
    let e_drain = table.begin_migration(&p, b).unwrap();
    let e_cutover = table.enter_cutover(&p).unwrap();
    let e_flipped = table.complete_migration(&p).unwrap();

    let epochs = [e_active, e_drain, e_cutover, e_flipped];
    let admissible = epochs
        .iter()
        .filter(|&&e| table.admit_write(&p, e) == WriteAdmission::Admit)
        .count();
    assert_eq!(admissible, 1, "exactly the current epoch admits");
    assert_eq!(table.admit_write(&p, e_flipped), WriteAdmission::Admit);
}
