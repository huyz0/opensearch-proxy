//! Property-based correctness tests for `osproxy-core` (docs/09, docs/12).
//!
//! These assert *invariants over all inputs*, not single examples. As the
//! routing/tenancy logic lands, the headline properties (round-trip symmetry,
//! isolation, bulk order preservation, id-collision-freedom) live in their owning
//! crates; this file pins the core-level invariants the rest build on.

use std::time::Duration;

use osproxy_core::time::{Clock, ManualClock};
use osproxy_core::Epoch;
use proptest::prelude::*;

proptest! {
    /// Epoch::next is strictly increasing until it saturates, the property the
    /// migration cutover relies on (docs/06 INV-M2).
    #[test]
    fn epoch_next_is_strictly_increasing(g in 0u64..u64::MAX) {
        let e = Epoch::new(g);
        prop_assert!(e.next() > e);
        prop_assert_eq!(e.next().get(), g + 1);
    }

    /// A ManualClock reflects exactly the durations advanced into it, no drift,
    /// for any sequence of advances (deterministic time, docs/12).
    #[test]
    fn manual_clock_accumulates_advances_exactly(steps in prop::collection::vec(0u64..1_000_000, 0..32)) {
        let clock = ManualClock::new();
        let start = clock.now();
        let mut expected_nanos = 0u128;
        for ms in steps {
            clock.advance(Duration::from_micros(ms));
            expected_nanos += u128::from(ms) * 1_000;
        }
        let elapsed = clock.now().saturating_duration_since(start);
        prop_assert_eq!(elapsed.as_nanos(), expected_nanos);
    }
}
