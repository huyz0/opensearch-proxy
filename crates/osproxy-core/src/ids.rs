//! Strongly-typed identifier newtypes.
//!
//! Bare `String`/`u64` identifiers must never cross an API boundary: they are
//! trivially mixed up and they make traces ambiguous. Every identifier in the
//! system is a distinct type so the compiler catches misuse and so telemetry
//! can label each value precisely (`docs/08` §7, `docs/05`).

use std::fmt;

/// Defines a string-backed identifier newtype with `Display`, `From<String>`,
/// `From<&str>`, and an `as_str` accessor. Keeps each id a distinct type while
/// avoiding repetitive boilerplate.
macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            /// Borrows the underlying string.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the id, returning the owned string.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        // Identifiers are safe to render in telemetry (they are ids, not
        // tenant *values*); a precise Debug aids the `/debug/explain` story.
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({:?})"), self.0)
            }
        }
    };
}

string_id! {
    /// The tenancy/placement unit. Everything routes by this (`docs/03` §1).
    PartitionId
}

string_id! {
    /// Identifies a physical OpenSearch cluster.
    ClusterId
}

string_id! {
    /// A concrete (physical) OpenSearch index name.
    IndexName
}

string_id! {
    /// The authenticated principal's stable id. Never the raw token (`docs/05`).
    PrincipalId
}

string_id! {
    /// Correlates all telemetry for a single request (`docs/05` §6).
    RequestId
}

/// The placement-table generation a routing decision was resolved against.
///
/// Monotonically increasing. Stamped on every routed write so the sink can
/// reject a stale-epoch write for a migrating partition (`docs/06` §2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Epoch(u64);

impl Epoch {
    /// The initial epoch.
    pub const ZERO: Self = Self(0);

    /// Constructs an epoch from a raw generation number.
    #[must_use]
    pub fn new(generation: u64) -> Self {
        Self(generation)
    }

    /// Returns the raw generation number.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }

    /// Returns the next epoch. Saturates at `u64::MAX` rather than wrapping, so
    /// monotonicity (relied on by migration cutover, `docs/06` INV-M2) can
    /// never be violated by overflow.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_id_roundtrips_through_str_and_string() {
        let from_str = PartitionId::from("tenant-7");
        let from_string = PartitionId::from("tenant-7".to_owned());
        assert_eq!(from_str, from_string);
        assert_eq!(from_str.as_str(), "tenant-7");
        assert_eq!(from_str.clone().into_string(), "tenant-7");
    }

    #[test]
    fn distinct_id_types_do_not_compare_but_display_plainly() {
        let cluster = ClusterId::from("eu-1");
        assert_eq!(cluster.to_string(), "eu-1");
        // Debug is labelled so traces are unambiguous.
        assert_eq!(format!("{cluster:?}"), r#"ClusterId("eu-1")"#);
    }

    #[test]
    fn epoch_is_monotonic_and_saturates() {
        assert_eq!(Epoch::ZERO.get(), 0);
        assert_eq!(Epoch::ZERO.next(), Epoch::new(1));
        assert!(Epoch::new(1) > Epoch::ZERO);
        assert_eq!(Epoch::new(u64::MAX).next(), Epoch::new(u64::MAX));
    }
}
