//! The clock seam — the foundation of deterministic time.
//!
//! Production code must never read wall-clock time directly: a hidden
//! `Instant::now()` makes behavior depend on the machine and turns tests flaky.
//! Instead, every component that needs time takes a [`Clock`]. Production wires
//! [`SystemClock`]; tests wire [`ManualClock`] and advance it explicitly, so a
//! timeout, a TTL, or an affinity expiry is reproducible to the nanosecond.
//!
//! This is enforced mechanically: `clippy.toml` bans `SystemTime::now`,
//! `Instant::now`, and friends everywhere except [`SystemClock`], which is the
//! single sanctioned place that touches the real clock (`docs/09`, `docs/12`).

use std::sync::Mutex;
use std::time::Duration;

/// A monotonic instant, in nanoseconds since an unspecified epoch.
///
/// Opaque and only meaningful relative to another [`Instant`] from the same
/// [`Clock`]. Comparable and subtractable; never convertible to wall-clock time
/// (the proxy reasons about elapsed durations, not calendar time).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct Instant(u64);

impl Instant {
    /// Returns the duration elapsed from `earlier` to `self`, saturating at zero
    /// if `earlier` is later (clocks are monotonic, so this should not happen,
    /// but saturation keeps the type panic-free — NFR-R1).
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    /// Returns the instant `delta` after `self`, saturating at the maximum.
    #[must_use]
    pub fn saturating_add(self, delta: Duration) -> Instant {
        let nanos = u64::try_from(delta.as_nanos()).unwrap_or(u64::MAX);
        Instant(self.0.saturating_add(nanos))
    }
}

/// A source of monotonic time. Inject this anywhere time is needed.
pub trait Clock: Send + Sync {
    /// The current monotonic instant.
    fn now(&self) -> Instant;
}

/// The production clock, backed by the operating system's monotonic timer.
///
/// This is the **only** type permitted to read the real clock.
#[derive(Clone, Copy, Default, Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        // Anchor to a process-lifetime epoch so values are stable u64 nanos.
        static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        // SystemClock is the single sanctioned site permitted to read the OS
        // clock; everything else takes a Clock so time stays deterministic.
        #[allow(
            clippy::disallowed_methods,
            reason = "the one sanctioned site reading the OS monotonic clock (docs/12)"
        )]
        let (raw, epoch) = (
            std::time::Instant::now(),
            *EPOCH.get_or_init(std::time::Instant::now),
        );
        Instant(u64::try_from(raw.saturating_duration_since(epoch).as_nanos()).unwrap_or(u64::MAX))
    }
}

/// A test clock advanced explicitly. Starts at zero; never moves on its own.
#[derive(Debug, Default)]
pub struct ManualClock {
    nanos: Mutex<u64>,
}

impl ManualClock {
    /// Creates a clock reading zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Advances the clock by `delta`. Saturates at the maximum.
    pub fn advance(&self, delta: Duration) {
        let add = u64::try_from(delta.as_nanos()).unwrap_or(u64::MAX);
        if let Ok(mut nanos) = self.nanos.lock() {
            *nanos = nanos.saturating_add(add);
        }
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        Instant(self.nanos.lock().map(|n| *n).unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_is_frozen_until_advanced() {
        let clock = ManualClock::new();
        let t0 = clock.now();
        assert_eq!(clock.now(), t0, "clock must not advance on its own");
        clock.advance(Duration::from_millis(250));
        let t1 = clock.now();
        assert_eq!(t1.saturating_duration_since(t0), Duration::from_millis(250));
    }

    #[test]
    fn instant_arithmetic_saturates_and_does_not_panic() {
        let clock = ManualClock::new();
        let t0 = clock.now();
        let later = t0.saturating_add(Duration::from_secs(5));
        assert_eq!(later.saturating_duration_since(t0), Duration::from_secs(5));
        // Reverse subtraction saturates to zero rather than panicking.
        assert_eq!(t0.saturating_duration_since(later), Duration::ZERO);
    }

    #[test]
    fn system_clock_is_monotonic() {
        let clock = SystemClock;
        let a = clock.now();
        let b = clock.now();
        assert!(b >= a, "monotonic clock must not go backwards");
    }
}
