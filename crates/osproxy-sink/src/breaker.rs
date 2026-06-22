//! A per-cluster circuit breaker for health-checked eviction (NFR-R, `docs/01`).
//!
//! Health is observed **passively** from request outcomes rather than by active
//! probing: a run of transport/timeout failures to a cluster *opens* the
//! breaker, so subsequent requests fail fast (no doomed connection attempt)
//! until a cooldown elapses. After the cooldown one trial request is allowed,
//! a success closes the breaker, a failure re-opens it. Time comes from an
//! injected [`Clock`](osproxy_core::Clock), so the cooldown is deterministic in
//! tests (`docs/12`).

use std::sync::Mutex;
use std::time::Duration;

use osproxy_core::Instant;

/// The mutable health state of one cluster's breaker.
#[derive(Debug, Default)]
struct State {
    /// Consecutive transport/timeout failures since the last success.
    consecutive_failures: u32,
    /// When the breaker opened, if it is currently open.
    opened_at: Option<Instant>,
}

/// A single cluster's circuit breaker. Holds only state; the failure threshold
/// and cooldown are owned by the sink and passed in, so they stay configurable
/// without rebuilding the per-cluster pools.
#[derive(Debug, Default)]
pub(crate) struct Breaker {
    state: Mutex<State>,
}

impl Breaker {
    /// Whether a request may be dispatched now: always when closed, and once the
    /// `cooldown` has elapsed when open (the half-open trial).
    pub(crate) fn allows(&self, now: Instant, cooldown: Duration) -> bool {
        let state = self.lock();
        match state.opened_at {
            None => true,
            Some(opened) => now.saturating_duration_since(opened) >= cooldown,
        }
    }

    /// Records a successful dispatch: the cluster is healthy, so close the breaker.
    pub(crate) fn record_success(&self) {
        let mut state = self.lock();
        state.consecutive_failures = 0;
        state.opened_at = None;
    }

    /// Records a transport/timeout failure: open the breaker once `threshold`
    /// consecutive failures are seen (and re-stamp the open time on a failed
    /// half-open trial, restarting the cooldown).
    pub(crate) fn record_failure(&self, now: Instant, threshold: u32) {
        let mut state = self.lock();
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.consecutive_failures >= threshold {
            state.opened_at = Some(now);
        }
    }

    /// Locks the state, recovering a poisoned lock, the breaker is inert health
    /// data with no invariant a panicking holder could tear (NFR-R1).
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{Clock, ManualClock};

    const THRESHOLD: u32 = 2;
    const COOLDOWN: Duration = Duration::from_secs(5);

    #[test]
    fn opens_after_threshold_then_recovers_after_cooldown() {
        let clock = ManualClock::new();
        let breaker = Breaker::default();

        // Closed: requests flow while failures stay under the threshold.
        assert!(breaker.allows(clock.now(), COOLDOWN));
        breaker.record_failure(clock.now(), THRESHOLD);
        assert!(
            breaker.allows(clock.now(), COOLDOWN),
            "one failure must not open"
        );

        // Second failure trips it: requests are shed during the cooldown.
        breaker.record_failure(clock.now(), THRESHOLD);
        assert!(
            !breaker.allows(clock.now(), COOLDOWN),
            "must open at threshold"
        );

        // Before the cooldown elapses it stays open.
        clock.advance(Duration::from_secs(4));
        assert!(!breaker.allows(clock.now(), COOLDOWN));

        // After the cooldown a trial is allowed; a success closes it for good.
        clock.advance(Duration::from_secs(2));
        assert!(
            breaker.allows(clock.now(), COOLDOWN),
            "half-open trial allowed"
        );
        breaker.record_success();
        assert!(
            breaker.allows(clock.now(), COOLDOWN),
            "success closes the breaker"
        );
    }

    #[test]
    fn a_failed_trial_reopens_and_restarts_the_cooldown() {
        let clock = ManualClock::new();
        let breaker = Breaker::default();
        breaker.record_failure(clock.now(), THRESHOLD);
        breaker.record_failure(clock.now(), THRESHOLD); // open at t=0
        clock.advance(Duration::from_secs(6));
        assert!(breaker.allows(clock.now(), COOLDOWN)); // trial allowed at t=6
        breaker.record_failure(clock.now(), THRESHOLD); // trial fails → reopen at t=6
        assert!(
            !breaker.allows(clock.now(), COOLDOWN),
            "failed trial re-opens"
        );
        clock.advance(Duration::from_secs(4));
        assert!(
            !breaker.allows(clock.now(), COOLDOWN),
            "cooldown restarted from t=6"
        );
    }
}
