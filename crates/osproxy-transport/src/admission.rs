//! Bounded-memory admission control for the ingress (NFR-R3, `docs/04` §3).
//!
//! Two cooperating bounds keep the proxy from being driven to OOM by large or
//! numerous request bodies:
//!
//! - a **per-request cap** ([`IngressLimits::max_body_bytes`]) — a single body
//!   larger than this is rejected with `413 Payload Too Large`; and
//! - a **global in-flight ceiling** ([`IngressLimits::inflight_ceiling`]) — the
//!   sum of the bodies currently buffered across all connections. A request that
//!   would push the total over the ceiling is shed with `429 Too Many Requests`
//!   and retry guidance, rather than admitted into memory.
//!
//! The ceiling is enforced by a single atomic counter: a request reserves its
//! (content-length-bounded) size up front via [`Admission::try_reserve`] and the
//! returned [`Reservation`] releases it on drop, so the budget is returned the
//! instant the response is sent — no queue, no lock.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Per-ingress memory bounds. Sized for bulk: `max_body_bytes` is the largest
/// single body buffered, `inflight_ceiling` the largest sum across concurrent
/// requests before new ones are shed with `429`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IngressLimits {
    /// The largest single request body the ingress will buffer (else `413`).
    pub max_body_bytes: usize,
    /// The largest total of concurrently-buffered bodies (else `429`).
    pub inflight_ceiling: usize,
}

impl Default for IngressLimits {
    fn default() -> Self {
        // 8 MiB per body, 256 MiB in flight — bulk-sized, bounded, never OOM.
        Self {
            max_body_bytes: 8 * 1024 * 1024,
            inflight_ceiling: 256 * 1024 * 1024,
        }
    }
}

/// The shared in-flight-bytes budget enforcing [`IngressLimits::inflight_ceiling`].
#[derive(Debug)]
pub(crate) struct Admission {
    inflight: AtomicUsize,
    ceiling: usize,
}

impl Admission {
    /// Creates a budget with the given ceiling.
    pub(crate) fn new(ceiling: usize) -> Self {
        Self {
            inflight: AtomicUsize::new(0),
            ceiling,
        }
    }

    /// Reserves `amount` bytes of the budget, or `None` if that would exceed the
    /// ceiling (the caller sheds the request with `429`). The reservation is
    /// released when the returned [`Reservation`] is dropped.
    pub(crate) fn try_reserve(self: &Arc<Self>, amount: usize) -> Option<Reservation> {
        let mut current = self.inflight.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(amount)?;
            if next > self.ceiling {
                return None;
            }
            match self.inflight.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(Reservation {
                        admission: Arc::clone(self),
                        amount,
                    })
                }
                Err(actual) => current = actual,
            }
        }
    }
}

/// A held slice of the in-flight budget, released on drop.
#[derive(Debug)]
pub(crate) struct Reservation {
    admission: Arc<Admission>,
    amount: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.admission
            .inflight
            .fetch_sub(self.amount, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_up_to_the_ceiling_then_sheds() {
        let admission = Arc::new(Admission::new(100));
        let a = admission.try_reserve(60).expect("first fits");
        let b = admission.try_reserve(40).expect("second fills it exactly");
        // The budget is now full: a further reservation is shed.
        assert!(admission.try_reserve(1).is_none(), "over ceiling must shed");
        drop(a);
        // Releasing 60 makes room again.
        assert!(
            admission.try_reserve(50).is_some(),
            "freed budget is reusable"
        );
        drop(b);
    }

    #[test]
    fn an_amount_over_the_whole_ceiling_is_shed() {
        let admission = Arc::new(Admission::new(10));
        assert!(admission.try_reserve(11).is_none());
    }
}
