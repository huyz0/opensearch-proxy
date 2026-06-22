//! Bounded retry-with-backoff for the placement backend reads (`docs/06` §3a).
//!
//! Placement is resolved by polling the operator's backend (behind the SPI)
//! fresh on every request. A *momentary* backend unavailability should not bounce
//! the write to the client: we retry with exponential backoff up to a small
//! budget, then surface the (retryable) error so the client can try later.
//!
//! Only a **retryable** [`SpiError`] (the backend is unavailable) is retried, a
//! definitive routing answer (partition unresolved, placement missing, or a
//! migration *reject*) is returned immediately, never retried in-proxy.

use std::future::Future;
use std::time::Duration;

use osproxy_spi::SpiError;

/// How the proxy retries a transiently-unavailable placement backend.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RetryPolicy {
    /// Total attempts including the first (so `1` disables retry).
    pub max_attempts: u32,
    /// The first backoff delay; each subsequent attempt doubles it.
    pub base_backoff: Duration,
    /// The cap on a single backoff delay.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(1),
        }
    }
}

impl RetryPolicy {
    /// The backoff delay before the retry following `attempt` (0-based):
    /// `base * 2^attempt`, capped at `max_backoff` and saturating on overflow.
    fn backoff(self, attempt: u32) -> Duration {
        let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
        self.base_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }
}

/// Runs `op`, retrying on a retryable [`SpiError`] with the policy's backoff up
/// to `max_attempts`. A non-retryable error (or the last attempt's error) is
/// returned as-is.
pub(crate) async fn with_retry<T, F, Fut>(policy: RetryPolicy, mut op: F) -> Result<T, SpiError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SpiError>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if err.retryable() && attempt + 1 < policy.max_attempts => {
                let backoff = policy.backoff(attempt);
                if !backoff.is_zero() {
                    tokio::time::sleep(backoff).await;
                }
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn backend_unavailable() -> SpiError {
        SpiError::PlacementBackend { retryable: true }
    }

    #[tokio::test]
    async fn retries_a_transient_backend_then_succeeds() {
        // Fail the first two attempts (retryable), succeed on the third.
        let policy = RetryPolicy {
            max_attempts: 3,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };
        let calls = Cell::new(0);
        let out: Result<u8, SpiError> = with_retry(policy, || {
            let n = calls.get() + 1;
            calls.set(n);
            async move {
                if n < 3 {
                    Err(backend_unavailable())
                } else {
                    Ok(7)
                }
            }
        })
        .await;
        assert_eq!(out.unwrap(), 7);
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts_with_the_retryable_error() {
        let policy = RetryPolicy {
            max_attempts: 2,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        };
        let calls = Cell::new(0);
        let out: Result<u8, SpiError> = with_retry(policy, || {
            calls.set(calls.get() + 1);
            async { Err(backend_unavailable()) }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(calls.get(), 2, "exactly max_attempts tries");
    }

    #[tokio::test]
    async fn does_not_retry_a_non_retryable_error() {
        let policy = RetryPolicy::default();
        let calls = Cell::new(0);
        let out: Result<u8, SpiError> = with_retry(policy, || {
            calls.set(calls.get() + 1);
            async {
                Err(SpiError::PlacementMissing {
                    partition: osproxy_core::PartitionId::from("p"),
                })
            }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(calls.get(), 1, "a definitive error is not retried");
    }
}
