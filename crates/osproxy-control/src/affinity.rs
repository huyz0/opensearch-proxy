//! Cursor (scroll / PIT) affinity: pinning a cursor's follow-up requests to the
//! physical cluster that created it (`docs/03` §6).
//!
//! A scroll id or point-in-time id is bound to the cluster that opened it; a
//! continuation sent elsewhere is meaningless. So when affinity is **on**
//! ([`Affinity::Pin`]), the proxy records `cursor_id -> cluster` when a cursor is
//! created and resolves follow-ups to that cluster, bypassing the normal
//! partition-resolution path. The binding is **bounded and TTL'd**, it expires
//! with the cursor's keep-alive and the map is capacity-capped, so a flood of
//! cursors cannot grow memory without limit (NFR-P). Affinity is opt-in and off
//! by default, so deployments that do not use cursors pay no state cost.
//!
//! Time comes from an injected [`Clock`], so expiry is deterministic in tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use osproxy_core::{Clock, ClusterId, Instant, SystemClock};

/// The default cursor-binding TTL: bindings expire on this keep-alive if not
/// refreshed, matching a typical scroll/PIT lifetime.
pub const DEFAULT_CURSOR_TTL: Duration = Duration::from_secs(300);

/// The default cap on live cursor bindings, bounding affinity memory (NFR-P).
pub const DEFAULT_CAPACITY: usize = 100_000;

/// Whether the proxy pins cursor follow-ups to the cluster that created them.
/// Opt-in, off by default, deployments without cursors pay no state cost
/// (`docs/03` §6).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Affinity {
    /// No pinning; cursor requests resolve through the normal path.
    #[default]
    Off,
    /// Pin each cursor's follow-ups to its creating cluster.
    Pin,
}

/// One cursor's binding: the cluster that owns it and when it was pinned.
#[derive(Clone, Debug)]
struct Pinned {
    cluster: ClusterId,
    pinned_at: Instant,
}

/// A bounded, TTL'd map from cursor id to the cluster that created it
/// (`docs/03` §6). Cloneable handles are not provided; wrap in an `Arc` to share.
pub struct CursorAffinity {
    clock: Arc<dyn Clock>,
    ttl: Duration,
    capacity: usize,
    entries: Mutex<HashMap<String, Pinned>>,
}

impl std::fmt::Debug for CursorAffinity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected `Clock` is not `Debug`; the rest is the useful shape.
        f.debug_struct("CursorAffinity")
            .field("ttl", &self.ttl)
            .field("capacity", &self.capacity)
            .field("live", &self.len())
            .finish_non_exhaustive()
    }
}

impl CursorAffinity {
    /// Builds a cursor-affinity map with the given binding TTL and capacity,
    /// reading time from the system clock.
    #[must_use]
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            clock: Arc::new(SystemClock),
            ttl,
            capacity: capacity.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Swaps the clock that drives expiry (tests inject a `ManualClock`).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Records that `cursor_id` lives on `cluster`. Expired bindings are swept
    /// first; if the map is still at capacity, the oldest binding is evicted so
    /// the new one fits (bounded memory).
    pub fn pin(&self, cursor_id: impl Into<String>, cluster: ClusterId) {
        let now = self.clock.now();
        let mut entries = self.lock();
        entries.retain(|_, p| !self.is_expired(p, now));
        if entries.len() >= self.capacity {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, p)| p.pinned_at)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            cursor_id.into(),
            Pinned {
                cluster,
                pinned_at: now,
            },
        );
    }

    /// The cluster `cursor_id` is pinned to, or `None` if it is unknown or its
    /// binding has expired (lazy expiry, a stale binding is never returned).
    #[must_use]
    pub fn resolve(&self, cursor_id: &str) -> Option<ClusterId> {
        let now = self.clock.now();
        let entries = self.lock();
        entries
            .get(cursor_id)
            .filter(|p| !self.is_expired(p, now))
            .map(|p| p.cluster.clone())
    }

    /// Drops `cursor_id`'s binding (e.g. on an explicit clear-scroll / close-PIT).
    pub fn release(&self, cursor_id: &str) {
        self.lock().remove(cursor_id);
    }

    /// The number of bindings currently held (including any not yet swept).
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether no bindings are held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Whether a binding pinned at `p.pinned_at` is past its TTL at `now`.
    fn is_expired(&self, p: &Pinned, now: Instant) -> bool {
        now.saturating_duration_since(p.pinned_at) >= self.ttl
    }

    /// Locks the map, recovering a poisoned lock, it is plain cache data with no
    /// invariant a panicking holder could tear (NFR-R1).
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Pinned>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
