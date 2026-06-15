//! The fleet-wide diagnostics-directive store seam (`docs/05` §3-4).
//!
//! The signed `X-Debug-Directive` header is *surgical* — one request, one
//! instance. The store is its *fleet-wide* counterpart: a controller publishes a
//! [`DirectiveSet`] and every proxy instance reads it, so an operator can raise
//! verbosity across the fleet (a tenant, an endpoint, a sampled slice) without a
//! restart. Like the migration control plane (`osproxy-control`), the proxy ships
//! the **seam plus an in-process reference**, not a distributed store: a real
//! etcd/Consul/OpenSearch-index backend implements the same trait unchanged.
//!
//! Reads are **fresh per request** and on the hot path, so [`DirectiveStore::load`]
//! is a cheap `Arc` clone of the current snapshot — a distributed backend keeps a
//! watched local copy and returns it here rather than doing I/O per call. TTL
//! safety is intrinsic: directives carry an absolute expiry, so even a published
//! set that is never replaced self-expires at evaluation time.

use std::sync::{Arc, Mutex, PoisonError};

use crate::directive::DirectiveSet;

/// The backend holding the fleet's active diagnostics directives. Proxy instances
/// poll it fresh per request; a controller publishes new sets into it.
pub trait DirectiveStore: Send + Sync {
    /// The currently active directive set. Called on the request hot path, so it
    /// must be cheap (an `Arc` clone of a cached snapshot), never blocking I/O.
    fn load(&self) -> Arc<DirectiveSet>;
}

/// A fixed set: the directives never change for this process. The default store,
/// and the wrapper for a statically configured [`DirectiveSet`].
impl DirectiveStore for Arc<DirectiveSet> {
    fn load(&self) -> Arc<DirectiveSet> {
        Arc::clone(self)
    }
}

/// The in-process reference store: a controller (or an admin endpoint) `publish`es
/// a new set and proxy threads `load` it. Swappable for a distributed
/// `DirectiveStore` without touching the pipeline (`docs/05` §3).
#[derive(Debug, Default)]
pub struct InMemoryDirectiveStore {
    current: Mutex<Arc<DirectiveSet>>,
}

impl InMemoryDirectiveStore {
    /// An empty store — every request evaluates to `Off` until a set is published.
    #[must_use]
    pub fn new() -> Self {
        Self {
            current: Mutex::new(Arc::new(DirectiveSet::new())),
        }
    }

    /// Seeds the store with an initial directive set (builder style).
    #[must_use]
    pub fn with_directives(self, set: DirectiveSet) -> Self {
        self.publish(set);
        self
    }

    /// Replaces the active set — the fleet-wide "flip" an operator performs. The
    /// next `load` on every thread sees it (no restart).
    pub fn publish(&self, set: DirectiveSet) {
        *self.lock() = Arc::new(set);
    }

    /// Locks the snapshot, recovering a poisoned lock — it is a pointer swap with
    /// no torn invariant a panicking holder could leave behind (NFR-R1).
    fn lock(&self) -> std::sync::MutexGuard<'_, Arc<DirectiveSet>> {
        self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl DirectiveStore for InMemoryDirectiveStore {
    fn load(&self) -> Arc<DirectiveSet> {
        Arc::clone(&self.lock())
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
