//! Reference distributed [`DirectiveStore`] backed by etcd v3.
//!
//! A fleet of proxy instances must all see the *same* diagnostics directives, and
//! a control-plane flip must reach every instance with **no restart** (`docs/05`
//! §3, NFR-T3, ADR-013). This adapter realizes that over etcd's watch API using
//! the **watch-and-cache** model: a background task subscribes to one etcd key and
//! keeps a locally-cached [`DirectiveSet`] snapshot fresh, so [`DirectiveStore::load`]
//! on the request hot path is a cheap `Arc` clone, never per-request network I/O.
//!
//! It deliberately backs **only** the directive (observability) control plane.
//! The migration/placement store (`osproxy-control::MigrationStore`) needs a
//! linearizable compare-and-swap and a fallible, async seam; wiring it over etcd
//! is a separate step gated on that seam refactor.
//!
//! Posture:
//! - **Fail-fast at startup**: [`EtcdDirectiveStore::connect`] does an initial
//!   read, so an unreachable/misconfigured etcd is a loud construction error, not
//!   a silent empty directive set.
//! - **Fail-safe while running**: a transient etcd outage or a *malformed* publish
//!   keeps the **last good** snapshot rather than blanking diagnostics; the watch
//!   task reconnects with a bounded delay.
//! - **One fail-closed decoder**: directives are decoded with
//!   [`osproxy_observe::decode_directive_set`], the same decoder the admin
//!   `POST /admin/directives` endpoint uses, so a directive means the same thing
//!   however it is published, and a typo'd key can never widen its blast radius.
#![deny(missing_docs)]

use std::sync::Arc;

use arc_swap::ArcSwap;
use osproxy_core::Clock;
use osproxy_observe::{decode_directive_set, DirectiveSet, DirectiveStore};

mod watch;

/// Errors constructing the store. Only startup is fallible to the caller; once
/// running, the watch task absorbs transient failures (keeping the last snapshot).
#[derive(Debug, thiserror::Error)]
pub enum EtcdError {
    /// The initial connection or read against etcd failed, fail fast rather than
    /// serve an empty directive set the operator did not intend.
    #[error("etcd connect/read failed at startup")]
    Connect(#[from] etcd_client::Error),
}

/// A [`DirectiveStore`] whose snapshot is kept fresh by an etcd watch.
///
/// Construct with [`EtcdDirectiveStore::connect`] inside a Tokio runtime; it loads
/// the initial set and spawns the background watch. Clone is cheap (shared
/// snapshot) so the same store can be handed to the pipeline and an admin surface.
#[derive(Clone, Debug)]
pub struct EtcdDirectiveStore {
    current: Arc<ArcSwap<DirectiveSet>>,
}

impl EtcdDirectiveStore {
    /// Wraps an already-built shared snapshot, the seam the [`watch`] connect path
    /// uses after its initial read.
    fn from_snapshot(current: Arc<ArcSwap<DirectiveSet>>) -> Self {
        Self { current }
    }
}

impl DirectiveStore for EtcdDirectiveStore {
    fn load(&self) -> Arc<DirectiveSet> {
        // Lock-free atomic load of the watch-maintained snapshot (hot path).
        self.current.load_full()
    }
}

/// Swaps in a freshly decoded set, or **keeps the last good snapshot** if the
/// value does not parse, a malformed publish must never blank fleet diagnostics.
fn apply_value(current: &ArcSwap<DirectiveSet>, value: &[u8], clock: &dyn Clock) {
    if let Ok(set) = decode_directive_set(value, clock) {
        current.store(Arc::new(set));
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
