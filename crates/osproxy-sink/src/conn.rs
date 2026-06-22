//! Counting the real TCP connection opens behind a pooled client, so pool reuse
//! is observable (NFR-P, `docs/01` §7) without the pooled client exposing it.
//!
//! `hyper-util`'s legacy `Client` reuses connections from its pool but does not
//! report whether a given request rode a fresh or a reused connection. We learn
//! it the only honest way: the pool calls its *connector* exactly once per new
//! connection and never for a reused one, so a thin [`CountingConnector`] that
//! increments a counter on each [`Service::call`] turns "connections opened" into
//! a number we can compare against "requests dispatched". A cluster whose opens
//! stay far below its dispatches is amortizing handshakes, pool reuse, verified.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper::Uri;
use tower_service::Service;

/// A connector that counts how many connections its inner connector opens.
///
/// Wraps any pooled-client connector (e.g. `HttpConnector`), sharing an atomic
/// open-count with the owning pool. `Clone` is required because the legacy
/// `Client` clones its connector; clones share the same counter.
#[derive(Clone)]
pub(crate) struct CountingConnector<C> {
    inner: C,
    opens: Arc<AtomicU64>,
}

impl<C> CountingConnector<C> {
    /// Wraps `inner`, incrementing `opens` on each new connection it opens.
    pub(crate) fn new(inner: C, opens: Arc<AtomicU64>) -> Self {
        Self { inner, opens }
    }
}

impl<C> Service<Uri> for CountingConnector<C>
where
    C: Service<Uri>,
    C::Future: Send + 'static,
{
    type Response = C::Response;
    type Error = C::Error;
    type Future = Pin<Box<dyn Future<Output = Result<C::Response, C::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        // The pool calls the connector only when it needs a *new* connection;
        // a reused pooled connection never reaches here, so this is the true
        // count of TCP (and TLS) handshakes performed for this cluster.
        self.opens.fetch_add(1, Ordering::Relaxed);
        Box::pin(self.inner.call(dst))
    }
}

/// A snapshot of one cluster pool's connection-reuse counters.
///
/// `opened` is the number of TCP connections actually established; `dispatched`
/// is the number of requests sent. The gap is reuse: `dispatched - opened`
/// requests rode an already-open pooled connection (NFR-P).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PoolStats {
    /// Connections the pool opened to the cluster (cold handshakes).
    pub opened: u64,
    /// Requests dispatched to the cluster (cold + reused).
    pub dispatched: u64,
}

impl PoolStats {
    /// Requests that rode a reused pooled connection.
    #[must_use]
    pub fn reused(&self) -> u64 {
        self.dispatched.saturating_sub(self.opened)
    }
}
