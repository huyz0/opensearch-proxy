//! The etcd network paths: initial connect + read, and the background watch that
//! keeps the cached snapshot fresh. Split from [`super`] because these can only be
//! exercised against a live etcd (the Docker-gated `tests/etcd_live.rs`), so they
//! are excluded from unit-coverage by filename like the other live harnesses.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use etcd_client::{Client, EventType};
use osproxy_core::Clock;
use osproxy_observe::{decode_directive_set, DirectiveSet};

use super::{EtcdDirectiveStore, EtcdError};

/// How long the watch task waits before reconnecting after the etcd stream ends
/// or errors. Bounded so a flapping control plane cannot spin the task hot; a
/// production adapter would jitter this.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

impl EtcdDirectiveStore {
    /// Connects to `endpoints`, reads the directive set at `key`, and spawns the
    /// background watch that keeps it fresh.
    ///
    /// The value at `key` is the same JSON body the admin publish endpoint accepts
    /// (`{"directives":[...]}`). A missing key is a valid empty set (everything
    /// off). A *malformed* value at startup is treated as empty, fail-safe, the
    /// same as an unparseable later publish.
    ///
    /// # Errors
    /// [`EtcdError::Connect`] if etcd cannot be reached or the initial read fails.
    pub async fn connect(
        endpoints: &[String],
        key: impl Into<String>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, EtcdError> {
        let key = key.into();
        let mut client = Client::connect(endpoints, None).await?;
        let resp = client.get(key.clone(), None).await?;
        let initial = resp
            .kvs()
            .first()
            .and_then(|kv| decode_directive_set(kv.value(), clock.as_ref()).ok())
            .unwrap_or_default();
        let current = Arc::new(ArcSwap::from_pointee(initial));

        // Capture the runtime handle to spawn the watch (spawn discipline: never a
        // bare tokio::spawn in a library; mirror osproxy-otlp).
        let handle = tokio::runtime::Handle::current();
        let endpoints = endpoints.to_vec();
        let task_current = Arc::clone(&current);
        handle.spawn(watch_loop(endpoints, key, clock, task_current));

        Ok(Self::from_snapshot(current))
    }
}

/// Watches `key` forever, applying each update to `current`. Reconnects after a
/// bounded delay whenever the stream ends or etcd errors, so a transient outage
/// degrades to "serve the last snapshot" rather than losing fleet control.
async fn watch_loop(
    endpoints: Vec<String>,
    key: String,
    clock: Arc<dyn Clock>,
    current: Arc<ArcSwap<DirectiveSet>>,
) {
    loop {
        // A clean stream end or any error both fall through to the reconnect delay;
        // the snapshot is left untouched (last-good) across the gap.
        let _ = watch_once(&endpoints, &key, clock.as_ref(), &current).await;
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// One connect → watch → drain cycle. Returns when the stream ends or errors.
async fn watch_once(
    endpoints: &[String],
    key: &str,
    clock: &dyn Clock,
    current: &ArcSwap<DirectiveSet>,
) -> Result<(), etcd_client::Error> {
    let mut client = Client::connect(endpoints, None).await?;
    // Re-read once on (re)connect so an update missed during a disconnect is not
    // lost, then stream subsequent changes.
    let resp = client.get(key, None).await?;
    if let Some(kv) = resp.kvs().first() {
        super::apply_value(current, kv.value(), clock);
    }
    // The stream itself holds the watch open for the drain's lifetime.
    let mut stream = client.watch(key, None).await?;
    while let Some(resp) = stream.message().await? {
        for event in resp.events() {
            match event.event_type() {
                EventType::Put => {
                    if let Some(kv) = event.kv() {
                        super::apply_value(current, kv.value(), clock);
                    }
                }
                // A deleted key means "no directives": flip to the empty set.
                EventType::Delete => {
                    current.store(Arc::new(DirectiveSet::new()));
                }
            }
        }
    }
    Ok(())
}
