//! A durable spill buffer for capture: a [`Producer`] that persists every record
//! to a disk write-ahead log before a background drainer delivers it to an
//! [`AckProducer`], advancing an on-disk checkpoint only once the broker
//! acknowledges. Undelivered records survive a process restart and replay, so
//! delivery is **at-least-once** rather than the in-memory best-effort of the bare
//! producer.
//!
//! ## Guarantees and the honest caveats
//!
//! - **Survives restart / broker outage.** Records sit on disk until acknowledged;
//!   the drainer retries with capped backoff forever, and resumes from the
//!   checkpoint after a restart.
//! - **At-least-once, not exactly-once.** A crash in the window between the broker
//!   ack and the checkpoint write replays the record, so the consumer must
//!   tolerate duplicates (dedupe on the request id the capture envelope carries).
//! - **Group-commit durability.** Appends are not fsynced per record (that would
//!   stall the request path); the log is fsynced on a timer and when idle. A hard
//!   power loss can lose the last sub-second of *appended-but-undelivered*
//!   records. A graceful restart loses nothing.
//! - **Bounded disk.** Past `max_bytes` of undelivered records, new appends are
//!   dropped (the buffer is full), exactly like the in-memory cap.
//!
//! ## Composing it in
//!
//! ```ignore
//! use osproxy_kafka::KafkaCapture;
//! use osproxy_kafka_wal::{DurableProducer, WalConfig};
//!
//! // `krafka` is an AckProducer (it awaits the broker ack).
//! let durable = DurableProducer::spawn("/var/lib/osproxy/capture", krafka, WalConfig::default())?;
//! let capture = KafkaCapture::new(durable, "osproxy.capture");
//! ```
#![deny(missing_docs)]

use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use osproxy_kafka::{AckProducer, ProduceError, Producer};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
// The runtime clock (not the determinism-banned `std::time::Instant`); it also
// advances under tokio's paused-time test clock.
use tokio::time::Instant;

mod segment;
use segment::Wal;

/// Tuning for the durable buffer.
#[derive(Clone, Copy, Debug)]
pub struct WalConfig {
    /// Cap on undelivered records on disk; an append past it is dropped.
    pub max_bytes: u64,
    /// Reclaim the acknowledged prefix once it grows past this many bytes.
    pub compact_threshold: u64,
    /// The first drain-retry backoff after a failed send; doubles up to the cap.
    pub base_backoff: Duration,
    /// The ceiling on the drain-retry backoff.
    pub max_backoff: Duration,
    /// How often to fsync the log while draining (the group-commit interval).
    pub sync_interval: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024,
            compact_threshold: 8 * 1024 * 1024,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            sync_interval: Duration::from_millis(200),
        }
    }
}

/// A [`Producer`] that durably spools records to disk and drains them to an inner
/// [`AckProducer`]. Producing appends to the log and returns immediately; the
/// background drainer owns delivery and retry. See the module docs for the
/// at-least-once guarantee and its caveats.
pub struct DurableProducer {
    state: Arc<Mutex<Wal>>,
    notify: Arc<Notify>,
    drainer: JoinHandle<()>,
}

impl std::fmt::Debug for DurableProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableProducer").finish_non_exhaustive()
    }
}

impl DurableProducer {
    /// Opens (or recovers) the log under `dir` and spawns the drainer that
    /// delivers to `ack`. Must be called from within a Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the log directory or files cannot be opened.
    pub fn spawn<A: AckProducer>(
        dir: impl AsRef<Path>,
        ack: A,
        cfg: WalConfig,
    ) -> std::io::Result<Self> {
        let wal = Wal::open(dir.as_ref(), cfg.max_bytes, cfg.compact_threshold)?;
        let state = Arc::new(Mutex::new(wal));
        let notify = Arc::new(Notify::new());
        // Spawn-discipline: drive the drainer off the captured runtime handle, not
        // a bare tokio::spawn.
        let drainer = Handle::current().spawn(drain(
            Arc::clone(&state),
            Arc::clone(&notify),
            Arc::new(ack),
            cfg,
        ));
        Ok(Self {
            state,
            notify,
            drainer,
        })
    }
}

impl Drop for DurableProducer {
    fn drop(&mut self) {
        // Stop the drainer; undelivered records stay on disk for the next run.
        self.drainer.abort();
    }
}

impl Producer for DurableProducer {
    fn produce(&self, topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ProduceError> {
        let appended = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .append(topic, key, payload);
        appended.map_err(|()| ProduceError {
            reason: "capture WAL full, record dropped",
        })?;
        // Wake the drainer; it may be parked waiting for work.
        self.notify.notify_one();
        Ok(())
    }
}

/// Drains the log to `ack` forever: deliver the next record, advance the
/// checkpoint on ack, retry with capped backoff on failure, and fsync/compact
/// while idle or on the group-commit interval.
async fn drain<A: AckProducer>(
    state: Arc<Mutex<Wal>>,
    notify: Arc<Notify>,
    ack: Arc<A>,
    cfg: WalConfig,
) {
    let mut backoff = cfg.base_backoff;
    let mut last_sync = Instant::now();
    loop {
        let next = lock(&state).next();
        let Some(record) = next else {
            // Caught up: make undelivered appends durable, reclaim disk, then park
            // until a new record arrives or the sync interval elapses.
            {
                let mut wal = lock(&state);
                wal.sync();
                let _ = wal.maybe_compact();
            }
            last_sync = Instant::now();
            tokio::select! {
                () = notify.notified() => {}
                () = tokio::time::sleep(cfg.sync_interval) => {}
            }
            continue;
        };

        let sent = ack
            .send_acked(&record.topic, &record.key, &record.payload)
            .await;
        if sent.is_ok() {
            let mut wal = lock(&state);
            wal.commit(record.next);
            // Bound the group-commit window even under sustained load.
            if last_sync.elapsed() >= cfg.sync_interval {
                wal.sync();
                let _ = wal.maybe_compact();
                last_sync = Instant::now();
            }
            backoff = cfg.base_backoff;
        } else {
            // The record is still on disk; wait and retry, never dropping it.
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(cfg.max_backoff);
        }
    }
}

fn lock(state: &Mutex<Wal>) -> std::sync::MutexGuard<'_, Wal> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
#[path = "wal_tests.rs"]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod wal_tests;
