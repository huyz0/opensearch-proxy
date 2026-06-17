use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

/// One recorded delivery: `(topic, key, payload)`.
type Delivered = (String, Vec<u8>, Vec<u8>);

/// A tunable acknowledging producer: records deliveries and can fail the first N
/// sends to exercise the retry path.
#[derive(Clone, Default)]
struct MockAck {
    delivered: Arc<Mutex<Vec<Delivered>>>,
    fail_first: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
}

impl AckProducer for MockAck {
    async fn send_acked(
        &self,
        topic: &str,
        key: &[u8],
        payload: &[u8],
    ) -> Result<(), ProduceError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.fail_first.load(Ordering::SeqCst) > 0 {
            self.fail_first.fetch_sub(1, Ordering::SeqCst);
            return Err(ProduceError {
                reason: "mock failure",
            });
        }
        self.delivered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((topic.to_owned(), key.to_vec(), payload.to_vec()));
        Ok(())
    }
}

/// An acknowledging producer that never succeeds: records stay on disk.
struct NeverAck;

impl AckProducer for NeverAck {
    async fn send_acked(&self, _t: &str, _k: &[u8], _p: &[u8]) -> Result<(), ProduceError> {
        Err(ProduceError {
            reason: "never acks",
        })
    }
}

/// A unique temp directory cleaned up on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        static N: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "osproxy-wal-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn fast_cfg() -> WalConfig {
    WalConfig {
        max_bytes: 1 << 20,
        compact_threshold: 256,
        base_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
        sync_interval: Duration::from_millis(1),
    }
}

/// Polls `cond` until true, or panics after a generous timeout.
async fn wait_until<F: Fn() -> bool>(cond: F) {
    for _ in 0..2000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition not met within the timeout");
}

fn delivered_keys(ack: &MockAck) -> Vec<String> {
    ack.delivered
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .map(|(_, k, _)| String::from_utf8_lossy(k).into_owned())
        .collect()
}

#[tokio::test]
async fn delivers_every_record_in_order() {
    let dir = TempDir::new();
    let ack = MockAck::default();
    let producer = DurableProducer::spawn(&dir.path, ack.clone(), fast_cfg()).unwrap();
    for i in 0..5 {
        producer
            .produce("topic", format!("k{i}").as_bytes(), b"v")
            .unwrap();
    }
    wait_until(|| delivered_keys(&ack).len() == 5).await;
    assert_eq!(delivered_keys(&ack), ["k0", "k1", "k2", "k3", "k4"]);
}

#[tokio::test]
async fn retries_until_the_broker_acks() {
    let dir = TempDir::new();
    let ack = MockAck::default();
    ack.fail_first.store(3, Ordering::SeqCst); // fail the first three sends
    let producer = DurableProducer::spawn(&dir.path, ack.clone(), fast_cfg()).unwrap();
    producer.produce("topic", b"k", b"v").unwrap();
    wait_until(|| delivered_keys(&ack).len() == 1).await;
    assert!(
        ack.attempts.load(Ordering::SeqCst) >= 4,
        "the record is retried past the failures, never dropped"
    );
}

#[tokio::test]
async fn undelivered_records_survive_a_restart() {
    let dir = TempDir::new();
    // First "run": nothing is ever acknowledged, so both records stay on disk.
    {
        let producer = DurableProducer::spawn(&dir.path, NeverAck, fast_cfg()).unwrap();
        producer.produce("topic", b"k1", b"v1").unwrap();
        producer.produce("topic", b"k2", b"v2").unwrap();
        // Let the drainer attempt (and fail) a few times, then drop → abort.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // "Restart": reopen the same directory with a recording producer; the
    // checkpoint never advanced, so both records replay.
    let ack = MockAck::default();
    let _producer = DurableProducer::spawn(&dir.path, ack.clone(), fast_cfg()).unwrap();
    wait_until(|| delivered_keys(&ack).len() == 2).await;
    assert_eq!(delivered_keys(&ack), ["k1", "k2"]);
}

#[tokio::test]
async fn a_full_buffer_drops_new_records() {
    let dir = TempDir::new();
    let cfg = WalConfig {
        max_bytes: 64,
        ..fast_cfg()
    };
    // NeverAck so the log fills and nothing is reclaimed.
    let producer = DurableProducer::spawn(&dir.path, NeverAck, cfg).unwrap();
    let payload = vec![b'x'; 40];
    assert!(
        producer.produce("t", b"k", &payload).is_ok(),
        "the first record fits under the cap"
    );
    assert!(
        producer.produce("t", b"k", &payload).is_err(),
        "the second record exceeds the cap and is dropped"
    );
}
