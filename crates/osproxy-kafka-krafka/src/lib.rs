//! A portable, pure-Rust Kafka [`Producer`] over `krafka`, suitable for both
//! FIPS and non-FIPS deployments without changing clients.
//!
//! There is no native dependency (no librdkafka, no OpenSSL), so this builds and
//! runs anywhere the proxy does. TLS to the brokers goes through rustls, and the
//! crypto provider is selected at build time exactly like the rest of osproxy:
//! `ring` under the `non-fips` feature, the validated **aws-lc-rs** FIPS module
//! under `fips`.
//!
//! ## Why krafka over a hardcoded-`ring` client
//!
//! krafka declares `rustls` with default features off and gates the provider
//! purely through cargo features (`krafka/ring` → `rustls/ring`,
//! `krafka/rustls-aws-lc-rs` → `rustls/aws_lc_rs`). So a `fips` build enables
//! only the aws-lc-rs path and **ring is never linked** -- the ring-absent
//! boundary osproxy enforces for its own artifact. krafka itself never installs
//! a process default; it builds its `rustls::ClientConfig` from the
//! [`install_crypto_provider`]-installed default, so we hand it the FIPS provider
//! and every TLS operation runs through the validated module.
//!
//! ## Composing it in
//!
//! ```ignore
//! use osproxy_kafka::KafkaCapture;
//! use osproxy_capture::RedactingCapture;
//! use osproxy_kafka_krafka::{AuthConfig, KrafkaProducer, TlsConfig};
//!
//! // From within a Tokio runtime. Installs the build-selected crypto provider,
//! // then connects (optionally over TLS via krafka's file-based TlsConfig).
//! let tls = TlsConfig::new()
//!     .with_ca_cert("/etc/osproxy/kafka-ca.pem")
//!     .with_kafka_alpn();
//! let producer = KrafkaProducer::connect(
//!     vec!["broker-1:9092".to_owned()],
//!     "osproxy-capture",
//!     Some(AuthConfig::ssl(tls)),
//! )
//! .await?;
//! let handler = app_handler.with_capture(Box::new(RedactingCapture::new(
//!     KafkaCapture::new(producer, "osproxy.capture"),
//! )));
//! ```
#![deny(missing_docs)]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use krafka::producer::Producer as KrafkaInner;
use osproxy_kafka::{ProduceError, Producer};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

// Re-export the knobs an operator composes a TLS/SASL connection from, so callers
// need not depend on `krafka` directly.
pub use krafka::auth::{AuthConfig, TlsConfig};

/// Installs the rustls crypto provider this build links as the process default:
/// `ring` under `non-fips`, the validated aws-lc-rs FIPS module under `fips`.
///
/// krafka resolves its `ClientConfig` from the installed default provider and
/// never installs one itself, so this is the single point that decides which
/// crypto module the Kafka TLS link uses. It is idempotent and a no-op if a
/// provider is already installed; [`KrafkaProducer::connect`] calls it for you.
#[cfg(all(feature = "non-fips", not(feature = "fips")))]
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Installs the rustls crypto provider this build links (the FIPS aws-lc-rs
/// module) as the process default. See the `non-fips` variant for details.
#[cfg(feature = "fips")]
pub fn install_crypto_provider() {
    let _ = rustls::crypto::default_fips_provider().install_default();
}

/// How hard the producer tries to deliver each record, and how much it will
/// buffer while trying. This is *bounded, in-memory* best-effort: it rides out a
/// transient broker blip with a few retries, and caps how many records are in
/// flight so a sustained outage drops the overflow rather than growing without
/// bound. It is **not** durable across a process restart; for that, the consumer
/// of the capture topic must tolerate gaps, or a durable queue sits in front.
#[derive(Clone, Copy, Debug)]
pub struct DeliveryConfig {
    /// The most records in flight (buffered + retrying) at once. A `produce` that
    /// would exceed this is dropped rather than queued, bounding memory.
    pub max_inflight: usize,
    /// Total send attempts for one record before giving up (1 = no retry).
    pub max_attempts: u32,
    /// The delay before the first retry; it doubles after each failed attempt.
    pub base_backoff: Duration,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            max_inflight: 1024,
            max_attempts: 4,
            base_backoff: Duration::from_millis(50),
        }
    }
}

/// Bounds in-flight produces and runs the retry/backoff loop. Factored out of the
/// broker client so the delivery policy is unit-testable without a broker.
struct Delivery {
    inflight: Arc<Semaphore>,
    cfg: DeliveryConfig,
    handle: Handle,
}

impl Delivery {
    fn new(cfg: DeliveryConfig, handle: Handle) -> Self {
        Self {
            inflight: Arc::new(Semaphore::new(cfg.max_inflight)),
            cfg,
            handle,
        }
    }

    /// Acquires an in-flight slot and spawns the retrying delivery of one record.
    /// `send` is called once per attempt and yields `Ok(())` on a successful
    /// produce. Returns `ProduceError` (record dropped) when the buffer is full.
    fn spawn<F, Fut>(&self, send: F) -> Result<(), ProduceError>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ()>> + Send,
    {
        let permit = Arc::clone(&self.inflight)
            .try_acquire_owned()
            .map_err(|_| ProduceError {
                reason: "capture buffer saturated, record dropped",
            })?;
        let cfg = self.cfg;
        self.handle.spawn(async move {
            let _permit = permit; // released when delivery ends, freeing the slot
            deliver(send, cfg).await;
        });
        Ok(())
    }
}

/// Retries `send` up to `cfg.max_attempts`, sleeping a doubling backoff between
/// attempts. Returns once a send succeeds or the budget is exhausted.
async fn deliver<F, Fut>(send: F, cfg: DeliveryConfig)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), ()>>,
{
    let mut backoff = cfg.base_backoff;
    for attempt in 1..=cfg.max_attempts {
        if send().await.is_ok() {
            return;
        }
        if attempt < cfg.max_attempts {
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2);
        }
    }
}

/// A [`Producer`] that sends each envelope to Kafka via `krafka`. Producing is
/// fire-and-forget from the caller's view: the record is handed to a bounded
/// in-memory delivery worker ([`DeliveryConfig`]) and the forwarded request is
/// never blocked or failed by Kafka.
///
/// krafka resolves the partition from the topic and the key, so unlike the
/// per-partition rskafka binding one producer serves the whole topic; ordering
/// is preserved per key within a partition.
pub struct KrafkaProducer {
    inner: Arc<KrafkaInner>,
    delivery: Delivery,
}

impl std::fmt::Debug for KrafkaProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KrafkaProducer").finish_non_exhaustive()
    }
}

impl KrafkaProducer {
    /// Installs the build-selected crypto provider, then connects to `brokers`
    /// with `client_id`, optionally authenticated/encrypted via `auth` (use
    /// [`AuthConfig::ssl`] with a [`TlsConfig`] for TLS, or the SASL
    /// constructors). Must be called from within a Tokio runtime: the
    /// fire-and-forget produce reuses that runtime's handle.
    ///
    /// # Errors
    ///
    /// Returns [`ProduceError`] if the producer cannot be built (e.g. the
    /// brokers are unreachable or the TLS material cannot be loaded).
    pub async fn connect(
        brokers: Vec<String>,
        client_id: &str,
        auth: Option<AuthConfig>,
    ) -> Result<Self, ProduceError> {
        install_crypto_provider();
        let mut builder = KrafkaInner::builder()
            .bootstrap_servers(brokers.join(","))
            .client_id(client_id);
        if let Some(auth) = auth {
            builder = builder.auth(auth);
        }
        let inner = builder.build().await.map_err(|_| ProduceError {
            reason: "connecting to the kafka brokers",
        })?;
        Ok(Self {
            inner: Arc::new(inner),
            delivery: Delivery::new(DeliveryConfig::default(), Handle::current()),
        })
    }

    /// Overrides the default [`DeliveryConfig`] (retry budget and in-flight bound).
    #[must_use]
    pub fn with_delivery(mut self, cfg: DeliveryConfig) -> Self {
        self.delivery = Delivery::new(cfg, self.delivery.handle.clone());
        self
    }
}

impl Producer for KrafkaProducer {
    fn produce(&self, topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ProduceError> {
        let inner = Arc::clone(&self.inner);
        let topic = topic.to_owned();
        let key = key.to_vec();
        let payload = payload.to_vec();
        // Spawn-discipline: the delivery worker captures the runtime handle, never a
        // bare tokio::spawn. Each attempt re-borrows owned copies so the produce
        // future is 'static; a saturated buffer drops the record (bounded memory).
        self.delivery.spawn(move || {
            let inner = Arc::clone(&inner);
            let topic = topic.clone();
            let key = key.clone();
            let payload = payload.clone();
            async move {
                inner
                    .send(&topic, Some(&key), &payload)
                    .await
                    .map(|_| ())
                    .map_err(|_| ())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn cfg(max_attempts: u32, max_inflight: usize) -> DeliveryConfig {
        DeliveryConfig {
            max_inflight,
            max_attempts,
            base_backoff: Duration::from_millis(10),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deliver_stops_on_the_first_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        deliver(
            move || {
                let c = Arc::clone(&c);
                async move {
                    // Fail twice, then succeed on the third attempt.
                    let n = c.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        Err(())
                    } else {
                        Ok(())
                    }
                }
            },
            cfg(5, 1),
        )
        .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "stops once a send succeeds"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deliver_gives_up_after_the_attempt_budget() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        deliver(
            move || {
                let c = Arc::clone(&c);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err(())
                }
            },
            cfg(4, 1),
        )
        .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "exactly max_attempts sends, then gives up"
        );
    }

    #[tokio::test]
    async fn spawn_drops_when_the_buffer_is_saturated() {
        let delivery = Delivery::new(cfg(1, 1), Handle::current());
        // Occupy the single in-flight slot with a send that never completes, so the
        // permit stays held.
        let first = delivery.spawn(std::future::pending::<Result<(), ()>>);
        assert!(first.is_ok(), "the first record takes the only slot");
        // With no slot free, the next produce is dropped rather than queued.
        let second = delivery.spawn(|| async { Ok(()) });
        assert!(second.is_err(), "a saturated buffer drops the record");
    }

    #[tokio::test]
    async fn spawn_frees_the_slot_after_delivery() {
        let delivery = Delivery::new(cfg(1, 1), Handle::current());
        delivery.spawn(|| async { Ok(()) }).expect("first accepted");
        // Let the delivery task run to completion and release its permit.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(
            delivery.spawn(|| async { Ok(()) }).is_ok(),
            "the slot is reusable once delivery finishes"
        );
    }
}
