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

use std::sync::Arc;

use krafka::producer::Producer as KrafkaInner;
use osproxy_kafka::{ProduceError, Producer};
use tokio::runtime::Handle;

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

/// A [`Producer`] that sends each envelope to Kafka via `krafka`. Producing is
/// fire-and-forget: the record is enqueued onto the runtime and the forwarded
/// request is never blocked or failed by Kafka.
///
/// krafka resolves the partition from the topic and the key, so unlike the
/// per-partition rskafka binding one producer serves the whole topic; ordering
/// is preserved per key within a partition.
pub struct KrafkaProducer {
    inner: Arc<KrafkaInner>,
    handle: Handle,
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
            handle: Handle::current(),
        })
    }
}

impl Producer for KrafkaProducer {
    fn produce(&self, topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ProduceError> {
        let inner = Arc::clone(&self.inner);
        let topic = topic.to_owned();
        let key = key.to_vec();
        let payload = payload.to_vec();
        // Fire-and-forget on the captured runtime handle (spawn-discipline: never a
        // bare tokio::spawn). A produce failure must not break the request; a
        // future revision can add a bounded retry/buffer for at-least-once.
        self.handle.spawn(async move {
            let _ = inner.send(&topic, Some(&key), &payload).await;
        });
        Ok(())
    }
}
