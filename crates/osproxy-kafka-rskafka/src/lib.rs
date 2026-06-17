//! A portable, pure-Rust Kafka [`Producer`] over `rskafka`, suitable for both
//! FIPS and non-FIPS deployments.
//!
//! There is no native dependency (no librdkafka, no OpenSSL), so this builds and
//! runs anywhere the proxy does. TLS to the brokers goes through rustls, and the
//! crypto provider is selected at build time exactly like the rest of osproxy:
//! `ring` under the `non-fips` feature, the validated **aws-lc-rs** FIPS module
//! under `fips`. So one client serves both targets: for non-FIPS you get ring,
//! and for FIPS the TLS to Kafka runs through the same validated module as the
//! proxy's ingress and upstream TLS.
//!
//! ## FIPS caveat (read before relying on it)
//!
//! Under `fips` the *runtime* crypto uses aws-lc-rs (the provider you build with
//! [`crypto_provider`]), so every TLS operation goes through the validated
//! module. But rskafka 0.6 links `rustls` with its `ring` feature
//! unconditionally, so a FIPS build also *links* ring even though nothing calls
//! it. If your policy only requires that cryptographic operations use the
//! validated module, this is fine. If it requires a ring-absent binary (the
//! boundary osproxy enforces for its own artifact), rskafka 0.6 cannot meet it
//! yet, and you would use the OpenSSL-FIPS path (`osproxy-kafka-rdkafka` against
//! a FIPS OpenSSL) instead, or wait for upstream rskafka to make the provider
//! configurable.
//!
//! ## Composing it in
//!
//! ```ignore
//! use std::sync::Arc;
//! use osproxy_kafka::KafkaCapture;
//! use osproxy_capture::RedactingCapture;
//! use osproxy_kafka_rskafka::{crypto_provider, RsKafkaProducer};
//!
//! let tls = Arc::new(
//!     rustls::ClientConfig::builder_with_provider(crypto_provider())
//!         .with_safe_default_protocol_versions()?
//!         .with_root_certificates(roots)
//!         .with_no_client_auth(),
//! );
//! let producer =
//!     RsKafkaProducer::connect(vec!["broker-1:9092".to_owned()], "osproxy.capture", 0, Some(tls))
//!         .await?;
//! let handler = app_handler.with_capture(Box::new(RedactingCapture::new(
//!     KafkaCapture::new(producer, "osproxy.capture"),
//! )));
//! ```
#![deny(missing_docs)]

use std::sync::Arc;

use chrono::Utc;
use osproxy_kafka::{ProduceError, Producer};
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use tokio::runtime::Handle;

/// The rustls crypto provider this build links: `ring` under `non-fips`, the
/// validated aws-lc-rs FIPS module under `fips`. Use it to build the
/// `rustls::ClientConfig` you hand to [`RsKafkaProducer::connect`], so the Kafka
/// link uses the same crypto module as the rest of the proxy.
#[cfg(all(feature = "non-fips", not(feature = "fips")))]
#[must_use]
pub fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The rustls crypto provider this build links (the FIPS aws-lc-rs module).
#[cfg(feature = "fips")]
#[must_use]
pub fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::default_fips_provider())
}

/// A [`Producer`] that sends each envelope to one Kafka topic+partition via
/// `rskafka`. Producing is fire-and-forget: the record is enqueued onto the
/// runtime and the forwarded request is never blocked or failed by Kafka.
///
/// Bound to a single topic and partition resolved at [`RsKafkaProducer::connect`]
/// time (a capture stream is one topic; ordering is preserved within a partition).
/// For key-based partitioning, run one producer per partition or extend this.
pub struct RsKafkaProducer {
    partition: Arc<PartitionClient>,
    handle: Handle,
}

impl std::fmt::Debug for RsKafkaProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RsKafkaProducer").finish_non_exhaustive()
    }
}

impl RsKafkaProducer {
    /// Connects to `brokers`, optionally over `tls`, and resolves the client for
    /// `topic`/`partition`. Must be called from within a Tokio runtime (the
    /// fire-and-forget produce reuses that runtime's handle).
    ///
    /// # Errors
    ///
    /// Returns [`ProduceError`] if the client or the partition client cannot be
    /// built (e.g. the brokers are unreachable).
    pub async fn connect(
        brokers: Vec<String>,
        topic: &str,
        partition: i32,
        tls: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self, ProduceError> {
        let mut builder = ClientBuilder::new(brokers);
        if let Some(tls) = tls {
            builder = builder.tls_config(tls);
        }
        let client = builder.build().await.map_err(|_| ProduceError {
            reason: "connecting to the kafka brokers",
        })?;
        let partition = client
            .partition_client(topic.to_owned(), partition, UnknownTopicHandling::Retry)
            .await
            .map_err(|_| ProduceError {
                reason: "resolving the kafka topic partition",
            })?;
        Ok(Self {
            partition: Arc::new(partition),
            handle: Handle::current(),
        })
    }
}

impl Producer for RsKafkaProducer {
    fn produce(&self, _topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ProduceError> {
        let partition = Arc::clone(&self.partition);
        let record = Record {
            key: Some(key.to_vec()),
            value: Some(payload.to_vec()),
            headers: std::collections::BTreeMap::new(),
            timestamp: Utc::now(),
        };
        // Fire-and-forget on the captured runtime handle (spawn-discipline: never a
        // bare tokio::spawn). A produce failure must not break the request; a
        // future revision can add a bounded retry/buffer for at-least-once.
        self.handle.spawn(async move {
            let _ = partition
                .produce(vec![record], Compression::NoCompression)
                .await;
        });
        Ok(())
    }
}
