//! A librdkafka-backed [`Producer`](osproxy_kafka::Producer) for `osproxy-kafka`.
//!
//! This crate is **deliberately not a workspace member** (see the root
//! `Cargo.toml` `exclude` list). It links librdkafka, a heavy native library, so
//! the default build and CI never compile it. Build it on its own:
//!
//! ```text
//! cd crates/osproxy-kafka-rdkafka && cargo build
//! ```
//!
//! ## System requirements
//!
//! The `cmake-build` feature compiles librdkafka from source, which needs:
//!
//! - `cmake` and a C compiler (`cc`/`gcc`/`clang`)
//! - curl and OpenSSL development headers
//!   (`libcurl4-openssl-dev libssl-dev` on Debian/Ubuntu)
//!
//! ## Composing it in
//!
//! ```ignore
//! use osproxy_kafka::KafkaCapture;
//! use osproxy_kafka_rdkafka::RdKafkaProducer;
//! use osproxy_capture::RedactingCapture;
//!
//! let producer = RdKafkaProducer::new("broker-1:9092,broker-2:9092")?;
//! let capture = RedactingCapture::new(KafkaCapture::new(producer, "osproxy.capture"));
//! let handler = app_handler.with_capture(Box::new(capture));
//! ```

use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _};

use osproxy_kafka::{ProduceError, Producer};

/// A [`Producer`] that sends each envelope to Kafka via librdkafka.
///
/// Uses a [`BaseProducer`]: `produce` enqueues the record and serves the client's
/// delivery queue without blocking the forwarded request. Delivery is
/// at-least-once once enqueued; call [`RdKafkaProducer::flush`] on shutdown to
/// drain any in-flight records.
pub struct RdKafkaProducer {
    producer: BaseProducer,
}

impl RdKafkaProducer {
    /// Connects a producer to `brokers` (a comma-separated `host:port` list).
    ///
    /// # Errors
    ///
    /// Returns [`ProduceError`] if the client cannot be created.
    pub fn new(brokers: &str) -> Result<Self, ProduceError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .map_err(|_| ProduceError {
                reason: "creating the kafka producer",
            })?;
        Ok(Self { producer })
    }

    /// Drains in-flight records, blocking up to `timeout_ms`. Call on shutdown.
    pub fn flush(&self, timeout_ms: u64) {
        let _ = self
            .producer
            .flush(std::time::Duration::from_millis(timeout_ms));
    }
}

impl Producer for RdKafkaProducer {
    fn produce(&self, topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ProduceError> {
        self.producer
            .send(BaseRecord::to(topic).key(key).payload(payload))
            .map_err(|_| ProduceError {
                reason: "enqueueing the kafka record",
            })?;
        // Serve delivery callbacks without blocking; full drain happens on flush.
        self.producer.poll(std::time::Duration::from_millis(0));
        Ok(())
    }
}
