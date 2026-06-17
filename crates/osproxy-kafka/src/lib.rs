//! Queue-backed traffic capture.
//!
//! Implements the [`Capture`] seam by serializing each exchange to a stable
//! [`CaptureEnvelope`] (the replay wire format) and handing it to a [`Producer`].
//! The producer is the swappable piece: ship the captured stream to Kafka, a
//! file, or anywhere.
//!
//! **No broker dependency lives here.** This crate provides the envelope, the
//! `Producer` seam, and an in-memory producer; the Kafka (or other) client
//! composes in as your own `Producer` impl, so the heavy native client is never
//! forced into the build. A Kafka binding is a few lines over `rdkafka`:
//!
//! ```ignore
//! use osproxy_kafka::{ProduceError, Producer};
//! use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _};
//!
//! struct RdKafkaProducer(BaseProducer);
//!
//! impl Producer for RdKafkaProducer {
//!     fn produce(&self, topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ProduceError> {
//!         self.0
//!             .send(BaseRecord::to(topic).key(key).payload(payload))
//!             .map_err(|_| ProduceError { reason: "enqueueing the kafka record" })?;
//!         self.0.poll(std::time::Duration::from_millis(0));
//!         Ok(())
//!     }
//! }
//! ```
#![deny(missing_docs)]

use std::sync::{Arc, Mutex};

use osproxy_capture::{Capture, CaptureRecord};
use serde::{Deserialize, Serialize};

/// The serializable form of a captured exchange: the replay wire format a
/// downstream replayer consumes. Stable and self-contained (no borrows), so it
/// can be produced to a queue and read back later. Bodies are kept verbatim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureEnvelope {
    /// The proxy's correlation id for the exchange.
    pub request_id: String,
    /// The request method, e.g. `"POST"`.
    pub method: String,
    /// The request path.
    pub path: String,
    /// The request query string without the `?`, if any.
    pub query: Option<String>,
    /// The request headers, in order.
    pub headers: Vec<(String, String)>,
    /// The raw request body.
    pub body: Vec<u8>,
    /// The status the proxy returned.
    pub response_status: u16,
    /// The raw response body.
    pub response_body: Vec<u8>,
}

impl CaptureEnvelope {
    /// Builds an envelope from a borrowed capture record.
    #[must_use]
    pub fn from_record(record: &CaptureRecord<'_>) -> Self {
        Self {
            request_id: record.request_id.to_owned(),
            method: format!("{:?}", record.method).to_uppercase(),
            path: record.path.to_owned(),
            query: record.query.map(str::to_owned),
            headers: record.headers.to_vec(),
            body: record.body.to_vec(),
            response_status: record.response_status,
            response_body: record.response_body.to_vec(),
        }
    }
}

/// A failure to produce a record to the queue.
#[derive(Clone, Debug)]
pub struct ProduceError {
    /// A short, shape-only reason (never a captured value).
    pub reason: &'static str,
}

/// Where envelopes are sent. The swappable backend: Kafka, a file, a mock.
///
/// Implementations MUST NOT panic. `produce` is best-effort and non-blocking from
/// the caller's view; durability/retry is the producer's concern.
pub trait Producer: Send + Sync {
    /// Produces one serialized envelope under `key` (the request id) to `topic`.
    ///
    /// # Errors
    ///
    /// Returns [`ProduceError`] if the record could not be enqueued.
    fn produce(&self, topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ProduceError>;
}

/// A [`Capture`] that serializes each exchange to a [`CaptureEnvelope`] and
/// produces it to `topic` through a [`Producer`]. Generic over the producer, so
/// the broker client composes in.
#[derive(Clone, Debug)]
pub struct KafkaCapture<P> {
    producer: P,
    topic: String,
}

impl<P> KafkaCapture<P> {
    /// Captures to `topic` through `producer`.
    pub fn new(producer: P, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }
}

impl<P: Producer> Capture for KafkaCapture<P> {
    fn capture(&self, record: &CaptureRecord<'_>) {
        let envelope = CaptureEnvelope::from_record(record);
        let Ok(payload) = serde_json::to_vec(&envelope) else {
            return;
        };
        // Best-effort: a produce failure must never break the forwarded request.
        // A production producer buffers/retries for at-least-once delivery.
        let _ = self
            .producer
            .produce(&self.topic, envelope.request_id.as_bytes(), &payload);
    }
}

/// One produced record: `(topic, key, payload)`.
pub type Produced = (String, Vec<u8>, Vec<u8>);

/// A reference [`Producer`] that keeps produced records in memory, for tests and
/// for composing a `KafkaCapture` without a broker.
#[derive(Clone, Default, Debug)]
pub struct InMemoryProducer {
    produced: Arc<Mutex<Vec<Produced>>>,
}

impl InMemoryProducer {
    /// An empty producer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The `(topic, key, payload)` tuples produced so far, oldest first.
    #[must_use]
    pub fn produced(&self) -> Vec<Produced> {
        self.produced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Producer for InMemoryProducer {
    fn produce(&self, topic: &str, key: &[u8], payload: &[u8]) -> Result<(), ProduceError> {
        self.produced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((topic.to_owned(), key.to_vec(), payload.to_vec()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_capture::CaptureRecord;
    use osproxy_capture::HttpMethod;

    fn record<'a>(headers: &'a [(String, String)], rid: &'a str) -> CaptureRecord<'a> {
        CaptureRecord {
            request_id: rid,
            method: HttpMethod::Post,
            path: "/orders/_doc/1",
            query: Some("refresh=true"),
            headers,
            body: br#"{"id":7}"#,
            response_status: 201,
            response_body: br#"{"result":"created"}"#,
        }
    }

    #[test]
    fn the_envelope_round_trips_through_json() {
        let headers = vec![("content-type".to_owned(), "application/json".to_owned())];
        let env = CaptureEnvelope::from_record(&record(&headers, "r1"));
        let bytes = serde_json::to_vec(&env).unwrap();
        let back: CaptureEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, env);
        assert_eq!(back.method, "POST");
        assert_eq!(back.body, br#"{"id":7}"#);
        assert_eq!(back.response_status, 201);
    }

    #[test]
    fn kafka_capture_produces_one_envelope_per_exchange() {
        let producer = InMemoryProducer::new();
        let capture = KafkaCapture::new(producer.clone(), "osproxy.capture");
        let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
        capture.capture(&record(&headers, "r1"));
        capture.capture(&record(&headers, "r2"));

        let sent = producer.produced();
        assert_eq!(sent.len(), 2);
        let (topic, key, payload) = &sent[0];
        assert_eq!(topic, "osproxy.capture");
        assert_eq!(key, b"r1", "the request id is the partition key");
        let env: CaptureEnvelope = serde_json::from_slice(payload).unwrap();
        assert_eq!(env.path, "/orders/_doc/1");
    }
}
