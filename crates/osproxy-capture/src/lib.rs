//! Full-fidelity traffic capture (tenant-agnostic).
//!
//! A capture proxy forwards each request to the upstream while recording the raw
//! request and response to a durable stream, so a replayer can later apply that
//! stream to another cluster (the OpenSearch Migration Assistant capture proxy).
//! This crate is the seam: [`Capture`] receives each exchange; a queue writer
//! (the `osproxy-kafka` crate), a file recorder, or the in-memory
//! [`MemoryCapture`] implement it. It pulls in no broker dependency, so the seam
//! is implementable from a leaf crate without dragging a Kafka client into the
//! default build.
//!
//! **This is not the shape-only telemetry.** Everything else osproxy records is
//! shapes/ids/names and safe to expose by construction. A capture record carries
//! the raw bodies and header *values*, tenant data, and any credential a
//! redaction layer did not strip. The capture stream is a privileged channel:
//! secure it (encryption, access control), and enable it deliberately, never by
//! default. Redaction is composed in via [`RedactingCapture`] rather than baked
//! into every recorder.
#![deny(missing_docs)]

use std::sync::{Arc, Mutex};

pub use osproxy_spi::HttpMethod;

/// One captured request/response exchange, borrowed for the duration of the
/// [`Capture::capture`] call. Full fidelity: bodies and header values included.
#[derive(Clone, Copy, Debug)]
pub struct CaptureRecord<'a> {
    /// The proxy's correlation id for the exchange.
    pub request_id: &'a str,
    /// The client request method.
    pub method: HttpMethod,
    /// The request path (e.g. `/orders/_doc/1`).
    pub path: &'a str,
    /// The request query string without the `?`, if any.
    pub query: Option<&'a str>,
    /// The request headers (subject to any composed redaction).
    pub headers: &'a [(String, String)],
    /// The raw request body.
    pub body: &'a [u8],
    /// The status the proxy returned to the client.
    pub response_status: u16,
    /// The raw response body.
    pub response_body: &'a [u8],
}

/// Receives one record per forwarded request. The recorder a capture proxy
/// provides: a queue writer implements this.
///
/// Implementations MUST NOT panic. `capture` is called inline after the response
/// is produced, so heavy delivery (a network write) belongs behind a background
/// queue; do the minimum here.
pub trait Capture: Send + Sync {
    /// Whether this capture will record. The handler checks it before assembling
    /// a record, so a disabled capture costs only this call.
    fn enabled(&self) -> bool {
        true
    }

    /// Records one exchange.
    fn capture(&self, record: &CaptureRecord<'_>);
}

/// The default: capture off. No record is assembled.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCapture;

impl Capture for NoCapture {
    fn enabled(&self) -> bool {
        false
    }
    fn capture(&self, _record: &CaptureRecord<'_>) {}
}

/// The header list with any `Authorization` header removed (case-insensitive).
#[must_use]
pub fn without_authorization(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("authorization"))
        .cloned()
        .collect()
}

/// Strips the `Authorization` header before the wrapped capture sees the record.
/// Composition: redaction is a layer over any recorder, not a feature each
/// recorder reimplements.
///
/// This removes the credential only. The bodies remain full-fidelity (a replay
/// needs them); securing the stream itself is the operator's responsibility.
#[derive(Clone, Copy, Debug, Default)]
pub struct RedactingCapture<C> {
    inner: C,
}

impl<C> RedactingCapture<C> {
    /// Wraps `inner` with `Authorization`-header redaction.
    pub fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C: Capture> Capture for RedactingCapture<C> {
    fn enabled(&self) -> bool {
        self.inner.enabled()
    }
    fn capture(&self, record: &CaptureRecord<'_>) {
        let safe_headers = without_authorization(record.headers);
        let redacted = CaptureRecord {
            headers: &safe_headers,
            ..*record
        };
        self.inner.capture(&redacted);
    }
}

/// An owned copy of a captured exchange, for the in-memory reference recorder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedCapture {
    /// See [`CaptureRecord::request_id`].
    pub request_id: String,
    /// See [`CaptureRecord::method`].
    pub method: HttpMethod,
    /// See [`CaptureRecord::path`].
    pub path: String,
    /// See [`CaptureRecord::query`].
    pub query: Option<String>,
    /// See [`CaptureRecord::headers`].
    pub headers: Vec<(String, String)>,
    /// See [`CaptureRecord::body`].
    pub body: Vec<u8>,
    /// See [`CaptureRecord::response_status`].
    pub response_status: u16,
    /// See [`CaptureRecord::response_body`].
    pub response_body: Vec<u8>,
}

impl OwnedCapture {
    /// Copies a borrowed record into an owned one.
    #[must_use]
    pub fn from_record(record: &CaptureRecord<'_>) -> Self {
        Self {
            request_id: record.request_id.to_owned(),
            method: record.method,
            path: record.path.to_owned(),
            query: record.query.map(str::to_owned),
            headers: record.headers.to_vec(),
            body: record.body.to_vec(),
            response_status: record.response_status,
            response_body: record.response_body.to_vec(),
        }
    }
}

/// A reference [`Capture`] that keeps exchanges in memory. For tests and local
/// inspection; a real deployment writes to a durable queue.
#[derive(Clone, Default, Debug)]
pub struct MemoryCapture {
    records: Arc<Mutex<Vec<OwnedCapture>>>,
}

impl MemoryCapture {
    /// An empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of everything captured so far, oldest first.
    #[must_use]
    pub fn records(&self) -> Vec<OwnedCapture> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Capture for MemoryCapture {
    fn capture(&self, record: &CaptureRecord<'_>) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(OwnedCapture::from_record(record));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(headers: &[(String, String)]) -> CaptureRecord<'_> {
        CaptureRecord {
            request_id: "r1",
            method: HttpMethod::Post,
            path: "/orders/_doc",
            query: None,
            headers,
            body: br#"{"tenant_id":"acme"}"#,
            response_status: 201,
            response_body: b"{}",
        }
    }

    #[test]
    fn the_default_capture_is_off() {
        assert!(!NoCapture.enabled());
        NoCapture.capture(&record(&[])); // no panic
    }

    #[test]
    fn memory_capture_keeps_full_fidelity_records() {
        let cap = MemoryCapture::new();
        let headers = vec![("content-type".to_owned(), "application/json".to_owned())];
        cap.capture(&record(&headers));
        let got = cap.records();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].path, "/orders/_doc");
        assert_eq!(got[0].body, br#"{"tenant_id":"acme"}"#);
        assert_eq!(got[0].response_status, 201);
    }

    #[test]
    fn redacting_capture_strips_only_the_authorization_header() {
        let inner = MemoryCapture::new();
        let cap = RedactingCapture::new(inner.clone());
        let headers = vec![
            ("Authorization".to_owned(), "Bearer s3cret".to_owned()),
            ("x-tenant".to_owned(), "acme".to_owned()),
        ];
        cap.capture(&record(&headers));
        let got = inner.records();
        assert_eq!(got.len(), 1);
        assert!(
            !got[0]
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("authorization")),
            "credential redacted: {:?}",
            got[0].headers
        );
        // The body is still full fidelity (a replay needs it).
        assert_eq!(got[0].body, br#"{"tenant_id":"acme"}"#);
        assert!(got[0].headers.iter().any(|(k, _)| k == "x-tenant"));
    }
}
