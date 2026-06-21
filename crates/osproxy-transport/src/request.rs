//! The owned request/response shapes the ingress hands to a handler.
//!
//! The transport parses bytes off the wire into an [`IngressRequest`] (owned, so
//! it outlives the borrowed hyper request) and writes an [`IngressResponse`]
//! back. It carries no routing or tenancy meaning — just the parsed HTTP facts
//! plus the endpoint classification.

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use osproxy_core::EndpointKind;
use osproxy_spi::{HttpMethod, Protocol};

/// The transport's HTTP response body: boxed so a response may be buffered bytes
/// or a **live stream** piped from the upstream without buffering (ADR-014).
/// Unsync — the server only needs `Send`. Structurally identical to
/// `osproxy-sink`'s `ByteBody`, so a streamed upstream response flows through
/// as-is, no copy.
pub type ResponseBody = UnsyncBoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

/// Wraps fully-buffered bytes as a [`ResponseBody`] (the buffered response path).
#[must_use]
pub fn buffered_response(body: Vec<u8>) -> ResponseBody {
    Full::new(Bytes::from(body))
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// A streaming response a handler returns for a verbatim forward (ADR-014): a
/// status, extra headers, and a body piped to the client without buffering.
pub struct StreamingResponse {
    /// The HTTP status code.
    pub status: u16,
    /// Extra response headers (beyond the content type the transport sets).
    pub headers: Vec<(String, String)>,
    /// The response body — a live stream, or buffered bytes for an error.
    pub body: ResponseBody,
}

impl std::fmt::Debug for StreamingResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The body is a stream, not `Debug`; show the rest of the shape.
        f.debug_struct("StreamingResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

impl StreamingResponse {
    /// A response whose body is a live stream.
    #[must_use]
    pub fn stream(status: u16, body: ResponseBody) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body,
        }
    }

    /// A response with a buffered body (e.g. an error), boxed into the streaming
    /// body type so both kinds share one response type.
    #[must_use]
    pub fn buffered(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: buffered_response(body),
        }
    }

    /// Adds a response header (builder style).
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// A parsed, owned client request ready for the pipeline.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IngressRequest {
    /// The HTTP method.
    pub method: HttpMethod,
    /// The wire protocol the request arrived on (HTTP/1.1, HTTP/2, or gRPC). The
    /// `auto` ingress builder negotiates h1/h2 per connection; the engine records
    /// it for tracing and may select the upstream protocol from it (`docs/04` §7).
    pub protocol: Protocol,
    /// The raw request path (used to route proxy admin endpoints such as
    /// `/debug/explain/{id}` that are not OpenSearch paths).
    pub path: String,
    /// The endpoint classification derived from method + path.
    pub endpoint: EndpointKind,
    /// The logical index from the path (pre-rewrite), empty if the path has none.
    pub logical_index: String,
    /// The document id from the path, if the endpoint carries one (`_doc/{id}`).
    pub doc_id: Option<String>,
    /// The request headers, in arrival order.
    pub headers: Vec<(String, String)>,
    /// The request body.
    pub body: Vec<u8>,
    /// The raw URL query string (without the `?`), if any. The engine forwards
    /// only an allow-list of cursor params (`scroll`/`keep_alive`) upstream —
    /// query-affecting params are never forwarded, so the body partition filter
    /// cannot be bypassed (NFR-S4).
    pub query: Option<String>,
    /// The verified client-certificate identity, if the connection was mutually
    /// authenticated (mTLS). A stable id derived from the cert, never the raw
    /// certificate material.
    pub client_cert_subject: Option<String>,
    /// Whether the request arrived over a TLS-terminated connection. The handler
    /// refuses to mutate a request body over cleartext, since the proxy must
    /// terminate TLS to rewrite the stream (NFR-S1).
    pub secure: bool,
}

/// The response a handler returns for the transport to write back.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IngressResponse {
    /// The HTTP status code.
    pub status: u16,
    /// Extra response headers (beyond the JSON content type the transport sets).
    pub headers: Vec<(String, String)>,
    /// The response body (JSON).
    pub body: Vec<u8>,
}

impl IngressResponse {
    /// A JSON response with the given status and body.
    #[must_use]
    pub fn json(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body,
        }
    }

    /// Adds a response header (builder style).
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}
