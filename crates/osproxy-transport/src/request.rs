//! The owned request/response shapes the ingress hands to a handler.
//!
//! The transport parses bytes off the wire into an [`IngressRequest`] (owned, so
//! it outlives the borrowed hyper request) and writes an [`IngressResponse`]
//! back. It carries no routing or tenancy meaning — just the parsed HTTP facts
//! plus the endpoint classification.

use osproxy_core::EndpointKind;
use osproxy_spi::HttpMethod;

/// A parsed, owned client request ready for the pipeline.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IngressRequest {
    /// The HTTP method.
    pub method: HttpMethod,
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
