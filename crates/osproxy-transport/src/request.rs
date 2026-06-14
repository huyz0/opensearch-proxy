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
}

/// The response a handler returns for the transport to write back.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IngressResponse {
    /// The HTTP status code.
    pub status: u16,
    /// The response body (JSON).
    pub body: Vec<u8>,
}

impl IngressResponse {
    /// A JSON response with the given status and body.
    #[must_use]
    pub fn json(status: u16, body: Vec<u8>) -> Self {
        Self { status, body }
    }
}
