//! The read-only view of an authenticated request handed to the SPI.

use osproxy_core::{EndpointKind, PrincipalId, RequestId};

use crate::principal::Principal;

/// The wire protocol a request arrived on (or is sent upstream on).
///
/// `#[non_exhaustive]` so additional protocols are additive. M1 implements
/// [`Protocol::Http1`] only; HTTP/2 and gRPC arrive in M4 (`docs/11`).
#[non_exhaustive]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Protocol {
    /// HTTP/1.1, cleartext or over TLS.
    Http1,
    /// HTTP/2.
    Http2,
    /// gRPC (over HTTP/2).
    Grpc,
}

/// The HTTP method of a request.
#[non_exhaustive]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HttpMethod {
    /// `GET`.
    Get,
    /// `PUT`.
    Put,
    /// `POST`.
    Post,
    /// `DELETE`.
    Delete,
    /// `HEAD`.
    Head,
}

/// A minimal, borrowed view of request headers.
///
/// Backed by the transport's parsed headers; the SPI may read a header (e.g. to
/// find a partition key) but cannot mutate it here — mutations are expressed as
/// [`crate::HeaderOp`]s in the returned decision.
#[derive(Clone, Copy, Debug)]
pub struct HeaderView<'a> {
    headers: &'a [(String, String)],
}

impl<'a> HeaderView<'a> {
    /// Wraps a parsed header list.
    #[must_use]
    pub fn new(headers: &'a [(String, String)]) -> Self {
        Self { headers }
    }

    /// Returns the first value for `name` (ASCII-case-insensitive), if present.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&'a str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// The read-only view of an authenticated request given to the SPI to decide
/// routing.
///
/// For M1 (single-doc ingest) the body is provided as a borrowed byte slice:
/// one document fits comfortably in memory. Streaming body access for bulk
/// arrives with the demux work in M3 (`docs/04` §3); the field is intentionally
/// accessed only through [`RequestCtx::body`] so that change stays internal.
#[derive(Clone, Copy, Debug)]
pub struct RequestCtx<'a> {
    principal: &'a Principal,
    request_id: &'a RequestId,
    method: HttpMethod,
    endpoint: EndpointKind,
    protocol: Protocol,
    logical_index: &'a str,
    doc_id: Option<&'a str>,
    headers: HeaderView<'a>,
    body: &'a [u8],
    query: Option<&'a str>,
    path: &'a str,
}

impl<'a> RequestCtx<'a> {
    /// Constructs a request context from its already-authenticated parts.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "an authenticated request genuinely has this many independent, \
                  read-only facets; bundling them into sub-structs would only \
                  shuffle the same fields around (docs/08 §3)"
    )]
    pub fn new(
        principal: &'a Principal,
        request_id: &'a RequestId,
        method: HttpMethod,
        endpoint: EndpointKind,
        protocol: Protocol,
        logical_index: &'a str,
        headers: HeaderView<'a>,
        body: &'a [u8],
    ) -> Self {
        Self {
            principal,
            request_id,
            method,
            endpoint,
            protocol,
            logical_index,
            doc_id: None,
            headers,
            body,
            query: None,
            path: "",
        }
    }

    /// Sets the raw request path (e.g. `/_cat/indices`). Builder style. Used by
    /// the admin pass-through, which forwards the path verbatim to the configured
    /// admin cluster; the tenancy-aware paths derive their index/id at classify
    /// time and do not consult it.
    #[must_use]
    pub fn with_path(mut self, path: &'a str) -> Self {
        self.path = path;
        self
    }

    /// Sets the document id from the request path (e.g. `_doc/{id}`), present on
    /// by-id reads/writes. Builder style; `RequestCtx` is `Copy` (`docs/04` §5).
    #[must_use]
    pub fn with_doc_id(mut self, doc_id: Option<&'a str>) -> Self {
        self.doc_id = doc_id;
        self
    }

    /// Sets the raw URL query string (without the `?`). Builder style. Only an
    /// allow-list of cursor params (`scroll`/`keep_alive`) is ever forwarded
    /// upstream — query-affecting params are dropped so the body partition filter
    /// cannot be bypassed (NFR-S4).
    #[must_use]
    pub fn with_query(mut self, query: Option<&'a str>) -> Self {
        self.query = query;
        self
    }

    /// The authenticated caller.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        self.principal
    }

    /// The principal's id (convenience).
    #[must_use]
    pub fn principal_id(&self) -> &PrincipalId {
        self.principal.id()
    }

    /// The request correlation id (telemetry).
    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        self.request_id
    }

    /// The HTTP method.
    #[must_use]
    pub fn method(&self) -> HttpMethod {
        self.method
    }

    /// The endpoint classification.
    #[must_use]
    pub fn endpoint(&self) -> EndpointKind {
        self.endpoint
    }

    /// The ingress protocol.
    #[must_use]
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// The logical index from the request path (pre-rewrite).
    #[must_use]
    pub fn logical_index(&self) -> &str {
        self.logical_index
    }

    /// The client-supplied document id from the path, if the endpoint carries
    /// one (`GetById`/`DeleteById`/by-id ingest). This is the **logical** id;
    /// the tenancy layer maps it to the physical id (`docs/04` §5).
    #[must_use]
    pub fn doc_id(&self) -> Option<&'a str> {
        self.doc_id
    }

    /// The raw URL query string (without the `?`), if any. Consumers must forward
    /// only an allow-list of cursor params (`scroll`/`keep_alive`) upstream.
    #[must_use]
    pub fn query(&self) -> Option<&'a str> {
        self.query
    }

    /// The raw request path, if set (`with_path`). Empty unless the consumer
    /// attached it; the admin pass-through forwards it verbatim upstream.
    #[must_use]
    pub fn path(&self) -> &'a str {
        self.path
    }

    /// The request headers.
    #[must_use]
    pub fn headers(&self) -> HeaderView<'a> {
        self.headers
    }

    /// The raw request body.
    #[must_use]
    pub fn body(&self) -> &'a [u8] {
        self.body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_lookup_is_case_insensitive() {
        let raw = vec![("X-Tenant".to_owned(), "acme".to_owned())];
        let view = HeaderView::new(&raw);
        assert_eq!(view.get("x-tenant"), Some("acme"));
        assert_eq!(view.get("X-TENANT"), Some("acme"));
        assert_eq!(view.get("absent"), None);
    }

    #[test]
    fn ctx_exposes_its_parts() {
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("req-1");
        let raw: Vec<(String, String)> = vec![];
        let ctx = RequestCtx::new(
            &principal,
            &rid,
            HttpMethod::Put,
            EndpointKind::IngestDoc,
            Protocol::Http1,
            "orders",
            HeaderView::new(&raw),
            b"{}",
        );
        assert_eq!(ctx.method(), HttpMethod::Put);
        assert_eq!(ctx.endpoint(), EndpointKind::IngestDoc);
        assert_eq!(ctx.protocol(), Protocol::Http1);
        assert_eq!(ctx.logical_index(), "orders");
        assert_eq!(ctx.principal_id().as_str(), "svc");
        assert_eq!(ctx.request_id().as_str(), "req-1");
        assert_eq!(ctx.body(), b"{}");
        assert_eq!(ctx.doc_id(), None);
    }

    #[test]
    fn doc_id_is_attached_by_builder() {
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("req-1");
        let raw: Vec<(String, String)> = vec![];
        let ctx = RequestCtx::new(
            &principal,
            &rid,
            HttpMethod::Get,
            EndpointKind::GetById,
            Protocol::Http1,
            "orders",
            HeaderView::new(&raw),
            b"",
        )
        .with_doc_id(Some("7"));
        assert_eq!(ctx.doc_id(), Some("7"));
    }
}
