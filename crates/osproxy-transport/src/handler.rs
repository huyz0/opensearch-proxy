//! The seam between the wire and the pipeline.

use std::future::Future;

use hyper::body::Incoming;
use osproxy_core::EndpointKind;
use osproxy_spi::HttpMethod;

use crate::request::{IngressRequest, IngressResponse};

/// Handles a parsed ingress request, producing a response.
///
/// The binary implements this by authenticating the request, building a
/// [`RequestCtx`](osproxy_spi::RequestCtx) from the parsed parts, and driving
/// the engine pipeline. The transport stays free of routing/tenancy knowledge.
///
/// The handler is infallible by contract: it must map every failure to an
/// [`IngressResponse`] (an error status + body), so the transport never has to
/// decide how to render a pipeline error.
pub trait IngressHandler: Send + Sync + 'static {
    /// Handles one request. The returned future must be `Send` so connections
    /// can be served on the multi-threaded runtime.
    fn handle(&self, req: IngressRequest) -> impl Future<Output = IngressResponse> + Send;

    /// Whether this request is a verbatim passthrough that should be forwarded
    /// with a **streamed** body (ADR-014 stage 2), decided from the head alone so
    /// the transport can avoid buffering. Returns `false` by default (every
    /// request is buffered and handled by [`handle`](Self::handle)).
    fn forward_plan(&self, _method: HttpMethod, _path: &str, _logical_index: &str) -> bool {
        false
    }

    /// Handles a streamed verbatim forward: `body` is the downstream request body
    /// piped straight to the upstream without buffering. Called only when
    /// [`forward_plan`](Self::forward_plan) returned `true`; `req` carries the
    /// parsed head (its `body` field is empty — the body is the `body` argument).
    /// The default returns `500`, so a handler that opts in via `forward_plan`
    /// must implement it.
    fn handle_forward(
        &self,
        _req: IngressRequest,
        _body: Incoming,
    ) -> impl Future<Output = IngressResponse> + Send {
        async { IngressResponse::json(500, br#"{"error":"forward_not_implemented"}"#.to_vec()) }
    }

    /// Whether this `_bulk` request should be **stream-demuxed** (ADR-014 stage 4)
    /// rather than buffered: decided from the endpoint + headers (e.g. the write
    /// mode) alone, so the transport can avoid buffering the whole batch. `false`
    /// by default.
    fn wants_bulk_stream(&self, _endpoint: EndpointKind, _headers: &[(String, String)]) -> bool {
        false
    }

    /// Handles a stream-demuxed `_bulk`: `body` is the NDJSON batch, framed and
    /// dispatched op by op without buffering the whole thing. Called only when
    /// [`wants_bulk_stream`](Self::wants_bulk_stream) returned `true`. Default `500`.
    fn handle_bulk_stream(
        &self,
        _req: IngressRequest,
        _body: Incoming,
    ) -> impl Future<Output = IngressResponse> + Send {
        async { IngressResponse::json(500, br#"{"error":"bulk_stream_not_implemented"}"#.to_vec()) }
    }
}
