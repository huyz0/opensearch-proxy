//! The seam between the wire and the pipeline.

use std::future::Future;

use hyper::body::Incoming;
use osproxy_core::EndpointKind;

use crate::request::{IngressRequest, IngressResponse, StreamingResponse};

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
    /// request is buffered and handled by [`handle`](Self::handle)). Verbatim
    /// passthrough forwards every method, so the decision is path/index-only.
    fn forward_plan(&self, _path: &str, _logical_index: &str) -> bool {
        false
    }

    /// Handles a streamed verbatim forward: `body` is the downstream request body
    /// piped straight to the upstream, and the returned [`StreamingResponse`]'s
    /// body is the upstream response piped straight back, neither buffered.
    /// Called only when [`forward_plan`](Self::forward_plan) returned `true`; `req`
    /// carries the parsed head (its `body` field is empty, the body is the `body`
    /// argument). The default returns `500`, so a handler that opts in via
    /// `forward_plan` must implement it.
    fn handle_forward(
        &self,
        _req: IngressRequest,
        _body: Incoming,
    ) -> impl Future<Output = StreamingResponse> + Send {
        async {
            StreamingResponse::buffered(500, br#"{"error":"forward_not_implemented"}"#.to_vec())
        }
    }

    /// Whether this `_search` should have its **response streamed** back through
    /// the hit transform (ADR-014, final stage) rather than buffered: decided from
    /// the endpoint + query (e.g. a scroll-opening search keeps the buffered path).
    /// The request body is still buffered first (it is small); only the response
    /// streams. `false` by default.
    fn wants_search_stream(&self, _endpoint: EndpointKind, _query: Option<&str>) -> bool {
        false
    }

    /// Handles a streamed-response `_search`: `req` carries the (buffered) query
    /// body; the returned [`StreamingResponse`]'s body is the upstream hits
    /// envelope piped back through the hit transform without buffering. Called only
    /// when [`wants_search_stream`](Self::wants_search_stream) returned `true`.
    /// Default `500`.
    fn handle_search_stream(
        &self,
        _req: IngressRequest,
    ) -> impl Future<Output = StreamingResponse> + Send {
        async {
            StreamingResponse::buffered(
                500,
                br#"{"error":"search_stream_not_implemented"}"#.to_vec(),
            )
        }
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
