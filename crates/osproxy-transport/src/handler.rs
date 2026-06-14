//! The seam between the wire and the pipeline.

use std::future::Future;

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
}
