//! Turning a hyper request into an owned [`IngressRequest`] and an
//! [`IngressResponse`] back into a hyper response, the wire (de)serialization,
//! independent of the accept/shutdown loop in [`crate::server`].
//!
//! Admission (per-request `413`, in-flight `429`) is enforced here as the request
//! is parsed, so an over-budget request never reaches the handler.

use std::sync::Arc;

use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use osproxy_spi::{HttpMethod, Protocol};

use crate::admission::{Admission, IngressLimits, Reservation};
use crate::classify::classify;
use crate::handler::IngressHandler;
use crate::request::{
    buffered_response, IngressRequest, IngressResponse, ResponseBody, StreamingResponse,
};

/// Connection-level facts shared by every request on a connection: the verified
/// mTLS client identity (TLS suite/version for the trace's `ingress` span attach
/// here in a later slice).
#[derive(Clone, Debug, Default)]
pub(crate) struct ConnInfo {
    pub(crate) client_cert_subject: Option<String>,
    /// Whether the connection was terminated over TLS. The handler refuses to
    /// mutate a request body over cleartext (NFR-S1).
    pub(crate) secure: bool,
}

/// Parses one request and serves it. A verbatim passthrough is **streamed** to
/// the handler without buffering (ADR-014 stage 2); every other request is
/// buffered (capped, and holding an in-flight reservation across the handler
/// call) and handled normally. Early failures: `405` (method), `429` (in-flight
/// ceiling), `413` (body over the per-request cap).
pub(crate) async fn serve_request<H: IngressHandler>(
    handler: &H,
    req: Request<Incoming>,
    conn_info: &ConnInfo,
    limits: IngressLimits,
    admission: &Arc<Admission>,
) -> Response<ResponseBody> {
    let Some(method) = map_method(req.method()) else {
        return render(IngressResponse::json(405, error_body("method not allowed")));
    };
    let path = req.uri().path().to_owned();
    let query = req.uri().query().map(str::to_owned);
    // The `auto` builder negotiates h1/h2 per connection; record which so the
    // engine traces the true ingress protocol rather than assuming h1.
    let protocol = map_protocol(req.version());
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or("").to_owned()))
        .collect();

    let declared = content_length(&headers);

    let c = classify(method, &path);
    // The head, sans body, built before any body work so the streaming decision
    // (which reads only the head) can avoid buffering entirely.
    let head = IngressRequest {
        method,
        protocol,
        path,
        endpoint: c.endpoint,
        logical_index: c.logical_index,
        doc_id: c.doc_id,
        headers,
        body: Vec::new(),
        query,
        client_cert_subject: conn_info.client_cert_subject.clone(),
        secure: conn_info.secure,
    };

    // Streaming verbatim forward: pipe the downstream body straight upstream with
    // no buffering and no in-flight reservation (it never lands in memory). It is
    // *not* subject to the per-request size cap, the cap bounds buffered memory,
    // and this path buffers nothing, so a passthrough may stream a body of any
    // size with bounded memory (ADR-014 stage 2).
    if handler.forward_plan(&head.path, &head.logical_index) {
        return render_forward(handler.handle_forward(head, req.into_body()).await);
    }

    // The remaining paths are size-capped: the buffered path holds the whole body,
    // and the bulk demux holds one response line per op (bounded by the cap). A
    // declared Content-Length over the cap is too large outright.
    if declared.is_some_and(|n| n > limits.max_body_bytes) {
        return render(IngressResponse::json(
            413,
            error_body("request body too large"),
        ));
    }

    // Stream-demuxed `_bulk`: frame and dispatch the NDJSON op by op without
    // buffering the whole batch (ADR-014 stage 4).
    if handler.wants_bulk_stream(head.endpoint, &head.headers) {
        return render(handler.handle_bulk_stream(head, req.into_body()).await);
    }

    serve_buffered(handler, req.into_body(), head, declared, limits, admission).await
}

/// The buffered path: reserve the (declared, else worst-case) size against the
/// global budget, collect under the cap, then dispatch. A streamed-response
/// `_search` (the query body is small and buffered here; only its response
/// streams) is dispatched via `handle_search_stream`; everything else via the
/// buffered `handle`. The reservation is held until the response is rendered.
async fn serve_buffered<H: IngressHandler>(
    handler: &H,
    body: Incoming,
    mut head: IngressRequest,
    declared: Option<usize>,
    limits: IngressLimits,
    admission: &Arc<Admission>,
) -> Response<ResponseBody> {
    let reserve = declared.unwrap_or(limits.max_body_bytes);
    let Some(_reservation): Option<Reservation> = admission.try_reserve(reserve) else {
        return render(overloaded_response());
    };
    let collected = match Limited::new(body, limits.max_body_bytes).collect().await {
        Ok(collected) => collected.to_bytes().to_vec(),
        Err(_) => {
            return render(IngressResponse::json(
                413,
                error_body("request body too large"),
            ))
        }
    };
    head.body = collected;

    // Streamed-response `_search` (ADR-014, final stage): the upstream hits
    // envelope is piped back through the hit transform without buffering.
    if handler.wants_search_stream(head.endpoint, head.query.as_deref()) {
        return render_forward(handler.handle_search_stream(head).await);
    }

    render(handler.handle(head).await)
}

/// The `Content-Length` header parsed to a byte count, if present and valid.
fn content_length(headers: &[(String, String)]) -> Option<usize> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse().ok())
}

/// The `429` shed response with retry guidance (NFR-R3): the proxy is at its
/// in-flight memory budget; the client should back off and retry.
fn overloaded_response() -> IngressResponse {
    IngressResponse::json(429, error_body("ingress overloaded, retry later"))
        .with_header("retry-after", "1")
}

/// Maps a hyper method to the SPI's method, or `None` if unsupported.
fn map_method(method: &Method) -> Option<HttpMethod> {
    match *method {
        Method::GET => Some(HttpMethod::Get),
        Method::PUT => Some(HttpMethod::Put),
        Method::POST => Some(HttpMethod::Post),
        Method::DELETE => Some(HttpMethod::Delete),
        Method::HEAD => Some(HttpMethod::Head),
        _ => None,
    }
}

/// Maps a hyper HTTP version to the SPI's protocol. HTTP/2 is distinguished; all
/// 1.x (and the unreachable 0.9) collapse to [`Protocol::Http1`]. gRPC is not seen
/// here, it arrives on the dedicated tonic listener, which sets it directly.
fn map_protocol(version: hyper::Version) -> Protocol {
    if version == hyper::Version::HTTP_2 {
        Protocol::Http2
    } else {
        Protocol::Http1
    }
}

/// Renders a buffered [`IngressResponse`] into a hyper response, never panicking.
fn render(out: IngressResponse) -> Response<ResponseBody> {
    let mut builder = Response::builder()
        .status(out.status)
        .header("content-type", "application/json");
    for (name, value) in out.headers {
        builder = builder.header(name, value);
    }
    builder
        .body(buffered_response(out.body))
        .unwrap_or_else(|_| {
            // A well-formed status + static body cannot fail to build; fall back
            // to a minimal 500 rather than unwrapping (NFR-R1).
            Response::new(buffered_response(b"{\"error\":\"internal\"}".to_vec()))
        })
}

/// Renders a [`StreamingResponse`] (a verbatim forward) into a hyper response: the
/// body is the upstream stream, piped straight to the client without buffering.
fn render_forward(out: StreamingResponse) -> Response<ResponseBody> {
    let mut builder = Response::builder()
        .status(out.status)
        .header("content-type", "application/json");
    for (name, value) in out.headers {
        builder = builder.header(name, value);
    }
    builder
        .body(out.body)
        .unwrap_or_else(|_| Response::new(buffered_response(b"{\"error\":\"internal\"}".to_vec())))
}

/// A minimal JSON error body, value-free.
fn error_body(message: &str) -> Vec<u8> {
    format!(r#"{{"error":"{message}"}}"#).into_bytes()
}
