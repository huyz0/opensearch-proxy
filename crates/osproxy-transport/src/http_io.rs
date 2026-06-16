//! Turning a hyper request into an owned [`IngressRequest`] and an
//! [`IngressResponse`] back into a hyper response — the wire (de)serialization,
//! independent of the accept/shutdown loop in [`crate::server`].
//!
//! Admission (per-request `413`, in-flight `429`) is enforced here as the request
//! is parsed, so an over-budget request never reaches the handler.

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use osproxy_spi::HttpMethod;

use crate::admission::{Admission, IngressLimits, Reservation};
use crate::classify::classify;
use crate::handler::IngressHandler;
use crate::request::{IngressRequest, IngressResponse};

/// Connection-level facts shared by every request on a connection: the verified
/// mTLS client identity (TLS suite/version for the trace's `ingress` span attach
/// here in a later slice).
#[derive(Clone, Debug, Default)]
pub(crate) struct ConnInfo {
    pub(crate) client_cert_subject: Option<String>,
}

/// Parses one request, runs the handler, and renders the response. The body's
/// in-flight reservation is held across the handler call and released when the
/// response is rendered.
pub(crate) async fn serve_request<H: IngressHandler>(
    handler: &H,
    req: Request<Incoming>,
    conn_info: &ConnInfo,
    limits: IngressLimits,
    admission: &Arc<Admission>,
) -> Response<Full<Bytes>> {
    match parse(req, conn_info, limits, admission).await {
        Ok((ingress, _reservation)) => render(handler.handle(ingress).await),
        Err(early) => render(early),
    }
}

/// Parses a hyper request into an owned [`IngressRequest`] plus the in-flight
/// [`Reservation`] covering its body, or an early [`IngressResponse`]: `405` for
/// an unsupported method, `429` when the in-flight ceiling is reached, or `413`
/// for a body over the per-request cap.
async fn parse(
    req: Request<Incoming>,
    conn_info: &ConnInfo,
    limits: IngressLimits,
    admission: &Arc<Admission>,
) -> Result<(IngressRequest, Reservation), IngressResponse> {
    let Some(method) = map_method(req.method()) else {
        return Err(IngressResponse::json(405, error_body("method not allowed")));
    };
    let path = req.uri().path().to_owned();
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or("").to_owned()))
        .collect();

    // A declared Content-Length over the per-request cap is too large outright.
    let declared = content_length(&headers);
    if declared.is_some_and(|n| n > limits.max_body_bytes) {
        return Err(IngressResponse::json(
            413,
            error_body("request body too large"),
        ));
    }
    // Reserve the (declared, else worst-case) size against the global budget
    // before buffering; shed with 429 + retry guidance if the ceiling is reached.
    let reserve = declared.unwrap_or(limits.max_body_bytes);
    let reservation = admission
        .try_reserve(reserve)
        .ok_or_else(overloaded_response)?;

    let body = Limited::new(req.into_body(), limits.max_body_bytes)
        .collect()
        .await
        .map(|c| c.to_bytes().to_vec())
        .map_err(|_| IngressResponse::json(413, error_body("request body too large")))?;

    let c = classify(method, &path);
    Ok((
        IngressRequest {
            method,
            path,
            endpoint: c.endpoint,
            logical_index: c.logical_index,
            doc_id: c.doc_id,
            headers,
            body,
            client_cert_subject: conn_info.client_cert_subject.clone(),
        },
        reservation,
    ))
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

/// Renders an [`IngressResponse`] into a hyper response, never panicking.
fn render(out: IngressResponse) -> Response<Full<Bytes>> {
    let mut builder = Response::builder()
        .status(out.status)
        .header("content-type", "application/json");
    for (name, value) in out.headers {
        builder = builder.header(name, value);
    }
    builder
        .body(Full::new(Bytes::from(out.body)))
        .unwrap_or_else(|_| {
            // A well-formed status + static body cannot fail to build; fall back
            // to a minimal 500 rather than unwrapping (NFR-R1).
            Response::new(Full::new(Bytes::from_static(b"{\"error\":\"internal\"}")))
        })
}

/// A minimal JSON error body, value-free.
fn error_body(message: &str) -> Vec<u8> {
    format!(r#"{{"error":"{message}"}}"#).into_bytes()
}
