//! The HTTP ingress loop (HTTP/1.1 and HTTP/2).
//!
//! Accepts connections, parses each request into an [`IngressRequest`], invokes
//! the [`IngressHandler`], and writes the response. Each connection is served by
//! hyper-util's protocol-auto builder, which negotiates HTTP/1.1 or HTTP/2 per
//! connection — h2c by the HTTP/2 preface on cleartext, h2 by ALPN on TLS
//! (`docs/07`). The handler contract is identical across protocols.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use osproxy_spi::HttpMethod;
use tokio::net::TcpListener;

use crate::admission::{Admission, IngressLimits, Reservation};
use crate::classify::classify;
use crate::handler::IngressHandler;
use crate::request::{IngressRequest, IngressResponse};

/// Serves HTTP/1.1 requests on `listener` with the default [`IngressLimits`].
///
/// # Errors
///
/// Returns the I/O error if accepting a connection fails. Per-connection
/// protocol errors (client disconnects, malformed framing) are isolated to that
/// connection and do not stop the loop.
pub async fn serve<H: IngressHandler>(
    listener: TcpListener,
    handler: Arc<H>,
) -> std::io::Result<()> {
    serve_with_limits(listener, handler, IngressLimits::default()).await
}

/// Serves HTTP/1.1 requests on `listener`, dispatching each to `handler` under
/// the given memory `limits` (per-request `413`, in-flight `429`), until the
/// listener errors.
///
/// # Errors
///
/// Returns the I/O error if accepting a connection fails.
pub async fn serve_with_limits<H: IngressHandler>(
    listener: TcpListener,
    handler: Arc<H>,
    limits: IngressLimits,
) -> std::io::Result<()> {
    let admission = Arc::new(Admission::new(limits.inflight_ceiling));
    loop {
        let (stream, _peer) = listener.accept().await?;
        let handler = Arc::clone(&handler);
        let admission = Arc::clone(&admission);
        tokio::spawn(async move {
            serve_connection(
                TokioIo::new(stream),
                handler,
                ConnInfo::default(),
                limits,
                admission,
            )
            .await;
        });
    }
}

/// Connection-level facts shared by every request on a connection. M1 carries
/// the verified mTLS client identity; TLS suite/version for the trace's
/// `ingress` span attach here in a later slice.
#[derive(Clone, Debug, Default)]
struct ConnInfo {
    client_cert_subject: Option<String>,
}

/// Serves HTTPS requests on `listener`, terminating TLS with `provider`'s
/// configuration, until the listener errors.
///
/// A TLS handshake failure is isolated to its connection (the connection is
/// dropped); the accept loop keeps serving. The handler contract is identical to
/// [`serve`] — TLS is transparent to it.
///
/// # Errors
///
/// Returns the I/O error if accepting a connection fails.
pub async fn serve_tls<H, P>(
    listener: TcpListener,
    provider: Arc<P>,
    handler: Arc<H>,
) -> std::io::Result<()>
where
    H: IngressHandler,
    P: crate::tls::CryptoProvider,
{
    serve_tls_with_limits(listener, provider, handler, IngressLimits::default()).await
}

/// Serves HTTPS requests on `listener` under the given memory `limits`,
/// terminating TLS with `provider`'s configuration, until the listener errors.
///
/// # Errors
///
/// Returns the I/O error if accepting a connection fails.
pub async fn serve_tls_with_limits<H, P>(
    listener: TcpListener,
    provider: Arc<P>,
    handler: Arc<H>,
    limits: IngressLimits,
) -> std::io::Result<()>
where
    H: IngressHandler,
    P: crate::tls::CryptoProvider,
{
    let acceptor = tokio_rustls::TlsAcceptor::from(provider.server_config());
    let admission = Arc::new(Admission::new(limits.inflight_ceiling));
    loop {
        let (stream, _peer) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let handler = Arc::clone(&handler);
        let admission = Arc::clone(&admission);
        tokio::spawn(async move {
            // Drop the connection on handshake failure; logged via observability
            // in a later slice.
            if let Ok(tls) = acceptor.accept(stream).await {
                let conn_info = conn_info_from_tls(&tls);
                serve_connection(TokioIo::new(tls), handler, conn_info, limits, admission).await;
            }
        });
    }
}

/// Extracts connection-level facts (the verified mTLS client identity) from a
/// completed TLS handshake.
fn conn_info_from_tls(tls: &tokio_rustls::server::TlsStream<tokio::net::TcpStream>) -> ConnInfo {
    let (_, conn) = tls.get_ref();
    let client_cert_subject = conn
        .peer_certificates()
        .and_then(<[_]>::first)
        .map(|cert| format!("cert:{}", crate::tls::cert_fingerprint(cert.as_ref())));
    ConnInfo {
        client_cert_subject,
    }
}

/// Serves HTTP/1.1 over one already-accepted byte stream (cleartext or TLS).
async fn serve_connection<H, IO>(
    io: IO,
    handler: Arc<H>,
    conn_info: ConnInfo,
    limits: IngressLimits,
    admission: Arc<Admission>,
) where
    H: IngressHandler,
    IO: hyper::rt::Read + hyper::rt::Write + Unpin + 'static,
{
    let service = service_fn(move |req: Request<Incoming>| {
        let handler = Arc::clone(&handler);
        let conn_info = conn_info.clone();
        let admission = Arc::clone(&admission);
        async move {
            Ok::<_, Infallible>(serve_request(&*handler, req, &conn_info, limits, &admission).await)
        }
    });
    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .serve_connection(io, service)
        .await;
}

/// Parses one request, runs the handler, and renders the response. The body's
/// in-flight reservation is held across the handler call and released when the
/// response is rendered.
async fn serve_request<H: IngressHandler>(
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
