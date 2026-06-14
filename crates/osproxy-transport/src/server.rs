//! The HTTP/1.1 cleartext ingress loop.
//!
//! Accepts connections, parses each request into an [`IngressRequest`], invokes
//! the [`IngressHandler`], and writes the response. TLS termination behind the
//! `CryptoProvider` seam (`docs/07`) and HTTP/2 attach here in a later slice
//! without changing the handler contract.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use osproxy_spi::HttpMethod;
use tokio::net::TcpListener;

use crate::classify::classify;
use crate::handler::IngressHandler;
use crate::request::{IngressRequest, IngressResponse};

/// The largest request body the ingress will buffer. Bounds memory per request
/// (NFR-P3); single-doc ingest is far smaller. Streaming/bulk relaxes this in
/// M3 with backpressure instead of a hard cap.
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Serves HTTP/1.1 requests on `listener`, dispatching each to `handler`, until
/// the listener errors.
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
    loop {
        let (stream, _peer) = listener.accept().await?;
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            serve_connection(TokioIo::new(stream), handler).await;
        });
    }
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
    let acceptor = tokio_rustls::TlsAcceptor::from(provider.server_config());
    loop {
        let (stream, _peer) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            // Drop the connection on handshake failure; logged via observability
            // in a later slice.
            if let Ok(tls) = acceptor.accept(stream).await {
                serve_connection(TokioIo::new(tls), handler).await;
            }
        });
    }
}

/// Serves HTTP/1.1 over one already-accepted byte stream (cleartext or TLS).
async fn serve_connection<H, IO>(io: IO, handler: Arc<H>)
where
    H: IngressHandler,
    IO: hyper::rt::Read + hyper::rt::Write + Unpin + 'static,
{
    let service = service_fn(move |req: Request<Incoming>| {
        let handler = Arc::clone(&handler);
        async move { Ok::<_, Infallible>(serve_request(&*handler, req).await) }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(io, service)
        .await;
}

/// Parses one request, runs the handler, and renders the response.
async fn serve_request<H: IngressHandler>(
    handler: &H,
    req: Request<Incoming>,
) -> Response<Full<Bytes>> {
    match parse(req).await {
        Ok(ingress) => render(handler.handle(ingress).await),
        Err(early) => render(early),
    }
}

/// Parses a hyper request into an owned [`IngressRequest`], or an early
/// [`IngressResponse`] for an unsupported method or oversized body.
async fn parse(req: Request<Incoming>) -> Result<IngressRequest, IngressResponse> {
    let Some(method) = map_method(req.method()) else {
        return Err(IngressResponse::json(405, error_body("method not allowed")));
    };
    let path = req.uri().path().to_owned();
    let headers = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or("").to_owned()))
        .collect();

    let body = Limited::new(req.into_body(), MAX_BODY_BYTES)
        .collect()
        .await
        .map(|c| c.to_bytes().to_vec())
        .map_err(|_| IngressResponse::json(413, error_body("request body too large")))?;

    let c = classify(method, &path);
    Ok(IngressRequest {
        method,
        path,
        endpoint: c.endpoint,
        logical_index: c.logical_index,
        doc_id: c.doc_id,
        headers,
        body,
    })
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
