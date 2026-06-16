//! The HTTP ingress loop (HTTP/1.1 and HTTP/2).
//!
//! Accepts connections, parses each request into an
//! [`IngressRequest`](crate::IngressRequest), invokes the [`IngressHandler`], and
//! writes the response. Each connection is served by
//! hyper-util's protocol-auto builder, which negotiates HTTP/1.1 or HTTP/2 per
//! connection — h2c by the HTTP/2 preface on cleartext, h2 by ALPN on TLS
//! (`docs/07`). The handler contract is identical across protocols.
//!
//! **Graceful shutdown (NFR-R5).** The `*_with_shutdown` variants take a future;
//! when it resolves the accept loop stops (new connections are no longer
//! accepted) and every in-flight connection is told to finish its current
//! request and close, bounded by [`DRAIN_DEADLINE`]. The plain `serve*` variants
//! delegate with a never-resolving signal, so they run until the listener errors
//! exactly as before.

use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::admission::{Admission, IngressLimits};
use crate::handler::IngressHandler;
use crate::http_io::{serve_request, ConnInfo};

/// How long graceful shutdown waits for in-flight requests to drain before
/// giving up and dropping the remainder (NFR-R5).
pub const DRAIN_DEADLINE: Duration = Duration::from_secs(30);

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
    run(listener, handler, limits, Mode::Plain, never()).await
}

/// Like [`serve`], but stops accepting and **drains in-flight requests** when
/// `shutdown` resolves (NFR-R5). In-flight connections finish their current
/// request and close; the drain is bounded by [`DRAIN_DEADLINE`].
///
/// # Errors
///
/// Returns the I/O error if accepting a connection fails before shutdown.
pub async fn serve_with_shutdown<H: IngressHandler>(
    listener: TcpListener,
    handler: Arc<H>,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    run(
        listener,
        handler,
        IngressLimits::default(),
        Mode::Plain,
        shutdown,
    )
    .await
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
    run(listener, handler, limits, Mode::Tls(acceptor), never()).await
}

/// Like [`serve_tls`], but drains in-flight requests when `shutdown` resolves
/// (NFR-R5), bounded by [`DRAIN_DEADLINE`].
///
/// # Errors
///
/// Returns the I/O error if accepting a connection fails before shutdown.
pub async fn serve_tls_with_shutdown<H, P>(
    listener: TcpListener,
    provider: Arc<P>,
    handler: Arc<H>,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()>
where
    H: IngressHandler,
    P: crate::tls::CryptoProvider,
{
    let acceptor = tokio_rustls::TlsAcceptor::from(provider.server_config());
    run(
        listener,
        handler,
        IngressLimits::default(),
        Mode::Tls(acceptor),
        shutdown,
    )
    .await
}

/// How a freshly accepted TCP stream becomes a served connection: directly, or
/// through a TLS handshake first.
enum Mode {
    Plain,
    Tls(tokio_rustls::TlsAcceptor),
}

/// A never-resolving shutdown signal — the plain `serve*` paths run until the
/// listener errors, exactly as before graceful shutdown existed.
fn never() -> impl Future<Output = ()> {
    std::future::pending()
}

/// The shared accept loop. Spawns each accepted connection, tracking the live
/// count so shutdown can wait for it to reach zero. When `shutdown` resolves it
/// breaks out of accepting and drains (NFR-R5).
async fn run<H: IngressHandler>(
    listener: TcpListener,
    handler: Arc<H>,
    limits: IngressLimits,
    mode: Mode,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    let admission = Arc::new(Admission::new(limits.inflight_ceiling));
    // `false` until shutdown begins; flipped to `true` to tell every live
    // connection to finish its current request and close.
    let (drain_tx, drain_rx) = watch::channel(false);
    let active = Arc::new(AtomicUsize::new(0));
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _peer) = accepted?;
                spawn_conn(stream, &mode, &handler, &admission, limits, &active, &drain_rx);
            }
            () = &mut shutdown => break,
        }
    }
    // Stop accepting (the loop has exited), signal in-flight connections to wind
    // down, and wait for them to drain within the deadline.
    let _ = drain_tx.send(true);
    await_drain(&active, DRAIN_DEADLINE).await;
    Ok(())
}

/// Spawns a task serving one accepted connection, bumping the live-connection
/// count for the duration so shutdown can wait it out. TLS connections handshake
/// inside the task so a slow handshake never stalls the accept loop.
fn spawn_conn<H: IngressHandler>(
    stream: TcpStream,
    mode: &Mode,
    handler: &Arc<H>,
    admission: &Arc<Admission>,
    limits: IngressLimits,
    active: &Arc<AtomicUsize>,
    drain_rx: &watch::Receiver<bool>,
) {
    // Relaxed is sufficient for the increment: it is published to the drain by
    // control-flow happens-before (this runs on the accept loop, strictly before
    // the loop can break and reach `await_drain`), not by the atomic itself. The
    // load-bearing edge is the guard's Release `fetch_sub` paired with the
    // Acquire load in `await_drain`.
    active.fetch_add(1, Ordering::Relaxed);
    let guard = ActiveGuard(Arc::clone(active));
    let handler = Arc::clone(handler);
    let admission = Arc::clone(admission);
    let drain_rx = drain_rx.clone();
    match mode {
        Mode::Plain => {
            tokio::spawn(async move {
                let _guard = guard;
                serve_connection(
                    TokioIo::new(stream),
                    handler,
                    ConnInfo::default(),
                    limits,
                    admission,
                    drain_rx,
                )
                .await;
            });
        }
        Mode::Tls(acceptor) => {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let _guard = guard;
                // Drop the connection on handshake failure (isolated to it).
                if let Ok(tls) = acceptor.accept(stream).await {
                    let conn_info = conn_info_from_tls(&tls);
                    serve_connection(
                        TokioIo::new(tls),
                        handler,
                        conn_info,
                        limits,
                        admission,
                        drain_rx,
                    )
                    .await;
                }
            });
        }
    }
}

/// Decrements the live-connection count when a connection task ends (including on
/// panic), so the shutdown drain can observe completion.
struct ActiveGuard(Arc<AtomicUsize>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// Waits until no connections remain, or `deadline` elapses (then the remaining
/// connections are dropped). Polls rather than condvars — it runs once, at
/// shutdown, off the request path.
async fn await_drain(active: &AtomicUsize, deadline: Duration) {
    let drained = async {
        while active.load(Ordering::Acquire) > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let _ = tokio::time::timeout(deadline, drained).await;
}

/// Extracts connection-level facts (the verified mTLS client identity) from a
/// completed TLS handshake.
fn conn_info_from_tls(tls: &tokio_rustls::server::TlsStream<tokio::net::TcpStream>) -> ConnInfo {
    ConnInfo {
        client_cert_subject: crate::tls::client_subject_from_tls(tls),
    }
}

/// Serves HTTP/1.1 or HTTP/2 over one already-accepted byte stream (cleartext or
/// TLS). Races the connection against the `drain` signal: when it flips, the
/// connection is told to finish its current request and close (no new requests),
/// then awaited to completion — the per-connection half of graceful shutdown.
async fn serve_connection<H, IO>(
    io: IO,
    handler: Arc<H>,
    conn_info: ConnInfo,
    limits: IngressLimits,
    admission: Arc<Admission>,
    mut drain: watch::Receiver<bool>,
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
    let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection(io, service);
    tokio::pin!(conn);
    tokio::select! {
        _ = conn.as_mut() => {}
        // `changed()` resolves when shutdown flips the flag (it starts `false`).
        _ = drain.changed() => {
            conn.as_mut().graceful_shutdown();
            let _ = conn.await;
        }
    }
}
