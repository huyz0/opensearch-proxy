//! The upstream TCP/TLS connector.
//!
//! `hyper-util`'s [`HttpConnector`] is cleartext-only, so an `https://` cluster
//! endpoint needs a TLS handshake layered on top. [`MaybeTlsConnector`] does
//! exactly that: `http://` targets pass straight through to the inner
//! [`HttpConnector`]; `https://` targets add a TLS handshake using an
//! already-built [`ClientConfig`] the caller supplies (`osproxy-transport`'s
//! `CryptoProvider::client_config`, so the same crypto module backs both
//! ingress and egress TLS — this crate never selects `ring`/`aws-lc-rs`
//! itself, `docs/07`'s FIPS boundary keeps aws-lc-rs a single-consumer dep).
//!
//! Wraps the same [`HttpConnector`] instance [`OpenSearchSink`](crate::opensearch::OpenSearchSink)
//! already builds per cluster, so `https://` gets the identical `nodelay`/pooling
//! behavior as `http://`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper::rt::{Read, ReadBufCursor, Write};
use hyper::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::{client::TlsStream, TlsConnector};
use tower_service::Service;

use crate::error::SinkError;

/// A TCP connection, optionally upgraded to TLS, wrapped for hyper's I/O
/// traits (via [`TokioIo`], hyper's tokio bridge — `HttpConnector` hands back
/// [`TokioIo<TcpStream>`] already wrapped, and the TLS stream is wrapped the
/// same way after the handshake completes on the raw, unwrapped socket). The
/// [`Connect`](hyper_util::client::legacy::connect::Connect) bound
/// `hyper-util`'s pooled `Client` needs, over either transport.
#[derive(Debug)]
pub(crate) enum MaybeTlsStream {
    Plain(TokioIo<TcpStream>),
    Tls(Box<TokioIo<TlsStream<TcpStream>>>),
}

impl Read for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl Write for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

impl Connection for MaybeTlsStream {
    fn connected(&self) -> Connected {
        match self {
            MaybeTlsStream::Plain(s) => s.connected(),
            // `TlsStream<TcpStream>` itself has no `Connection` impl (only
            // `TcpStream` and `TokioIo<T>` do, and the orphan rule blocks
            // adding one here), so report on the underlying TCP socket
            // instead — the same connection facts either way.
            MaybeTlsStream::Tls(s) => s.inner().get_ref().0.connected(),
        }
    }
}

/// Connects over TCP, adding a TLS handshake for an `https://` target.
#[derive(Clone)]
pub(crate) struct MaybeTlsConnector {
    http: HttpConnector,
    /// The upstream TLS config, when at least one cluster might need it.
    /// `None` means every `https://` dispatch fails closed rather than
    /// silently connecting in cleartext or trusting nothing in particular.
    tls: Option<Arc<ClientConfig>>,
}

impl MaybeTlsConnector {
    pub(crate) fn new(mut http: HttpConnector, tls: Option<Arc<ClientConfig>>) -> Self {
        // HttpConnector refuses a non-http:// URI outright by default; scheme
        // handling is this connector's own job (dispatch to TLS or not).
        http.enforce_http(false);
        Self { http, tls }
    }
}

impl Service<Uri> for MaybeTlsConnector {
    type Response = MaybeTlsStream;
    type Error = SinkError;
    type Future = Pin<Box<dyn Future<Output = Result<MaybeTlsStream, SinkError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(|_| SinkError::Transport {
            kind: "upstream connector not ready",
        })
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let is_https = uri.scheme_str() == Some("https");
        let host = uri.host().unwrap_or_default().to_owned();
        let mut http = self.http.clone();
        let tls = self.tls.clone();
        Box::pin(async move {
            let tcp = http.call(uri).await.map_err(|_| SinkError::Transport {
                kind: "upstream TCP connect failed",
            })?;
            if !is_https {
                return Ok(MaybeTlsStream::Plain(tcp));
            }
            let Some(config) = tls else {
                return Err(SinkError::Transport {
                    kind: "https upstream endpoint but no upstream TLS configured",
                });
            };
            let server_name = ServerName::try_from(host).map_err(|_| SinkError::Transport {
                kind: "invalid upstream TLS server name",
            })?;
            // The TLS handshake runs on the raw tokio socket (TlsConnector
            // needs tokio::io::AsyncRead/Write, not hyper's Read/Write that
            // TokioIo adapts it to); the result is rewrapped in TokioIo so
            // hyper's pooled client can drive it like any other connection.
            let stream = TlsConnector::from(config)
                .connect(server_name, tcp.into_inner())
                .await
                .map_err(|_| SinkError::Transport {
                    kind: "upstream TLS handshake failed",
                })?;
            Ok(MaybeTlsStream::Tls(Box::new(TokioIo::new(stream))))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
    use tokio_rustls::TlsAcceptor;

    use super::*;

    fn self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let key = PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
        (cert.cert.der().clone(), key)
    }

    /// Spawns a real TLS echo server on a random port, returning it alongside
    /// the leaf cert (so a caller can build a trusting `ClientConfig`).
    async fn tls_echo_server() -> (SocketAddr, CertificateDer<'static>) {
        let (cert, key) = self_signed();
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let mut buf = [0u8; 5];
            tokio::io::AsyncReadExt::read_exact(&mut tls, &mut buf)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut tls, b"world")
                .await
                .unwrap();
        };
        tokio::spawn(task); // JUSTIFY(spawn): test-only mock server, always inside a running #[tokio::test] runtime
        (addr, cert)
    }

    fn client_config_trusting(cert: &CertificateDer<'static>) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(cert.clone()).unwrap();
        Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    }

    #[tokio::test]
    async fn https_dispatch_completes_a_real_tls_handshake() {
        let (addr, cert) = tls_echo_server().await;
        let mut connector =
            MaybeTlsConnector::new(HttpConnector::new(), Some(client_config_trusting(&cert)));
        // "localhost" (not the bound IP) so rustls's ServerName check matches
        // the leaf cert's SAN, exercised the same way a real cluster endpoint
        // URL would name it.
        let uri: Uri = format!("https://localhost:{}/", addr.port())
            .parse()
            .unwrap();
        let stream = connector.call(uri).await.expect("handshake succeeds");
        let mut stream = TokioIo::new(stream);
        tokio::io::AsyncWriteExt::write_all(&mut stream, b"hello")
            .await
            .unwrap();
        let mut buf = [0u8; 5];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf, b"world");
    }

    #[tokio::test]
    async fn https_without_a_configured_tls_config_fails_closed() {
        let (addr, _cert) = tls_echo_server().await;
        let mut connector = MaybeTlsConnector::new(HttpConnector::new(), None);
        let uri: Uri = format!("https://localhost:{}/", addr.port())
            .parse()
            .unwrap();
        let err = connector.call(uri).await.unwrap_err();
        assert!(
            matches!(err, SinkError::Transport { kind } if kind.contains("no upstream TLS configured"))
        );
    }

    #[tokio::test]
    async fn http_dispatch_never_attempts_tls() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 5];
            tokio::io::AsyncReadExt::read_exact(&mut tcp, &mut buf)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut tcp, b"world")
                .await
                .unwrap();
        };
        tokio::spawn(task); // JUSTIFY(spawn): test-only mock server, always inside a running #[tokio::test] runtime
        let mut connector = MaybeTlsConnector::new(HttpConnector::new(), None);
        let uri: Uri = format!("http://localhost:{}/", addr.port())
            .parse()
            .unwrap();
        let stream = connector.call(uri).await.expect("plain connect succeeds");
        let mut stream = TokioIo::new(stream);
        tokio::io::AsyncWriteExt::write_all(&mut stream, b"hello")
            .await
            .unwrap();
        let mut buf = [0u8; 5];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf, b"world");
    }
}
