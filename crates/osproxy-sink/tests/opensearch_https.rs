//! Exercises [`OpenSearchSink`] against a **real TLS** mock upstream — the
//! `https://` counterpart to `opensearch_http.rs`'s cleartext suite. Proves
//! the whole path end to end: `Target`'s endpoint scheme selects the TLS
//! connector, the handshake actually completes against a self-signed leaf
//! cert trusted via `with_upstream_tls`, and the request/response round-trips
//! same as cleartext.
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use osproxy_core::{ClusterId, Epoch, IndexName, Target};
use osproxy_sink::{DocOp, OpenSearchSink, Sink, WriteBatch, WriteOp};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

fn self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let key = PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
    (cert.cert.der().clone(), key)
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

/// Starts a one-shot **TLS** mock server that returns a canned index response,
/// counting the requests it actually served (so a test can assert the real
/// handshake — not a fallback to cleartext — is what completed).
async fn start_tls_mock() -> (String, CertificateDer<'static>, Arc<AtomicUsize>) {
    let (cert, key) = self_signed();
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = Arc::new(AtomicUsize::new(0));
    let served_for_task = Arc::clone(&served);

    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let tls = acceptor.accept(tcp).await.unwrap();
        let io = TokioIo::new(tls);
        let served = Arc::clone(&served_for_task);
        let service = service_fn(move |req: Request<Incoming>| {
            let served = Arc::clone(&served);
            async move {
                let _ = req.into_body().collect().await.unwrap().to_bytes();
                served.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(
                    r#"{"_index":"orders","_id":"1","result":"created"}"#,
                ))))
            }
        });
        let _ = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(io, service)
            .await;
    });
    (format!("https://localhost:{}", addr.port()), cert, served)
}

#[tokio::test]
async fn a_write_over_https_completes_a_real_tls_handshake_and_round_trips() {
    let (base, cert, served) = start_tls_mock().await;
    let sink = OpenSearchSink::new().with_upstream_tls(client_config_trusting(&cert));
    let target =
        Target::new(ClusterId::from("c1"), IndexName::from("orders")).with_endpoint(Some(base));

    let op = WriteOp::new(
        target,
        DocOp::Index {
            id: Some("1".to_owned()),
            routing: None,
            body: Bytes::from_static(br#"{"hello":"world"}"#),
        },
        Epoch::new(1),
    );
    let ack = sink
        .write(WriteBatch::single(op))
        .await
        .expect("write over https succeeds");

    assert!(ack.all_succeeded());
    assert_eq!(ack.results()[0].id, "1");
    assert_eq!(served.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_write_over_https_without_upstream_tls_configured_fails_closed() {
    let (base, _cert, served) = start_tls_mock().await;
    let sink = OpenSearchSink::new();
    let target =
        Target::new(ClusterId::from("c1"), IndexName::from("orders")).with_endpoint(Some(base));

    let op = WriteOp::new(
        target,
        DocOp::Index {
            id: Some("1".to_owned()),
            routing: None,
            body: Bytes::from_static(br#"{"hello":"world"}"#),
        },
        Epoch::new(1),
    );
    // The sink's send() choke point folds every connector failure into one
    // generic fail_kind (same as a plain TCP-connect failure), so the
    // MaybeTlsConnector's more specific "no upstream TLS configured" reason
    // does not surface here — what matters is that this fails closed rather
    // than silently connecting in cleartext or serving the request.
    sink.write(WriteBatch::single(op))
        .await
        .expect_err("no upstream TLS configured, so this must fail closed");
    assert_eq!(served.load(Ordering::SeqCst), 0);
}
