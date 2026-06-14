//! Proves downstream TLS termination end to end: a rustls client completes a TLS
//! handshake against the ingress (server cert from a self-signed test CA), sends
//! an HTTP/1.1 request over the encrypted connection, and gets a response. The
//! handler is unchanged from the cleartext path — TLS is transparent to it.

// Test scaffolding (helpers + spawned server, not `#[test]` fns) needs the
// unwrap allowance the test-only config does not reach.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use osproxy_core::EndpointKind;
use osproxy_transport::{serve_tls, IngressHandler, IngressRequest, IngressResponse, RingProvider};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

struct EchoHandler;

impl IngressHandler for EchoHandler {
    async fn handle(&self, req: IngressRequest) -> IngressResponse {
        let ingest = req.endpoint == EndpointKind::IngestDoc;
        IngressResponse::json(
            201,
            format!(r#"{{"index":"{}","ingest":{ingest}}}"#, req.logical_index).into_bytes(),
        )
    }
}

/// A self-signed cert for `localhost`: server PEM (cert+key) and the DER the
/// client trusts as its root.
struct TestCert {
    cert_pem: String,
    key_pem: String,
    cert_der: tokio_rustls::rustls::pki_types::CertificateDer<'static>,
}

fn test_cert() -> TestCert {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    TestCert {
        cert_pem: cert.cert.pem(),
        key_pem: cert.key_pair.serialize_pem(),
        cert_der: cert.cert.der().clone(),
    }
}

/// Builds a rustls client connector that trusts `cert_der`.
fn client_connector(
    cert_der: tokio_rustls::rustls::pki_types::CertificateDer<'static>,
) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    roots.add(cert_der).unwrap();
    let config = ClientConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

#[tokio::test]
async fn put_doc_round_trips_over_tls() {
    let tc = test_cert();
    let provider = RingProvider::from_pem(tc.cert_pem.as_bytes(), tc.key_pem.as_bytes()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve_tls(listener, Arc::new(provider), Arc::new(EchoHandler)).await;
    });

    // TLS-connect and send one request.
    let connector = client_connector(tc.cert_der);
    let tcp = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let tls = connector.connect(server_name, tcp).await.unwrap();

    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let req = Request::builder()
        .method(Method::PUT)
        .uri("/orders/_doc/acme:1")
        .header("host", "localhost")
        .body(Full::new(Bytes::from_static(br#"{"tenant_id":"acme"}"#)))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 201);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains(r#""index":"orders""#), "{text}");
    assert!(text.contains(r#""ingest":true"#), "{text}");
}

#[test]
fn invalid_pem_is_rejected() {
    assert!(RingProvider::from_pem(b"not a cert", b"not a key").is_err());
}

#[test]
fn alpn_advertises_h2_then_http11() {
    use osproxy_transport::CryptoProvider;
    let tc = test_cert();
    let provider = RingProvider::from_pem(tc.cert_pem.as_bytes(), tc.key_pem.as_bytes()).unwrap();
    // h2 preferred, http/1.1 as the fallback — so a TLS client negotiates HTTP/2
    // when it can, and the auto ingress builder serves whichever is selected.
    assert_eq!(
        provider.server_config().alpn_protocols,
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    );
}
