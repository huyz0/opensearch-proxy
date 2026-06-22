//! Proves mutual-TLS: a client presenting a certificate signed by the trusted
//! CA completes the handshake, the proxy derives a stable identity from that
//! certificate (a SHA-256 fingerprint), and surfaces it to the handler, while a
//! client presenting no certificate is refused at the TLS layer.

// Test scaffolding (helpers + spawned server, not `#[test]` fns) needs the
// unwrap allowance the test-only config does not reach.
#![allow(clippy::unwrap_used)]
// Builds the `ring` provider directly, part of the non-fips test surface.
#![cfg(feature = "non-fips")]

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use osproxy_transport::{serve_tls, IngressHandler, IngressRequest, IngressResponse, RingProvider};
use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName,
};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

/// Echoes the verified client-cert identity the transport extracted.
struct CertEchoHandler;

impl IngressHandler for CertEchoHandler {
    async fn handle(&self, req: IngressRequest) -> IngressResponse {
        let subject = req.client_cert_subject.unwrap_or_default();
        IngressResponse::json(
            200,
            format!(r#"{{"cert_subject":"{subject}"}}"#).into_bytes(),
        )
    }
}

/// A CA and a leaf certificate signed by it.
struct Leaf {
    cert_pem: String,
    key_pem: String,
    cert_der: CertificateDer<'static>,
    key_der: Vec<u8>,
}

struct Pki {
    ca_pem: String,
    ca_der: CertificateDer<'static>,
    server: Leaf,
    client: Leaf,
}

fn build_pki() -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "osproxy-test-ca");
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let server = leaf_signed_by(
        vec!["localhost".to_owned()],
        "localhost",
        None,
        &ca,
        &ca_key,
    );
    let client = leaf_signed_by(
        Vec::new(),
        "client-a",
        Some(ExtendedKeyUsagePurpose::ClientAuth),
        &ca,
        &ca_key,
    );

    Pki {
        ca_pem: ca.pem(),
        ca_der: ca.der().clone(),
        server,
        client,
    }
}

fn leaf_signed_by(
    sans: Vec<String>,
    cn: &str,
    eku: Option<ExtendedKeyUsagePurpose>,
    ca: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> Leaf {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(sans).unwrap();
    params.distinguished_name.push(DnType::CommonName, cn);
    if let Some(eku) = eku {
        params.extended_key_usages = vec![eku];
    }
    let cert = params.signed_by(&key, ca, ca_key).unwrap();
    Leaf {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
        cert_der: cert.der().clone(),
        key_der: key.serialize_der(),
    }
}

async fn spawn_proxy(provider: RingProvider) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = serve_tls(listener, Arc::new(provider), Arc::new(CertEchoHandler)).await;
    });
    addr
}

fn client_config(ca_der: &CertificateDer<'static>, client: Option<&Leaf>) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots.add(ca_der.clone()).unwrap();
    let builder = ClientConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots);
    match client {
        Some(c) => {
            let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(c.key_der.clone()));
            builder
                .with_client_auth_cert(vec![c.cert_der.clone()], key)
                .unwrap()
        }
        None => builder.with_no_client_auth(),
    }
}

#[tokio::test]
async fn client_with_valid_cert_gets_a_verified_identity() {
    let pki = build_pki();
    let provider = RingProvider::from_pem_mtls(
        pki.server.cert_pem.as_bytes(),
        pki.server.key_pem.as_bytes(),
        pki.ca_pem.as_bytes(),
    )
    .unwrap();
    let addr = spawn_proxy(provider).await;

    let connector = TlsConnector::from(Arc::new(client_config(&pki.ca_der, Some(&pki.client))));
    let tcp = TcpStream::connect(addr).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/orders/_doc")
        .header("host", "localhost")
        .body(Full::new(Bytes::from_static(br#"{"tenant_id":"acme"}"#)))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    // A stable cert-derived identity reached the handler.
    assert!(text.contains(r#""cert_subject":"cert:"#), "{text}");
}

#[tokio::test]
async fn client_without_cert_is_refused() {
    let pki = build_pki();
    let provider = RingProvider::from_pem_mtls(
        pki.server.cert_pem.as_bytes(),
        pki.server.key_pem.as_bytes(),
        pki.ca_pem.as_bytes(),
    )
    .unwrap();
    let addr = spawn_proxy(provider).await;

    // No client cert presented. The server requires one, so the connection must
    // not yield a successful response. In TLS 1.3 the client `connect` can
    // complete optimistically and the rejection surfaces on first use, so we
    // accept failure at any of: handshake, HTTP handshake, or send.
    let connector = TlsConnector::from(Arc::new(client_config(&pki.ca_der, None)));
    let tcp = TcpStream::connect(addr).await.unwrap();
    let Ok(tls) = connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
    else {
        return; // refused at the TLS layer, correct.
    };
    let Ok((mut sender, conn)) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await
    else {
        return; // refused as the HTTP handshake reads the closed connection.
    };
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder()
        .method(Method::POST)
        .uri("/orders/_doc")
        .header("host", "localhost")
        .body(Full::new(Bytes::from_static(br#"{"tenant_id":"acme"}"#)))
        .unwrap();
    assert!(
        sender.send_request(req).await.is_err(),
        "request over an unauthenticated connection must fail"
    );
}
