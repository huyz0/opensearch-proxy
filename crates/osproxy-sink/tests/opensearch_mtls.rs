//! Proves *mutual* TLS to the upstream: a mock OpenSearch cluster that
//! requires a client certificate accepts writes from [`OpenSearchSink`] when
//! its `ClientConfig` carries a CA-signed client identity (the same shape
//! `osproxy-transport::RingProvider::upstream_client_config` builds, verified
//! separately in that crate's own tests), and refuses the handshake outright
//! when the sink offers server-auth only. Real handshakes both ways, not a
//! config-shape assertion.
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
use rcgen::{BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

/// A CA and a leaf certificate signed by it.
struct Leaf {
    cert_der: CertificateDer<'static>,
    key_der: Vec<u8>,
}

struct Pki {
    ca_der: CertificateDer<'static>,
    server: Leaf,
    client: Leaf,
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
        cert_der: cert.der().clone(),
        key_der: key.serialize_der(),
    }
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
        ca_der: ca.der().clone(),
        server,
        client,
    }
}

fn server_config(pki: &Pki) -> Arc<ServerConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(pki.ca_der.clone()).unwrap();
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(pki.server.key_der.clone()));
    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![pki.server.cert_der.clone()], key)
        .unwrap();
    Arc::new(config)
}

/// Builds the client-side `ClientConfig`, trusting the CA and, when `client`
/// is given, presenting that leaf as this connection's identity — the same
/// shape `osproxy-transport`'s `upstream_client_config` produces, built here
/// directly so this crate's tests don't take a sibling-crate dependency
/// (`docs/01`'s downward-only DAG: siblings never depend on each other).
fn client_config(ca_der: &CertificateDer<'static>, client: Option<&Leaf>) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(ca_der.clone()).unwrap();
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match client {
        Some(leaf) => {
            let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(leaf.key_der.clone()));
            builder
                .with_client_auth_cert(vec![leaf.cert_der.clone()], key)
                .unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    Arc::new(config)
}

/// Starts a one-shot mTLS mock cluster, counting requests it actually served.
async fn start_mtls_mock(pki: &Pki) -> (String, Arc<AtomicUsize>) {
    let acceptor = TlsAcceptor::from(server_config(pki));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = Arc::new(AtomicUsize::new(0));
    let served_for_task = Arc::clone(&served);

    let task = async move {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let Ok(tls) = acceptor.accept(tcp).await else {
            return; // refused at the handshake, the "no identity" case.
        };
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
    };
    tokio::spawn(task); // JUSTIFY(spawn): test-only mock server, always inside a running #[tokio::test] runtime
    (format!("https://localhost:{}", addr.port()), served)
}

fn write_op(base: String) -> WriteOp {
    let target =
        Target::new(ClusterId::from("c1"), IndexName::from("orders")).with_endpoint(Some(base));
    WriteOp::new(
        target,
        DocOp::Index {
            id: Some("1".to_owned()),
            routing: None,
            body: Bytes::from_static(br#"{"hello":"world"}"#),
        },
        Epoch::new(1),
    )
}

#[tokio::test]
async fn a_client_identity_completes_a_real_mutual_tls_handshake_and_writes() {
    let pki = build_pki();
    let (base, served) = start_mtls_mock(&pki).await;
    let sink =
        OpenSearchSink::new().with_upstream_tls(client_config(&pki.ca_der, Some(&pki.client)));

    let ack = sink
        .write(WriteBatch::single(write_op(base)))
        .await
        .expect("mutual TLS write succeeds");

    assert!(ack.all_succeeded());
    assert_eq!(served.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn no_client_identity_is_refused_by_a_cluster_that_requires_mutual_tls() {
    let pki = build_pki();
    let (base, served) = start_mtls_mock(&pki).await;
    // Trusts the server's CA but presents no client certificate at all.
    let sink = OpenSearchSink::new().with_upstream_tls(client_config(&pki.ca_der, None));

    sink.write(WriteBatch::single(write_op(base)))
        .await
        .expect_err("a cluster requiring mutual TLS must refuse a client with no identity");
    assert_eq!(served.load(Ordering::SeqCst), 0);
}
