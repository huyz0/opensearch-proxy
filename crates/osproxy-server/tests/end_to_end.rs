//! Full-stack ingest: a real HTTP client speaks to the osproxy ingress (the
//! actual `AppHandler` + reference tenancy), which routes and transforms the
//! document and writes it to a mock OpenSearch. Proves the M1 spine end to end
//! (client → ingress → pipeline → upstream) without Docker; the live
//! testcontainer round-trip is a separate, ignored test.

// Test scaffolding (mock server + helpers, not `#[test]` fns) needs the unwrap
// allowance the test-only config does not reach.
#![allow(clippy::unwrap_used)]
// JUSTIFY(file-length): one cohesive full-stack suite — every scenario (tenanted
// forward, metrics, unresolved partition, token auth, authorizer decline, TLS
// gate, structured log) shares the mock-upstream + handler scaffolding; splitting
// would duplicate it across files for no real separation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use osproxy_core::{ClusterId, IndexName};
use osproxy_engine::{PassthroughPolicy, Pipeline};
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::capture::{MemoryCapture, RedactingCapture};
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_tenancy::TenancyRouter;
use osproxy_transport::IngressHandler;
use tokio::net::TcpListener;

#[derive(Clone, Debug, Default)]
struct Captured {
    method: String,
    uri: String,
    body: String,
}

/// A one-shot mock OpenSearch returning a fixed created response and capturing
/// the request it received.
async fn start_upstream() -> (String, Arc<Mutex<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(Captured::default()));
    let cap = Arc::clone(&captured);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        let svc = service_fn(move |req: Request<Incoming>| {
            let cap = Arc::clone(&cap);
            async move {
                let method = req.method().to_string();
                let uri = req.uri().to_string();
                let body = req.into_body().collect().await.unwrap().to_bytes();
                *cap.lock().unwrap() = Captured {
                    method,
                    uri,
                    body: String::from_utf8_lossy(&body).into_owned(),
                };
                let resp = Response::builder()
                    .status(201)
                    .body(Full::new(Bytes::from(
                        r#"{"_id":"acme:7","result":"created"}"#,
                    )))
                    .unwrap();
                Ok::<_, std::convert::Infallible>(resp)
            }
        });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(io, svc)
            .await
            .unwrap();
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
async fn put_doc_is_tenanted_and_forwarded_upstream() {
    let (upstream, captured) = start_upstream().await;
    let cluster = ClusterId::from("default");
    // The exact wiring the binary builds.
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from("osproxy-shared"), upstream);
    let handler = Arc::new(
        AppHandler::new(
            Pipeline::new(TenancyRouter::new(tenancy), sink),
            ReferenceAuthenticator::dev(),
        )
        .with_require_tls_for_mutation(false), // cleartext test harness (NFR-S1 opt-out)
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });

    // Client POSTs a document carrying its tenant.
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy_addr}/orders/_doc"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"tenant_id":"acme","id":7,"msg":"hi"}"#,
        )))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 201);

    // The upstream received the transformed doc at the constructed id with
    // routing, and the injected tenancy field — proving the full tenanting path.
    let got = captured.lock().unwrap().clone();
    assert_eq!(got.method, "PUT");
    assert_eq!(got.uri, "/osproxy-shared/_doc/acme:7?routing=acme");
    assert!(got.body.contains(r#""_tenant":"acme""#), "{}", got.body);

    // The response carries the request id; fetching /debug/explain/{id} returns
    // the shape-only causal story (blind diagnosis, docs/05 §6).
    let request_id = resp
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let explain = client
        .request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("http://{proxy_addr}/debug/explain/{request_id}"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(explain.status(), 200);
    let doc = explain.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(doc.to_vec()).unwrap();
    assert!(text.contains(r#""partition_id":"acme""#), "{text}");
    assert!(text.contains(r#""outcome":"ok""#), "{text}");
    // No tenant values, only ids/shapes.
    assert!(!text.contains("\"hi\""), "value leaked: {text}");

    assert_metrics_snapshot(&client, proxy_addr).await;
}

/// Scrapes `/metrics` and asserts the shape-only snapshot reflects one served,
/// successful data-plane request and the upstream pool — the prod-safe source an
/// external aggregator reads, with no auth and no tenant values.
async fn assert_metrics_snapshot(
    client: &Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
    proxy_addr: std::net::SocketAddr,
) {
    let metrics = client
        .request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("http://{proxy_addr}/metrics"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metrics.status(), 200);
    let mbody = metrics.into_body().collect().await.unwrap().to_bytes();
    let mtext = String::from_utf8(mbody.to_vec()).unwrap();
    let snap: serde_json::Value = serde_json::from_str(&mtext).unwrap();
    assert_eq!(snap["requests_total"], 1, "one data-plane request: {mtext}");
    assert_eq!(snap["requests_ok"], 1);
    assert_eq!(snap["requests_error"], 0);
    assert_eq!(snap["pools"][0]["cluster"], "default");
    assert_eq!(snap["pools"][0]["dispatched"], 1);
    assert!(!mtext.contains("acme"), "metrics leaked tenant: {mtext}");
}

#[tokio::test]
async fn unresolved_partition_returns_client_error() {
    let (upstream, _captured) = start_upstream().await;
    let cluster = ClusterId::from("default");
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from("osproxy-shared"), upstream);
    let handler = Arc::new(
        AppHandler::new(
            Pipeline::new(TenancyRouter::new(tenancy), sink),
            ReferenceAuthenticator::dev(),
        )
        .with_require_tls_for_mutation(false), // cleartext test harness (NFR-S1 opt-out)
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });

    // No tenant_id => partition cannot be resolved => 400 with a value-free body.
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy_addr}/orders/_doc"))
        .body(Full::new(Bytes::from_static(br#"{"id":7}"#)))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 400);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("partition_unresolved"), "{text}");
}

#[tokio::test]
async fn token_auth_rejects_missing_and_accepts_valid() {
    let (upstream, _captured) = start_upstream().await;
    let cluster = ClusterId::from("default");
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from("osproxy-shared"), upstream);

    let mut tokens = HashMap::new();
    tokens.insert("s3cr3t".to_owned(), "svc-ingest".to_owned());
    let handler = Arc::new(
        AppHandler::new(
            Pipeline::new(TenancyRouter::new(tenancy), sink),
            ReferenceAuthenticator::new(tokens),
        )
        .with_require_tls_for_mutation(false), // cleartext test harness (NFR-S1 opt-out)
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let body = || Full::new(Bytes::from_static(br#"{"tenant_id":"acme","id":7}"#));

    // No token => 401.
    let unauth = client
        .request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("http://{proxy_addr}/orders/_doc"))
                .body(body())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);

    // Valid token => 201.
    let ok = client
        .request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("http://{proxy_addr}/orders/_doc"))
                .header("authorization", "Bearer s3cr3t")
                .body(body())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok.status(), 201);
}

#[tokio::test]
async fn a_mutating_request_over_cleartext_is_refused_when_tls_is_required() {
    // NFR-S1: with enforcement on (the default), a body-mutating endpoint over a
    // cleartext connection is refused with 403 before auth or any upstream call;
    // the same request marked secure is processed. Driven through the handler
    // directly so the `secure` bit can be set without standing up a TLS listener.
    let (upstream, captured) = start_upstream().await;
    let cluster = ClusterId::from("default");
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from("osproxy-shared"), upstream);
    // Default construction enforces NFR-S1 (no opt-out here).
    let handler = AppHandler::new(
        Pipeline::new(TenancyRouter::new(tenancy), sink),
        ReferenceAuthenticator::dev(),
    );

    let ingest = |secure: bool| osproxy_transport::IngressRequest {
        method: osproxy_spi::HttpMethod::Post,
        protocol: osproxy_spi::Protocol::Http1,
        path: "/orders/_doc".to_owned(),
        endpoint: osproxy_core::EndpointKind::IngestDoc,
        logical_index: "orders".to_owned(),
        doc_id: None,
        headers: vec![],
        body: br#"{"tenant_id":"acme","id":7}"#.to_vec(),
        query: None,
        client_cert_subject: None,
        secure,
    };

    // Cleartext: refused with 403, and nothing reached the upstream.
    let refused = handler.handle(ingest(false)).await;
    assert_eq!(refused.status, 403);
    assert!(
        String::from_utf8_lossy(&refused.body).contains("tls_required"),
        "value-free tls_required body: {refused:?}"
    );
    assert_eq!(
        captured.lock().unwrap().method,
        "",
        "a refused request never reaches the upstream"
    );

    // Same request over TLS: processed and forwarded (201 from the mock upstream).
    let ok = handler.handle(ingest(true)).await;
    assert_eq!(ok.status, 201);
    assert_eq!(captured.lock().unwrap().method, "PUT");
}

/// An authorizer that denies one endpoint class and permits the rest, to prove
/// the post-authentication policy layer is consulted and can decline.
struct DenyIngest;

impl osproxy_spi::Authorizer for DenyIngest {
    async fn authorize(
        &self,
        _principal: &osproxy_spi::Principal,
        action: &osproxy_spi::Action,
    ) -> Result<(), osproxy_spi::AuthError> {
        if action.endpoint == osproxy_core::EndpointKind::IngestDoc {
            return Err(osproxy_spi::AuthError::Unauthorized);
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_supplied_authorizer_can_decline_an_authenticated_request() {
    // Authentication succeeds (dev mode), then the supplied authorizer declines
    // the ingest action: 403 Unauthorized, and the write never reaches upstream.
    let (upstream, captured) = start_upstream().await;
    let cluster = ClusterId::from("default");
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from("osproxy-shared"), upstream);
    let handler = AppHandler::new(
        Pipeline::new(TenancyRouter::new(tenancy), sink),
        ReferenceAuthenticator::dev(),
    )
    .with_require_tls_for_mutation(false)
    .with_authorizer(DenyIngest);

    let ingest = osproxy_transport::IngressRequest {
        method: osproxy_spi::HttpMethod::Post,
        protocol: osproxy_spi::Protocol::Http1,
        path: "/orders/_doc".to_owned(),
        endpoint: osproxy_core::EndpointKind::IngestDoc,
        logical_index: "orders".to_owned(),
        doc_id: None,
        headers: vec![],
        body: br#"{"tenant_id":"acme","id":7}"#.to_vec(),
        query: None,
        client_cert_subject: None,
        secure: true,
    };

    let denied = handler.handle(ingest).await;
    assert_eq!(denied.status, 403, "authorizer declined the action");
    assert_eq!(
        captured.lock().unwrap().method,
        "",
        "a request the authorizer declined never reaches the upstream"
    );
}

#[tokio::test]
async fn passthrough_forwards_verbatim_and_capture_records_the_exchange() {
    // Tenant-agnostic mode: no tenant_id in the body (tenancy would reject it),
    // yet passthrough forwards it verbatim to the source and returns the raw
    // response. The capture tees the exchange, with the credential redacted.
    let (upstream, captured) = start_upstream().await;
    let cluster = ClusterId::from("source");

    let sink = OpenSearchSink::new();
    // The router is unused in passthrough mode, but the pipeline is generic over
    // one, so wire the reference tenancy (never consulted here).
    let tenancy = ReferenceTenancy::new(cluster.clone(), IndexName::from("ignored"), &upstream);
    // A dedicated capture/migration proxy captures everything: turn the
    // always-capture baseline on (the alternative is an on-demand `capture`
    // directive; the default-off guarantee is covered by
    // a_wired_capture_sink_stays_off_until_enabled).
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy), sink)
        .with_passthrough(PassthroughPolicy::new(cluster, upstream))
        .with_baseline_capture(true);

    let capture = MemoryCapture::new();
    let handler = Arc::new(
        AppHandler::new(pipeline, ReferenceAuthenticator::dev())
            .with_require_tls_for_mutation(false)
            .with_capture(Box::new(RedactingCapture::new(capture.clone()))),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy_addr}/orders/_doc/raw-1"))
        .header("authorization", "Bearer s3cret")
        .body(Full::new(Bytes::from_static(br#"{"id":7,"msg":"hi"}"#)))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 201);

    // The upstream saw the request verbatim: same method, same path, same body,
    // with nothing injected (no _tenant, no constructed id).
    let got = captured.lock().unwrap().clone();
    assert_eq!(got.method, "POST");
    assert_eq!(got.uri, "/orders/_doc/raw-1");
    assert_eq!(got.body, r#"{"id":7,"msg":"hi"}"#);
    assert!(
        !got.body.contains("_tenant"),
        "no tenancy injection: {}",
        got.body
    );

    // The capture recorded the exchange, full-fidelity body, credential redacted.
    let records = capture.records();
    assert_eq!(records.len(), 1, "one captured exchange");
    assert_eq!(records[0].path, "/orders/_doc/raw-1");
    assert_eq!(records[0].body, br#"{"id":7,"msg":"hi"}"#);
    assert_eq!(records[0].response_status, 201);
    assert!(
        !records[0]
            .headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("authorization")),
        "the captured stream must not carry the credential: {:?}",
        records[0].headers
    );
}

#[tokio::test]
async fn a_wired_capture_sink_stays_off_until_enabled() {
    // The default-off guarantee: wiring a capture sink does NOT start capturing.
    // Capture is on demand — nothing is teed until the always-capture baseline or
    // a published `capture` directive selects requests.
    let (upstream, _captured) = start_upstream().await;
    let cluster = ClusterId::from("source");
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster.clone(), IndexName::from("ignored"), &upstream);
    // Passthrough + a wired sink, but the baseline stays off and no directive is
    // published, so the live decision is "do not capture".
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy), sink)
        .with_passthrough(PassthroughPolicy::new(cluster, upstream));

    let capture = MemoryCapture::new();
    let handler = Arc::new(
        AppHandler::new(pipeline, ReferenceAuthenticator::dev())
            .with_require_tls_for_mutation(false)
            .with_capture(Box::new(capture.clone())),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy_addr}/orders/_doc/raw-1"))
        .body(Full::new(Bytes::from_static(br#"{"id":7}"#)))
        .unwrap();
    assert_eq!(client.request(req).await.unwrap().status(), 201);

    assert!(
        capture.records().is_empty(),
        "a wired sink captures nothing until capture is enabled"
    );
}

/// A request log that records the structured records it is handed.
#[derive(Clone, Default)]
struct RecordingLog(Arc<Mutex<Vec<serde_json::Value>>>);

impl osproxy_server::log::RequestLog for RecordingLog {
    fn emit(&self, record: &serde_json::Value) {
        self.0.lock().unwrap().push(record.clone());
    }
}

#[tokio::test]
async fn a_handled_request_emits_a_structured_log_carrying_the_trace_id() {
    let (upstream, _captured) = start_upstream().await;
    let cluster = ClusterId::from("default");
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from("osproxy-shared"), upstream);

    let log = RecordingLog::default();
    let handler = Arc::new(
        AppHandler::new(
            Pipeline::new(TenancyRouter::new(tenancy), sink),
            ReferenceAuthenticator::dev(),
        )
        .with_request_log(Box::new(log.clone()))
        .with_require_tls_for_mutation(false), // cleartext test harness (NFR-S1 opt-out)
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{proxy_addr}/orders/_doc"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"tenant_id":"acme","id":7}"#,
        )))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 201);

    // The handler emitted exactly one structured record, and it carries the same
    // request_id and trace_id that correlate it with /debug/explain and the OTLP
    // trace.
    let records = log.0.lock().unwrap();
    assert_eq!(records.len(), 1, "one structured log line per request");
    let rec = &records[0];
    assert_eq!(rec["outcome"], "ok");
    assert!(rec["request_id"].is_string(), "record: {rec}");
    assert!(
        rec["trace_id"].as_str().is_some_and(|t| t.len() == 32),
        "structured log carries the 32-hex trace id: {rec}"
    );
}
