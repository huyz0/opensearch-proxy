//! The M1 exit gate (`docs/11`): a `PUT`/`POST` ingest round-trips through the
//! full proxy stack to a **real OpenSearch** running in a container, landing in
//! the right index with the injected tenancy field and the constructed `_id`;
//! and the blind-diagnosis story works for a success and a failure.
//!
//! These tests need a running Docker daemon, so they are `#[ignore]`'d — CI
//! without Docker stays green. Run them with:
//!   cargo test -p osproxy-server --test testcontainer -- --ignored

// Test scaffolding (helpers + spawned server/container, not `#[test]` fns).
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_core::{ClusterId, IndexName};
use osproxy_engine::Pipeline;
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_tenancy::TenancyRouter;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::net::TcpListener;

const INDEX: &str = "osproxy-shared";
type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

/// Starts a single-node OpenSearch (security disabled) and returns its base URL.
async fn start_opensearch() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("opensearchproject/opensearch", "2.11.1")
        .with_exposed_port(ContainerPort::Tcp(9200))
        .with_wait_for(WaitFor::message_on_stdout("] started"))
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .with_env_var("bootstrap.memory_lock", "false")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(9200).await.unwrap();
    let base = format!("http://{host}:{port}");
    (container, base)
}

/// Polls cluster health until OpenSearch answers; returns whether it became
/// ready within the timeout (the caller asserts).
async fn wait_ready(client: &HttpClient, base: &str) -> bool {
    for _ in 0..60 {
        if let Ok((200, _)) = get(client, &format!("{base}/_cluster/health")).await {
            return true;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    false
}

/// Spawns the proxy (real [`OpenSearchSink`] to `upstream`) and returns its base
/// URL.
async fn spawn_proxy(upstream: String) -> String {
    let cluster = ClusterId::from("default");
    let mut endpoints = HashMap::new();
    endpoints.insert(cluster.clone(), upstream);
    let sink = OpenSearchSink::new(endpoints);
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from(INDEX));
    let handler = Arc::new(AppHandler::new(
        Pipeline::new(TenancyRouter::new(tenancy), sink),
        ReferenceAuthenticator::dev(),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });
    format!("http://{addr}")
}

async fn get(client: &HttpClient, url: &str) -> Result<(u16, String), String> {
    send(client, Method::GET, url, Bytes::new()).await
}

/// A `GET` carrying the `x-tenant` partition header the proxy needs to resolve a
/// by-id read (there is no body to carry the partition on a read).
async fn get_with_tenant(
    client: &HttpClient,
    url: &str,
    tenant: &str,
) -> Result<(u16, String), String> {
    request_with_tenant(client, Method::GET, url, tenant, Bytes::new()).await
}

/// A request carrying the `x-tenant` partition header (for header-routed reads
/// and searches).
async fn request_with_tenant(
    client: &HttpClient,
    method: Method,
    url: &str,
    tenant: &str,
    body: Bytes,
) -> Result<(u16, String), String> {
    let req = Request::builder()
        .method(method)
        .uri(url)
        .header("content-type", "application/json")
        .header("x-tenant", tenant)
        .body(Full::new(body))
        .map_err(|e| e.to_string())?;
    let resp: Response<_> = client.request(req).await.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| e.to_string())?
        .to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

async fn send(
    client: &HttpClient,
    method: Method,
    url: &str,
    body: Bytes,
) -> Result<(u16, String), String> {
    let req = Request::builder()
        .method(method)
        .uri(url)
        .header("content-type", "application/json")
        .body(Full::new(body))
        .map_err(|e| e.to_string())?;
    let resp: Response<_> = client.request(req).await.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| e.to_string())?
        .to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn ingest_round_trips_to_real_opensearch() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    assert!(
        wait_ready(&client, &os_base).await,
        "opensearch did not become ready"
    );
    let proxy = spawn_proxy(os_base.clone()).await;

    // Ingest a document through the proxy.
    let (status, body) = send(
        &client,
        Method::POST,
        &format!("{proxy}/orders/_doc"),
        Bytes::from_static(br#"{"tenant_id":"acme","id":7,"msg":"hello"}"#),
    )
    .await
    .unwrap();
    assert_eq!(status, 201, "proxy ingest failed: {body}");
    assert!(body.contains(r#""_id":"acme:7""#), "{body}");

    // The document is in OpenSearch, in the shared index, at the constructed id,
    // with the injected tenancy field and routing — query OpenSearch directly.
    let (status, doc) = get(
        &client,
        &format!("{os_base}/{INDEX}/_doc/acme:7?routing=acme"),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "doc not found in opensearch: {doc}");
    let parsed: serde_json::Value = serde_json::from_str(&doc).unwrap();
    assert_eq!(parsed["_index"], INDEX);
    assert_eq!(parsed["_id"], "acme:7");
    assert_eq!(parsed["_source"]["_tenant"], "acme");
    assert_eq!(parsed["_source"]["msg"], "hello");
    assert_eq!(parsed["_routing"], "acme");

    // The proxy read→delete→read round-trip in the client's logical terms.
    assert_logical_read(&client, &proxy).await;
    assert_delete_removes(&client, &proxy).await;
}

/// Reads the doc back through the proxy in the client's logical view, and that a
/// miss is a logical not-found (not a leak of physical naming).
async fn assert_logical_read(client: &HttpClient, proxy: &str) {
    let (status, logical) = get_with_tenant(client, &format!("{proxy}/orders/_doc/7"), "acme")
        .await
        .unwrap();
    assert_eq!(status, 200, "proxy read failed: {logical}");
    let seen: serde_json::Value = serde_json::from_str(&logical).unwrap();
    assert_eq!(seen["_index"], "orders");
    assert_eq!(seen["_id"], "7");
    assert!(seen.get("_routing").is_none(), "{logical}");
    assert!(seen["_source"].get("_tenant").is_none(), "{logical}");
    assert_eq!(seen["_source"]["msg"], "hello");

    let (status, miss) = get_with_tenant(client, &format!("{proxy}/orders/_doc/999"), "acme")
        .await
        .unwrap();
    assert_eq!(status, 404, "{miss}");
    let miss: serde_json::Value = serde_json::from_str(&miss).unwrap();
    assert_eq!(miss["_id"], "999");
    assert_eq!(miss["found"], false);
}

/// Deletes the doc through the proxy by logical id and confirms it is gone — the
/// write→delete→read round-trip (`docs/04` §5).
async fn assert_delete_removes(client: &HttpClient, proxy: &str) {
    let (status, deleted) = request_with_tenant(
        client,
        Method::DELETE,
        &format!("{proxy}/orders/_doc/7"),
        "acme",
        Bytes::new(),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{deleted}");
    let deleted: serde_json::Value = serde_json::from_str(&deleted).unwrap();
    assert_eq!(deleted["_id"], "7");
    assert_eq!(deleted["result"], "deleted");

    let (status, gone) = get_with_tenant(client, &format!("{proxy}/orders/_doc/7"), "acme")
        .await
        .unwrap();
    assert_eq!(status, 404, "doc should be gone after delete: {gone}");
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn search_is_isolated_to_the_callers_partition() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    assert!(
        wait_ready(&client, &os_base).await,
        "opensearch did not become ready"
    );
    let proxy = spawn_proxy(os_base.clone()).await;

    // Two tenants ingest into the same shared index through the proxy.
    for (tenant, id, msg) in [("acme", 1, "acme-doc"), ("globex", 1, "globex-doc")] {
        let body = format!(r#"{{"tenant_id":"{tenant}","id":{id},"msg":"{msg}"}}"#);
        let (status, b) = send(
            &client,
            Method::POST,
            &format!("{proxy}/orders/_doc"),
            Bytes::from(body),
        )
        .await
        .unwrap();
        assert_eq!(status, 201, "{b}");
    }
    // Make the writes visible to search.
    let _ = send(
        &client,
        Method::POST,
        &format!("{os_base}/{INDEX}/_refresh"),
        Bytes::new(),
    )
    .await
    .unwrap();

    // acme searches match_all *through the proxy*: it sees only its own document
    // — the mandatory partition filter isolates it from globex (docs/03 §5).
    let (status, hits) = request_with_tenant(
        &client,
        Method::POST,
        &format!("{proxy}/orders/_search"),
        "acme",
        Bytes::from_static(br#"{"query":{"match_all":{}}}"#),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{hits}");
    let parsed: serde_json::Value = serde_json::from_str(&hits).unwrap();
    let hits_arr = parsed["hits"]["hits"].as_array().unwrap();
    assert_eq!(hits_arr.len(), 1, "expected only acme's doc: {hits}");
    let hit = &hits_arr[0];
    assert_eq!(hit["_index"], "orders");
    assert_eq!(hit["_id"], "1");
    assert!(hit["_source"].get("_tenant").is_none(), "{hits}");
    assert_eq!(hit["_source"]["msg"], "acme-doc");
    // Globex's value never appears in acme's results.
    assert!(!hits.contains("globex-doc"), "isolation breach: {hits}");
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn blind_diagnosis_for_success_and_failure() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    assert!(
        wait_ready(&client, &os_base).await,
        "opensearch did not become ready"
    );
    let proxy = spawn_proxy(os_base).await;

    // Success: ingest, then fetch /debug/explain for its request id.
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("{proxy}/orders/_doc"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"tenant_id":"acme","id":1}"#,
        )))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), 201);
    let request_id = resp
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let (status, explain) = get(&client, &format!("{proxy}/debug/explain/{request_id}"))
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert!(explain.contains(r#""outcome":"ok""#), "{explain}");
    assert!(explain.contains(r#""partition_id":"acme""#), "{explain}");
    assert!(explain.contains(r#""upstream_status":201"#), "{explain}");
    // No tenant value leaked.
    assert!(!explain.contains("\"hello\""), "value leaked: {explain}");

    // Failure: no partition key => 400, diagnosable from the explain document.
    let (status, body) = send(
        &client,
        Method::POST,
        &format!("{proxy}/orders/_doc"),
        Bytes::from_static(br#"{"id":2}"#),
    )
    .await
    .unwrap();
    assert_eq!(status, 400);
    assert!(body.contains("partition_unresolved"), "{body}");
}
