//! The M1 exit gate (`docs/11`): a `PUT`/`POST` ingest round-trips through the
//! full proxy stack to a **real OpenSearch** running in a container, landing in
//! the right index with the injected tenancy field and the constructed `_id`;
//! and the blind-diagnosis story works for a success and a failure.
//!
//! These tests need a running Docker daemon, so they are `#[ignore]`'d, CI
//! without Docker stays green. Run them with:
//!   cargo test -p osproxy-server --test testcontainer -- --ignored

// Test scaffolding (helpers + spawned server/container, not `#[test]` fns).
#![allow(clippy::unwrap_used)]
// JUSTIFY(file-length): the live-OpenSearch exit gate, several end-to-end
// scenarios (ingest, by-id read/delete, search/count isolation, bulk demux with
// create/update verbs, blind diagnosis) share one set of container/proxy
// scaffolding helpers.
// Splitting into multiple files would duplicate that ~120-line scaffold per
// file (and spin extra containers); keeping them together is the cohesive unit.

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
use osproxy_server::cursor::HmacCursorSigner;
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
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from(INDEX), upstream);
    let handler = Arc::new(
        AppHandler::new(
            Pipeline::new(TenancyRouter::new(tenancy), sink),
            ReferenceAuthenticator::dev(),
        )
        // Cleartext test harness: allow mutating requests over h1 (NFR-S1 opt-out).
        .with_require_tls_for_mutation(false),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, handler).await;
    });
    format!("http://{addr}")
}

/// Like [`spawn_proxy`], but with scroll/PIT cursor affinity enabled (a fixed
/// test key), so scroll responses carry a signed `_scroll_id` envelope.
async fn spawn_proxy_with_affinity(upstream: String) -> String {
    let cluster = ClusterId::from("default");
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from(INDEX), upstream);
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy), sink)
        .with_cursor_signer(Arc::new(HmacCursorSigner::new(b"scroll-test-key")));
    let handler = Arc::new(
        AppHandler::new(pipeline, ReferenceAuthenticator::dev())
            .with_require_tls_for_mutation(false),
    );

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
    // The proxy echoes the client's *logical* id (`7`); the partition-prefixed
    // physical id (`acme:7`) is an upstream detail and must not leak to the client.
    assert!(body.contains(r#""_id":"7""#), "{body}");
    assert!(
        !body.contains("acme:7"),
        "physical id leaked to client: {body}"
    );

    // The document is in OpenSearch, in the shared index, at the constructed id,
    // with the injected tenancy field and routing, query OpenSearch directly.
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

/// Deletes the doc through the proxy by logical id and confirms it is gone, the
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

    // acme searches match_all *through the proxy*: it sees only its own document,
    // the mandatory partition filter isolates it from globex (docs/03 §5).
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

    // _count is scoped to the partition too: acme counts only its own docs.
    assert_count_is_partition_scoped(&client, &proxy).await;
}

/// `_count` through the proxy returns only the caller partition's total.
async fn assert_count_is_partition_scoped(client: &HttpClient, proxy: &str) {
    let (status, counted) = request_with_tenant(
        client,
        Method::POST,
        &format!("{proxy}/orders/_count"),
        "acme",
        Bytes::from_static(br#"{"query":{"match_all":{}}}"#),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{counted}");
    let counted: serde_json::Value = serde_json::from_str(&counted).unwrap();
    assert_eq!(
        counted["count"], 1,
        "count must be partition-scoped: {counted}"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn bulk_demux_round_trips_to_real_opensearch() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    assert!(
        wait_ready(&client, &os_base).await,
        "opensearch did not become ready"
    );
    let proxy = spawn_proxy(os_base.clone()).await;

    // A mixed-partition bulk through the proxy: two acme docs and one globex.
    let ndjson = concat!(
        "{\"index\":{}}\n{\"tenant_id\":\"acme\",\"id\":1,\"msg\":\"a1\"}\n",
        "{\"index\":{}}\n{\"tenant_id\":\"globex\",\"id\":2,\"msg\":\"g2\"}\n",
        "{\"index\":{}}\n{\"tenant_id\":\"acme\",\"id\":3,\"msg\":\"a3\"}\n",
    );
    let (status, body) = send(
        &client,
        Method::POST,
        &format!("{proxy}/orders/_bulk"),
        Bytes::from_static(ndjson.as_bytes()),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{body}");
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["errors"], false, "{body}");
    let items = parsed["items"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    for item in items {
        assert_eq!(item["index"]["status"], 201, "{body}");
    }

    let _ = send(
        &client,
        Method::POST,
        &format!("{os_base}/{INDEX}/_refresh"),
        Bytes::new(),
    )
    .await
    .unwrap();

    // acme sees exactly its two bulk docs; globex's is isolated out.
    let (status, counted) = request_with_tenant(
        &client,
        Method::POST,
        &format!("{proxy}/orders/_count"),
        "acme",
        Bytes::from_static(br#"{"query":{"match_all":{}}}"#),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{counted}");
    let counted: serde_json::Value = serde_json::from_str(&counted).unwrap();
    assert_eq!(counted["count"], 2, "acme bulk docs, isolated: {counted}");

    // The create/update verbs also round-trip and stay tenanted.
    assert_bulk_create_and_update(&client, &os_base, &proxy).await;
}

/// A second bulk exercising `create` and `update` end-to-end: an upsert that
/// creates a tenanted doc, a partial `doc` update of an existing doc, and a
/// `create` conflict that is positioned as a per-item error. Verified by reading
/// the physical documents straight from OpenSearch.
async fn assert_bulk_create_and_update(client: &HttpClient, os_base: &str, proxy: &str) {
    // Header-routed (the update bodies carry no partition key): create acme:5,
    // upsert-create acme:6, and patch the existing acme:1.
    let ndjson = concat!(
        "{\"create\":{\"_id\":\"5\"}}\n{\"msg\":\"c5\"}\n",
        "{\"update\":{\"_id\":\"6\"}}\n{\"doc\":{\"msg\":\"u6\"},\"upsert\":{\"msg\":\"up6\"}}\n",
        "{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"msg\":\"a1-patched\"}}\n",
    );
    let (status, body) = request_with_tenant(
        client,
        Method::POST,
        &format!("{proxy}/orders/_bulk"),
        "acme",
        Bytes::from(ndjson.to_owned()),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{body}");
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["errors"], false, "create+update bulk: {body}");

    let _ = send(
        client,
        Method::POST,
        &format!("{os_base}/{INDEX}/_refresh"),
        Bytes::new(),
    )
    .await
    .unwrap();

    // The physical docs exist, tenanted: acme:5 (create), acme:6 (upsert-create),
    // acme:1 (patched). The injected `_tenant` is present on each.
    for (id, msg) in [("5", "c5"), ("6", "up6"), ("1", "a1-patched")] {
        let (status, doc) = get(
            client,
            &format!("{os_base}/{INDEX}/_doc/acme:{id}?routing=acme"),
        )
        .await
        .unwrap();
        assert_eq!(status, 200, "acme:{id} missing: {doc}");
        let parsed: serde_json::Value = serde_json::from_str(&doc).unwrap();
        assert_eq!(parsed["_source"]["_tenant"], "acme", "{doc}");
        assert_eq!(parsed["_source"]["msg"], msg, "{doc}");
    }

    // A `create` of an existing id is a positioned conflict (errors:true), not a
    // silent overwrite, proving op_type=create reached OpenSearch.
    let (status, body) = request_with_tenant(
        client,
        Method::POST,
        &format!("{proxy}/orders/_bulk"),
        "acme",
        Bytes::from_static(b"{\"create\":{\"_id\":\"5\"}}\n{\"msg\":\"dup\"}\n"),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "{body}");
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        parsed["errors"], true,
        "create conflict should error: {body}"
    );
    assert_eq!(parsed["items"][0]["create"]["status"], 409, "{body}");
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

/// Ingests three `acme` docs through the proxy and refreshes the index so they
/// are searchable.
async fn seed_acme_docs(client: &HttpClient, proxy: &str, os_base: &str) {
    for id in 1..=3 {
        let (status, body) = send(
            client,
            Method::POST,
            &format!("{proxy}/orders/_doc"),
            Bytes::from(format!(r#"{{"tenant_id":"acme","id":{id},"msg":"m{id}"}}"#)),
        )
        .await
        .unwrap();
        assert_eq!(status, 201, "ingest {id}: {body}");
    }
    let _ = send(
        client,
        Method::POST,
        &format!("{os_base}/{INDEX}/_refresh"),
        Bytes::new(),
    )
    .await
    .unwrap();
}

/// The full scroll loop against real OpenSearch: a scroll-opening search returns
/// a *wrapped* `_scroll_id` (proving `?scroll=` reached the upstream), and a
/// continue with that wrapped id pages forward (proving unwrap + route + re-wrap).
#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn scroll_create_and_continue_round_trip_through_the_proxy() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    assert!(wait_ready(&client, &os_base).await, "opensearch not ready");
    let proxy = spawn_proxy_with_affinity(os_base.clone()).await;
    seed_acme_docs(&client, &proxy, &os_base).await;

    // Open a scroll (size 1 so it spans multiple pages). Searches carry the
    // partition in the `x-tenant` header, not the body.
    let (status, body) = request_with_tenant(
        &client,
        Method::POST,
        &format!("{proxy}/orders/_search?scroll=1m"),
        "acme",
        Bytes::from_static(br#"{"size":1,"query":{"match_all":{}}}"#),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "scroll open: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let scroll_id = v["_scroll_id"]
        .as_str()
        .expect("scroll create returns a _scroll_id");
    assert!(
        scroll_id.contains('.'),
        "the scroll id must be a wrapped envelope, not the raw upstream id: {scroll_id}"
    );
    assert_eq!(
        v["hits"]["hits"].as_array().unwrap().len(),
        1,
        "first page has one hit: {body}"
    );

    // Continue with the WRAPPED id: the proxy unwraps, routes, and re-wraps the
    // next page's id.
    let (status, body) = send(
        &client,
        Method::POST,
        &format!("{proxy}/_search/scroll"),
        Bytes::from(format!(r#"{{"scroll":"1m","scroll_id":"{scroll_id}"}}"#)),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "scroll continue: {body}");
    let v2: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        v2["_scroll_id"].as_str().is_some_and(|s| s.contains('.')),
        "the continue response re-wraps the next page id: {body}"
    );
    assert_eq!(
        v2["hits"]["hits"].as_array().unwrap().len(),
        1,
        "second page has one hit: {body}"
    );
}

/// The full point-in-time loop against real OpenSearch (2.11): create on the
/// resolved cluster returns a *wrapped* `pit_id` (proving the OpenSearch
/// `_search/point_in_time` endpoint was hit), a PIT search routes back there and
/// stays partition-isolated, and a close with the wrapped id succeeds. This is the
/// regression guard for the ES→OpenSearch PIT shape (`_search/point_in_time`,
/// `pit_id` array), see `docs/specs/opensearch-endpoints.md`.
#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn pit_create_search_and_close_round_trip_through_the_proxy() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    assert!(wait_ready(&client, &os_base).await, "opensearch not ready");
    let proxy = spawn_proxy_with_affinity(os_base.clone()).await;
    seed_acme_docs(&client, &proxy, &os_base).await;

    // Create a PIT on the orders index (resolves acme's cluster, wraps the id).
    let (status, body) = request_with_tenant(
        &client,
        Method::POST,
        &format!("{proxy}/orders/_search/point_in_time?keep_alive=5m"),
        "acme",
        Bytes::new(),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "pit create: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let pit_id = v["pit_id"].as_str().expect("create returns a pit_id");
    assert!(
        pit_id.contains('.'),
        "the pit id must be a wrapped envelope, not the raw upstream id: {pit_id}"
    );

    // Search the PIT (no index in the path, the PIT defines the index set). It
    // must route to the pinned cluster yet stay scoped to acme's partition.
    let (status, body) = request_with_tenant(
        &client,
        Method::POST,
        &format!("{proxy}/_search"),
        "acme",
        Bytes::from(format!(
            r#"{{"query":{{"match_all":{{}}}},"pit":{{"id":"{pit_id}","keep_alive":"5m"}}}}"#
        )),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "pit search: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["hits"]["total"]["value"].as_u64(),
        Some(3),
        "pit search sees acme's three docs, partition-scoped: {body}"
    );
    assert!(
        v["pit_id"].as_str().is_some_and(|s| s.contains('.')),
        "the search response re-wraps the refreshed pit_id: {body}"
    );

    // Close the PIT with the wrapped id (the OpenSearch `pit_id` array shape).
    let (status, body) = send(
        &client,
        Method::DELETE,
        &format!("{proxy}/_search/point_in_time"),
        Bytes::from(format!(r#"{{"pit_id":["{pit_id}"]}}"#)),
    )
    .await
    .unwrap();
    assert_eq!(status, 200, "pit close: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["pits"][0]["successful"].as_bool(),
        Some(true),
        "the pit was closed on its pinned cluster: {body}"
    );
}
