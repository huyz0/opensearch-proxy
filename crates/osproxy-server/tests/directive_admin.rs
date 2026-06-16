//! The `/admin/directives` control-plane channel: `POST` publishes a directive
//! set (token-gated, fail-closed, live on the same pipeline the requests flow
//! through — the fleet flip with no restart), and `GET` introspects the settings
//! the instance is currently applying (the agent's read-back of fleet state).

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use osproxy_core::{ClusterId, EndpointKind, IndexName, ManualClock};
use osproxy_engine::Pipeline;
use osproxy_observe::{BreakGlassBuffer, DirectiveStore, InMemoryDirectiveStore};
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_spi::HttpMethod;
use osproxy_tenancy::TenancyRouter;
use osproxy_transport::{IngressHandler, IngressRequest};

const TOKEN: &str = "admin-secret";

fn sink() -> OpenSearchSink {
    OpenSearchSink::new(HashMap::<ClusterId, String>::new())
}

fn tenancy() -> ReferenceTenancy {
    ReferenceTenancy::new(ClusterId::from("c"), IndexName::from("shared"))
}

fn post(body: &str, token: Option<&str>) -> IngressRequest {
    let headers = token
        .map(|t| vec![("authorization".to_owned(), format!("Bearer {t}"))])
        .unwrap_or_default();
    IngressRequest {
        method: HttpMethod::Post,
        path: "/admin/directives".to_owned(),
        endpoint: EndpointKind::Unknown,
        logical_index: String::new(),
        doc_id: None,
        headers,
        body: body.as_bytes().to_vec(),
        query: None,
        client_cert_subject: None,
        secure: false,
    }
}

/// A `GET /admin/directives` introspection request, optionally bearing a token.
fn get(token: Option<&str>) -> IngressRequest {
    let headers = token
        .map(|t| vec![("authorization".to_owned(), format!("Bearer {t}"))])
        .unwrap_or_default();
    IngressRequest {
        method: HttpMethod::Get,
        path: "/admin/directives".to_owned(),
        endpoint: EndpointKind::Unknown,
        logical_index: String::new(),
        doc_id: None,
        headers,
        body: Vec::new(),
        query: None,
        client_cert_subject: None,
        secure: false,
    }
}

/// A handler with the admin channel enabled against `store`.
fn admin_handler(
    store: Arc<InMemoryDirectiveStore>,
    pipeline: Pipeline<ReferenceTenancy, OpenSearchSink>,
) -> AppHandler<ReferenceAuthenticator> {
    AppHandler::new(pipeline, ReferenceAuthenticator::dev()).with_directive_admin(
        store,
        TOKEN.to_owned(),
        Arc::new(ManualClock::new()),
    )
}

#[tokio::test]
async fn publishing_requires_the_admin_token() {
    let store = Arc::new(InMemoryDirectiveStore::new());
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy()), sink());
    let handler = admin_handler(store.clone(), pipeline);

    let body = r#"{"directives":[{"id":"a","level":"Shape","ttl_secs":60}]}"#;
    // No token, then a wrong token: both rejected, nothing published.
    assert_eq!(handler.handle(post(body, None)).await.status, 401);
    assert_eq!(handler.handle(post(body, Some("wrong"))).await.status, 401);
    assert_eq!(
        store.load().len(),
        0,
        "an unauthorized publish changes nothing"
    );
}

#[tokio::test]
async fn a_disabled_endpoint_reports_not_enabled() {
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy()), sink());
    let handler = AppHandler::new(pipeline, ReferenceAuthenticator::dev());
    let resp = handler
        .handle(post(r#"{"directives":[]}"#, Some(TOKEN)))
        .await;
    assert_eq!(resp.status, 404, "no admin channel configured");
}

#[tokio::test]
async fn a_malformed_body_is_rejected_and_changes_nothing() {
    let store = Arc::new(InMemoryDirectiveStore::new());
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy()), sink());
    let handler = admin_handler(store.clone(), pipeline);

    let resp = handler
        .handle(post(
            r#"{"directives":[{"id":"a","ttl_secs":60}]}"#,
            Some(TOKEN),
        ))
        .await;
    assert_eq!(resp.status, 400);
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["error"], "missing_level");
    assert_eq!(store.load().len(), 0);
}

#[tokio::test]
async fn a_published_directive_takes_effect_on_the_live_pipeline() {
    // The whole point: publish through the API, and the running pipeline captures
    // matching requests into the break-glass tape with no restart.
    let store = Arc::new(InMemoryDirectiveStore::new());
    let tape = Arc::new(BreakGlassBuffer::new(8));
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy()), sink())
        .with_directive_store(store.clone())
        .with_break_glass(tape.clone());
    let handler = admin_handler(store, pipeline);

    // Publish a fleet-wide ring_buffer directive.
    let resp = handler
        .handle(post(
            r#"{"directives":[{"id":"bg","level":"Shape","ttl_secs":3600,"ring_buffer":true}]}"#,
            Some(TOKEN),
        ))
        .await;
    assert_eq!(resp.status, 200);
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["published"], 1);

    // A subsequent request is now captured (it fails at resolution, but the
    // ring_buffer directive still tapes it — capture is independent of outcome).
    let ingest = IngressRequest {
        method: HttpMethod::Put,
        path: "/orders/_doc".to_owned(),
        endpoint: EndpointKind::IngestDoc,
        logical_index: "orders".to_owned(),
        doc_id: None,
        headers: vec![],
        body: b"{}".to_vec(),
        query: None,
        client_cert_subject: None,
        secure: true,
    };
    let _ = handler.handle(ingest).await;
    assert_eq!(
        tape.len(),
        1,
        "the published directive captured the request"
    );
}

#[tokio::test]
async fn introspecting_returns_the_settings_the_instance_is_applying() {
    let store = Arc::new(InMemoryDirectiveStore::new());
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy()), sink());
    let handler = admin_handler(store.clone(), pipeline);

    // Publish a targeted directive, then read it back — the agent's observe loop.
    let body = r#"{"directives":[{"id":"raise","level":"ShapeTiming","ttl_secs":60,"tenant":"acme","sample_per_mille":500,"ring_buffer":true}]}"#;
    assert_eq!(handler.handle(post(body, Some(TOKEN))).await.status, 200);

    // Fail-closed: a missing or wrong token reveals nothing.
    assert_eq!(handler.handle(get(None)).await.status, 401);
    assert_eq!(handler.handle(get(Some("wrong"))).await.status, 401);

    // The read describes exactly what this instance is applying.
    let resp = handler.handle(get(Some(TOKEN))).await;
    assert_eq!(resp.status, 200);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let d = &v["directives"][0];
    assert_eq!(d["id"], "raise");
    assert_eq!(d["level"], "ShapeTiming");
    assert_eq!(d["tenant"], "acme");
    assert_eq!(d["sample_per_mille"], 500);
    assert_eq!(d["ring_buffer"], true);
    assert_eq!(d["expired"], false);
}

#[tokio::test]
async fn an_introspected_directive_re_publishes_verbatim() {
    // The observe→act loop closes only if what an agent reads back can be fed
    // straight to POST. Publish an endpoint-targeted directive (the field that
    // previously did not round-trip), read it, and re-publish that exact body.
    let store = Arc::new(InMemoryDirectiveStore::new());
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy()), sink());
    let handler = admin_handler(store.clone(), pipeline);

    let body = r#"{"directives":[{"id":"r","level":"Shape","ttl_secs":60,"endpoint":"Search","sample_per_mille":1000}]}"#;
    assert_eq!(handler.handle(post(body, Some(TOKEN))).await.status, 200);
    let read = handler.handle(get(Some(TOKEN))).await;
    let view: serde_json::Value = serde_json::from_slice(&read.body).unwrap();
    assert_eq!(view["directives"][0]["endpoint"], "Search");

    // Re-publish the *introspected* directive (restoring the relative ttl the read
    // omits): the decoder accepts it — schema parity, no unknown_field rejection.
    let mut directive = view["directives"][0].clone();
    directive["ttl_secs"] = serde_json::json!(60);
    directive.as_object_mut().unwrap().remove("expired");
    let republish = serde_json::json!({ "directives": [directive] }).to_string();
    assert_eq!(
        handler.handle(post(&republish, Some(TOKEN))).await.status,
        200,
        "an introspected directive must re-publish without rejection"
    );
}
