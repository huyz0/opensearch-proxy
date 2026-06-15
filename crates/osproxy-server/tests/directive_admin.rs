//! The `POST /admin/directives` channel: token-gated, fail-closed, and — once a
//! set is published — live on the same pipeline the requests flow through (the
//! fleet flip with no restart).

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
        client_cert_subject: None,
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
        client_cert_subject: None,
    };
    let _ = handler.handle(ingest).await;
    assert_eq!(
        tape.len(),
        1,
        "the published directive captured the request"
    );
}
