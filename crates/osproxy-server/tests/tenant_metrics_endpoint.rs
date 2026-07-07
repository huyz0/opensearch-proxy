//! `GET /debug/metrics/tenants`: opt-in, gated the same as the other
//! `/debug/*` diagnostics surfaces, unlike the always-on `/metrics`.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use osproxy_core::{ClusterId, EndpointKind, IndexName};
use osproxy_engine::Pipeline;
use osproxy_observe::TenantMetrics;
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_spi::HttpMethod;
use osproxy_tenancy::TenancyRouter;
use osproxy_transport::{IngressHandler, IngressRequest};

fn get(path: &str) -> IngressRequest {
    IngressRequest {
        method: HttpMethod::Get,
        protocol: osproxy_spi::Protocol::Http1,
        path: path.to_owned(),
        endpoint: EndpointKind::Unknown,
        logical_index: String::new(),
        doc_id: None,
        headers: vec![],
        body: vec![],
        query: None,
        client_cert_subject: None,
        secure: false,
    }
}

fn handler_pipeline() -> Pipeline<TenancyRouter<ReferenceTenancy>, OpenSearchSink> {
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(
        ClusterId::from("c"),
        IndexName::from("shared"),
        "http://unused",
    );
    Pipeline::new(TenancyRouter::new(tenancy), sink)
}

#[tokio::test]
async fn refuses_not_enabled_when_no_tenant_metrics_configured() {
    let handler = AppHandler::new(handler_pipeline(), ReferenceAuthenticator::dev());
    let resp = handler.handle(get("/debug/metrics/tenants")).await;
    assert_eq!(resp.status, 404);
    assert!(String::from_utf8_lossy(&resp.body).contains("not_enabled"));
}

#[tokio::test]
async fn serves_prometheus_text_once_enabled() {
    let handler = AppHandler::new(handler_pipeline(), ReferenceAuthenticator::dev())
        .with_tenant_metrics(Arc::new(TenantMetrics::new()));
    let resp = handler.handle(get("/debug/metrics/tenants")).await;
    assert_eq!(resp.status, 200);
    let content_type = resp
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.as_str());
    assert_eq!(content_type, Some("text/plain; version=0.0.4"));
    let body = String::from_utf8_lossy(&resp.body);
    assert!(body.contains("# HELP osproxy_tenant_requests_total"));
    assert!(body.contains("# TYPE osproxy_tenant_requests_total counter"));
}

#[tokio::test]
async fn is_also_refused_when_debug_endpoints_are_disabled() {
    let handler = AppHandler::new(handler_pipeline(), ReferenceAuthenticator::dev())
        .with_tenant_metrics(Arc::new(TenantMetrics::new()))
        .with_debug_endpoints(false);
    let resp = handler.handle(get("/debug/metrics/tenants")).await;
    assert_eq!(resp.status, 404);
    assert!(String::from_utf8_lossy(&resp.body).contains("not_enabled"));
}
