//! The `/debug/breakglass` admin endpoint returns the captured forensic tape as
//! a JSON array (oldest first), without touching auth, routing, or the upstream —
//! it is a read of in-process telemetry, the operator-facing counterpart of the
//! `ring_buffer` capture.

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use osproxy_core::{ClusterId, EndpointKind, IndexName};
use osproxy_engine::Pipeline;
use osproxy_observe::BreakGlassBuffer;
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_spi::HttpMethod;
use osproxy_tenancy::TenancyRouter;
use osproxy_transport::{IngressHandler, IngressRequest};
use serde_json::{json, Value};

fn get(path: &str) -> IngressRequest {
    IngressRequest {
        method: HttpMethod::Get,
        path: path.to_owned(),
        endpoint: EndpointKind::Unknown,
        logical_index: String::new(),
        doc_id: None,
        headers: vec![],
        body: vec![],
        client_cert_subject: None,
    }
}

#[tokio::test]
async fn the_breakglass_endpoint_returns_the_captured_tape() {
    let tape = Arc::new(BreakGlassBuffer::new(8));
    // Two captures, as a ring_buffer directive would have produced.
    tape.capture(json!({"request_id": "r1", "outcome": "error"}));
    tape.capture(json!({"request_id": "r2", "outcome": "ok"}));

    let endpoints: HashMap<ClusterId, String> = HashMap::new();
    let sink = OpenSearchSink::new(endpoints);
    let tenancy = ReferenceTenancy::new(ClusterId::from("c"), IndexName::from("shared"));
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy), sink).with_break_glass(tape.clone());
    let handler = AppHandler::new(pipeline, ReferenceAuthenticator::dev());

    let resp = handler.handle(get("/debug/breakglass")).await;
    assert_eq!(resp.status, 200);
    let body: Value = serde_json::from_slice(&resp.body).unwrap();
    let entries = body.as_array().expect("the tape is a JSON array");
    assert_eq!(entries.len(), 2, "both captures are returned, oldest first");
    assert_eq!(entries[0]["request_id"], "r1");
    assert_eq!(entries[1]["request_id"], "r2");
}

#[tokio::test]
async fn an_empty_tape_is_an_empty_array() {
    let endpoints: HashMap<ClusterId, String> = HashMap::new();
    let sink = OpenSearchSink::new(endpoints);
    let tenancy = ReferenceTenancy::new(ClusterId::from("c"), IndexName::from("shared"));
    let handler = AppHandler::new(
        Pipeline::new(TenancyRouter::new(tenancy), sink),
        ReferenceAuthenticator::dev(),
    );

    let resp = handler.handle(get("/debug/breakglass")).await;
    assert_eq!(resp.status, 200);
    let body: Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body, json!([]), "no captures → empty array, never an error");
}
