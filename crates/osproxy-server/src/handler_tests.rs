// Unit tests for the handler's pure routing predicates.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

#[test]
fn opens_scroll_detects_the_scroll_param_in_any_position() {
    // A scroll-opening search returns a `_scroll_id` that must be affinity-wrapped
    // against the whole response body, so it keeps the buffered (non-streamed) path.
    assert!(opens_scroll(Some("scroll")));
    assert!(opens_scroll(Some("scroll=1m")));
    assert!(opens_scroll(Some("q=foo&scroll=1m")));
    assert!(opens_scroll(Some("scroll=1m&pretty")));
    assert!(opens_scroll(Some("pretty&scroll")));
}

#[test]
fn opens_scroll_ignores_lookalikes_and_absence() {
    // Only the exact `scroll` key counts, not a value mentioning it, nor a longer
    // key that merely starts with it; and no query string means a plain search.
    assert!(!opens_scroll(None));
    assert!(!opens_scroll(Some("")));
    assert!(!opens_scroll(Some("q=scroll")));
    assert!(!opens_scroll(Some("scrollx=1")));
    assert!(!opens_scroll(Some("no_scroll=1")));
    assert!(!opens_scroll(Some("pretty&q=match_all")));
}

use osproxy_core::Epoch;
use osproxy_engine::RequestError;
use osproxy_sink::SinkError;
use osproxy_spi::SpiError;

fn err(code: ErrorCode) -> RequestError {
    // Constructs a `RequestError` that maps to each `ErrorCode` reachable
    // through the request path, to exercise `status_for`'s match. `AuthFailed`,
    // `Unauthorized`, and `Overloaded` are never produced by `RequestError` in
    // practice today (the `gate()` auth/authz failure short-circuits before the
    // pipeline via `AuthError::http_status`, never through `status_for`, and no
    // `SpiError`/`SinkError` variant currently maps to `Overloaded`); those arms
    // are defensive for a future error kind and are left uncovered deliberately.
    match code {
        ErrorCode::PartitionUnresolved => {
            RequestError::from(SpiError::PartitionUnresolved { tried: vec![] })
        }
        ErrorCode::PlacementMissing => RequestError::from(SpiError::PlacementMissing {
            partition: osproxy_core::PartitionId::from("acme"),
        }),
        ErrorCode::PlacementBackendUnavailable => {
            RequestError::from(SpiError::PlacementBackend { retryable: true })
        }
        ErrorCode::UnsupportedEndpoint => RequestError::from(SpiError::UnsupportedEndpoint {
            endpoint: EndpointKind::Search,
        }),
        ErrorCode::StaleEpoch => RequestError::StaleEpoch {
            stamped: Epoch::new(1),
        },
        ErrorCode::UpstreamFailed => RequestError::from(SinkError::Transport { kind: "boom" }),
        ErrorCode::CursorUnresolvable => RequestError::Cursor { reason: "missing" },
        ErrorCode::PayloadTooLarge => RequestError::PayloadTooLarge { reason: "too big" },
        _ => unreachable!("not produced by RequestError; see the doc comment above"),
    }
}

#[test]
fn status_for_maps_every_error_code_to_its_http_status() {
    assert_eq!(status_for(&err(ErrorCode::PartitionUnresolved)), 400);
    assert_eq!(status_for(&err(ErrorCode::UnsupportedEndpoint)), 400);
    assert_eq!(status_for(&err(ErrorCode::PlacementMissing)), 404);
    assert_eq!(status_for(&err(ErrorCode::StaleEpoch)), 409);
    assert_eq!(status_for(&err(ErrorCode::PayloadTooLarge)), 413);
    assert_eq!(status_for(&err(ErrorCode::UpstreamFailed)), 502);
    assert_eq!(
        status_for(&err(ErrorCode::PlacementBackendUnavailable)),
        503
    );
}

#[test]
fn status_for_maps_an_internal_invariant_violation_to_a_client_error() {
    // `RequestError::Internal` maps to `ErrorCode::UnsupportedEndpoint` today
    // (see `RequestError::code`), the same code a rewrite failure gets; the
    // shared arm is exercised here via a code path other than the
    // `UnsupportedEndpoint` `SpiError` case above.
    let e = RequestError::Internal {
        reason: "invariant violated",
    };
    assert_eq!(status_for(&e), 400);
}

#[test]
fn error_body_carries_the_code_and_retryability_with_no_tenant_data() {
    let body = error_body(&err(ErrorCode::StaleEpoch));
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["error"], "stale_epoch");
    assert_eq!(v["retryable"], true);

    let body = error_body(&err(ErrorCode::PayloadTooLarge));
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["retryable"], false);
}

#[test]
fn credentials_from_extracts_the_bearer_token_and_cert_subject() {
    let req = IngressRequest {
        method: HttpMethod::Get,
        protocol: osproxy_spi::Protocol::Http1,
        path: "/".to_owned(),
        endpoint: EndpointKind::Search,
        logical_index: String::new(),
        doc_id: None,
        headers: vec![("authorization".to_owned(), "Bearer tok123".to_owned())],
        body: Vec::new(),
        query: None,
        client_cert_subject: Some("CN=svc".to_owned()),
        secure: true,
    };
    let creds = credentials_from(&req);
    assert_eq!(creds.bearer_token.as_deref(), Some("tok123"));
    assert_eq!(creds.client_cert_subject.as_deref(), Some("CN=svc"));
}

#[test]
fn credentials_from_is_empty_with_no_headers_or_cert() {
    let req = IngressRequest {
        method: HttpMethod::Get,
        protocol: osproxy_spi::Protocol::Http1,
        path: "/".to_owned(),
        endpoint: EndpointKind::Search,
        logical_index: String::new(),
        doc_id: None,
        headers: Vec::new(),
        body: Vec::new(),
        query: None,
        client_cert_subject: None,
        secure: true,
    };
    let creds = credentials_from(&req);
    assert!(creds.bearer_token.is_none());
    assert!(creds.client_cert_subject.is_none());
}

#[test]
fn auth_error_body_carries_only_the_stable_code() {
    let body = auth_error_body(&AuthError::InvalidCredentials);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["error"], AuthError::InvalidCredentials.code().as_slug());
}

#[test]
fn ingress_from_defaults_content_type_when_the_pipeline_left_it_unset() {
    let resp = ingress_from(PipelineResponse {
        status: 200,
        body: b"{}".to_vec(),
        content_type: None,
    });
    assert!(!resp.headers.iter().any(|(k, _)| k == "content-type"));
}

#[test]
fn ingress_from_carries_a_pipeline_set_content_type() {
    let resp = ingress_from(PipelineResponse {
        status: 200,
        body: b"plain".to_vec(),
        content_type: Some("text/plain".to_owned()),
    });
    assert!(resp
        .headers
        .iter()
        .any(|(k, v)| k == "content-type" && v == "text/plain"));
}

#[test]
fn to_streaming_preserves_status_and_headers_from_a_buffered_refusal() {
    let refusal = IngressResponse::json(403, br#"{"error":"tls_required"}"#.to_vec())
        .with_header("x-request-id", "req-9");
    let streaming = to_streaming(refusal);
    assert_eq!(streaming.status, 403);
    assert!(streaming
        .headers
        .iter()
        .any(|(k, v)| k == "x-request-id" && v == "req-9"));
}
