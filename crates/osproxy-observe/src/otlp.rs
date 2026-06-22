//! Encoding a [`RequestTrace`] as an OTLP/HTTP **JSON** `ResourceSpans` payload
//! (`docs/specs/observability-otel.md`).
//!
//! This is the wire encoding only, pure and I/O-free, so it is exhaustively
//! testable without a collector. One span per request represents the proxy's hop;
//! its id is the W3C `span_id` the proxy already presents to downstream calls, so
//! the upstream's spans nest under it. The span's attributes are the same
//! **shape-only** stage data as `/debug/explain` (ids, names, sizes, codes,
//! never a tenant value), keyed under the `osproxy.*` / standard `OTel`
//! namespaces.
//!
//! OTLP/JSON specifics: `trace_id`/`span_id` are hex strings, 64-bit ints and
//! nanosecond timestamps are rendered as strings, and span kind `2` is `SERVER`.

use osproxy_core::{RequestId, TraceContext};
use serde_json::{json, Value};

use crate::trace::RequestTrace;

/// OTLP span kind `SERVER` (the proxy handling a client request).
const SPAN_KIND_SERVER: i32 = 2;
/// OTLP status code `OK`.
const STATUS_OK: i32 = 1;
/// OTLP status code `ERROR`.
const STATUS_ERROR: i32 = 2;

/// Builds the OTLP/HTTP JSON `ResourceSpans` envelope for one completed request,
/// or `None` if the request never recorded a trace context (no ids to emit).
///
/// `service_name` identifies this proxy instance/service to the collector;
/// `start_unix_nano`/`end_unix_nano` bound the request's wall-clock span.
#[must_use]
pub fn resource_spans(
    service_name: &str,
    request_id: &RequestId,
    trace: &RequestTrace,
    start_unix_nano: u64,
    end_unix_nano: u64,
) -> Option<Value> {
    let context = trace.context()?;
    let span = span_json(request_id, trace, context, start_unix_nano, end_unix_nano);
    Some(json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [attr_str("service.name", service_name)],
            },
            "scopeSpans": [{
                "scope": { "name": "osproxy" },
                "spans": [span],
            }],
        }],
    }))
}

/// The single proxy span for the request.
fn span_json(
    request_id: &RequestId,
    trace: &RequestTrace,
    context: &TraceContext,
    start_unix_nano: u64,
    end_unix_nano: u64,
) -> Value {
    let status = if let Some(err) = &trace.error {
        json!({ "code": STATUS_ERROR, "message": err.code.as_slug() })
    } else {
        json!({ "code": STATUS_OK })
    };
    let mut span = json!({
        "traceId": context.trace_id_hex(),
        "spanId": context.span_id_hex(),
        "name": span_name(trace),
        "kind": SPAN_KIND_SERVER,
        "startTimeUnixNano": start_unix_nano.to_string(),
        "endTimeUnixNano": end_unix_nano.to_string(),
        "attributes": attributes(request_id, trace),
        "status": status,
    });
    // Nest under the caller's span when this request continued an incoming trace,
    // so the proxy's span is a child of the client's, not a detached root.
    if let Some(parent) = context.parent_span_id_hex() {
        span["parentSpanId"] = json!(parent);
    }
    span
}

/// The span name: the request's endpoint classification, or `request` before it
/// is classified.
fn span_name(trace: &RequestTrace) -> String {
    trace
        .classify
        .as_ref()
        .map_or_else(|| "request".to_owned(), |c| format!("{:?}", c.endpoint))
}

/// The shape-only span attributes, assembled from the recorded stage spans. Every
/// value is an id, name, size, count, or stable code, never request data.
fn attributes(request_id: &RequestId, trace: &RequestTrace) -> Vec<Value> {
    let mut a = vec![attr_str("osproxy.request.id", request_id.as_str())];
    if let Some(i) = &trace.ingress {
        a.push(attr_str("osproxy.protocol", i.protocol));
        if let Some(reused) = i.tls_reused {
            a.push(attr_bool("tls.session_reused", reused));
        }
    }
    if let Some(c) = &trace.classify {
        a.push(attr_str("osproxy.endpoint", &format!("{:?}", c.endpoint)));
        a.push(attr_bool("osproxy.request.is_write", c.endpoint.is_write()));
    }
    if let Some(r) = &trace.resolve {
        let names: Vec<&str> = r
            .inject_fields
            .iter()
            .map(osproxy_core::FieldName::as_str)
            .collect();
        a.push(attr_str("osproxy.partition.id", r.partition.as_str()));
        a.push(attr_str("osproxy.placement.kind", r.placement_kind));
        a.push(attr_str("osproxy.target.cluster", r.cluster.as_str()));
        a.push(attr_str("osproxy.target.index", r.index.as_str()));
        a.push(attr_int("osproxy.epoch", r.epoch.get()));
        a.push(attr_strs("osproxy.inject.field_names", &names));
        a.push(attr_bool("osproxy.routing", r.routing));
        a.push(attr_str("osproxy.migration.phase", r.migration));
    }
    if let Some(r) = &trace.rewrite {
        a.push(attr_str("osproxy.rewrite.kind", r.transform_kind));
        a.push(attr_int("osproxy.rewrite.body_bytes", r.body_bytes as u64));
    }
    if let Some(d) = &trace.dispatch {
        a.push(attr_int(
            "osproxy.upstream.status",
            u64::from(d.upstream_status),
        ));
        a.push(attr_bool("osproxy.pool.reuse", d.pool_reuse));
    }
    if let Some(e) = &trace.egress {
        a.push(attr_int("http.response.status_code", u64::from(e.status)));
        a.push(attr_int("osproxy.response.bytes", e.response_bytes as u64));
    }
    if let Some(err) = &trace.error {
        a.push(attr_str("osproxy.error.code", err.code.as_slug()));
        a.push(attr_bool("osproxy.error.retryable", err.retryable));
    }
    a
}

/// A string-valued OTLP attribute.
fn attr_str(key: &str, value: &str) -> Value {
    json!({ "key": key, "value": { "stringValue": value } })
}

/// An int64 OTLP attribute (rendered as a string per OTLP/JSON).
fn attr_int(key: &str, value: u64) -> Value {
    json!({ "key": key, "value": { "intValue": value.to_string() } })
}

/// A bool OTLP attribute.
fn attr_bool(key: &str, value: bool) -> Value {
    json!({ "key": key, "value": { "boolValue": value } })
}

/// A string-array OTLP attribute.
fn attr_strs(key: &str, values: &[&str]) -> Value {
    let items: Vec<Value> = values.iter().map(|v| json!({ "stringValue": v })).collect();
    json!({ "key": key, "value": { "arrayValue": { "values": items } } })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{ClassifyInfo, ResolveInfo};
    use osproxy_core::{ClusterId, EndpointKind, Epoch, FieldName, IndexName, PartitionId};

    fn traced() -> RequestTrace {
        let mut t = RequestTrace::new();
        t.record_context(TraceContext::propagate(
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            None,
            &RequestId::from("req-1"),
        ));
        t.record_classify(ClassifyInfo {
            endpoint: EndpointKind::IngestDoc,
            logical_index: IndexName::from("orders"),
        });
        t.record_resolve(ResolveInfo {
            partition: PartitionId::from("acme"),
            placement_kind: "shared_index",
            cluster: ClusterId::from("eu-1"),
            index: IndexName::from("orders-shared"),
            epoch: Epoch::new(7),
            inject_fields: vec![FieldName::from("_tenant")],
            routing: true,
            migration: "settled",
        });
        t
    }

    #[test]
    fn encodes_a_resource_span_with_the_proxy_trace_and_span_ids() {
        let trace = traced();
        let doc =
            resource_spans("osproxy", &RequestId::from("req-1"), &trace, 1_000, 2_000).unwrap();
        let span = &doc["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        // The emitted span carries the W3C ids: trace continues the caller's, the
        // span id is the proxy's (what downstream calls were told their parent is).
        assert_eq!(span["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(
            span["spanId"],
            trace.context().unwrap().span_id_hex(),
            "emitted span id must equal the id propagated downstream"
        );
        assert_eq!(span["kind"], SPAN_KIND_SERVER);
        // OTLP/JSON renders timestamps as strings.
        assert_eq!(span["startTimeUnixNano"], "1000");
        assert_eq!(span["endTimeUnixNano"], "2000");
        assert_eq!(
            doc["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"],
            "osproxy"
        );
    }

    #[test]
    fn a_continued_trace_nests_the_span_under_the_callers_parent() {
        // `traced()` propagates from an incoming traceparent whose span is
        // 00f067aa0ba902b7, that must surface as the proxy span's parent.
        let doc = resource_spans("svc", &RequestId::from("req-1"), &traced(), 0, 1).unwrap();
        let span = &doc["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["parentSpanId"], "00f067aa0ba902b7");
    }

    #[test]
    fn a_root_request_emits_no_parent_span_id() {
        // No incoming traceparent: the proxy span is the root, so no parentSpanId.
        let mut t = RequestTrace::new();
        t.record_context(TraceContext::propagate(
            None,
            None,
            &RequestId::from("req-1"),
        ));
        let doc = resource_spans("svc", &RequestId::from("req-1"), &t, 0, 1).unwrap();
        let span = &doc["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert!(
            span.get("parentSpanId").is_none(),
            "root span has no parent"
        );
    }

    #[test]
    fn attributes_are_shape_only_ids_and_names_never_values() {
        let doc = resource_spans("svc", &RequestId::from("req-1"), &traced(), 0, 1).unwrap();
        let attrs = &doc["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"];
        let text = serde_json::to_string(attrs).unwrap();
        // Field NAME present; a document value never would be.
        assert!(text.contains("osproxy.inject.field_names"));
        assert!(text.contains("_tenant"));
        assert!(text.contains("\"osproxy.partition.id\""));
        // int64 attributes are strings in OTLP/JSON.
        assert!(
            text.contains(r#"{"intValue":"7"}"#),
            "epoch as string int: {text}"
        );
    }

    #[test]
    fn a_failed_request_maps_to_otlp_error_status() {
        let mut trace = traced();
        trace.record_error(osproxy_core::ErrorContext::new(
            osproxy_core::ErrorCode::StaleEpoch,
            true,
            "retry the request",
        ));
        let doc = resource_spans("svc", &RequestId::from("req-1"), &trace, 0, 1).unwrap();
        let status = &doc["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["status"];
        assert_eq!(status["code"], STATUS_ERROR);
        assert_eq!(status["message"], "stale_epoch");
    }

    #[test]
    fn no_trace_context_means_nothing_to_export() {
        let trace = RequestTrace::new();
        assert!(resource_spans("svc", &RequestId::from("r"), &trace, 0, 1).is_none());
    }
}
