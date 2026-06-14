//! Assembling a [`RequestTrace`] into the `/debug/explain` document and a
//! bounded store of recent explanations.
//!
//! The document is purpose-built for LLM consumption (`docs/05` §6): the ordered
//! spans as shape attributes, the final status, and — on failure — the
//! `ErrorContext` with its decision chain and remediation. Because the trace is
//! shape-only by construction, so is this document; it cannot reveal a tenant
//! value because none was ever captured.

use std::collections::VecDeque;
use std::sync::Mutex;

use osproxy_core::{ErrorContext, RequestId};
use serde_json::{json, Value};

use crate::trace::RequestTrace;

/// Builds the explain document for `request_id` from its `trace`.
#[must_use]
pub fn explain_json(request_id: &RequestId, trace: &RequestTrace) -> Value {
    let mut spans = serde_json::Map::new();
    if let Some(i) = &trace.ingress {
        spans.insert(
            "ingress".into(),
            json!({ "protocol": i.protocol, "tls_suite": i.tls_suite, "tls_reused": i.tls_reused }),
        );
    }
    if let Some(c) = &trace.classify {
        spans.insert(
            "classify".into(),
            json!({ "endpoint_kind": format!("{:?}", c.endpoint), "is_write": c.endpoint.is_write(), "index_logical": c.logical_index.as_str() }),
        );
    }
    if let Some(r) = &trace.resolve {
        spans.insert("spi.resolve".into(), resolve_json(r));
    }
    if let Some(r) = &trace.rewrite {
        spans.insert(
            "rewrite".into(),
            json!({ "transform_kind": r.transform_kind, "body_bytes": r.body_bytes }),
        );
    }
    if let Some(d) = &trace.dispatch {
        spans.insert(
            "dispatch".into(),
            json!({ "target_cluster": d.cluster.as_str(), "upstream_status": d.upstream_status, "pool_reuse": d.pool_reuse }),
        );
    }
    if let Some(e) = &trace.egress {
        spans.insert(
            "egress".into(),
            json!({ "status": e.status, "response_bytes": e.response_bytes }),
        );
    }

    json!({
        "request_id": request_id.as_str(),
        "outcome": if trace.failed() { "error" } else { "ok" },
        "spans": Value::Object(spans),
        "error": trace.error.as_ref().map(error_json),
    })
}

/// Serializes the `spi.resolve` span (field names only, never values).
fn resolve_json(r: &crate::trace::ResolveInfo) -> Value {
    let fields: Vec<&str> = r
        .inject_fields
        .iter()
        .map(osproxy_core::FieldName::as_str)
        .collect();
    json!({
        "partition_id": r.partition.as_str(),
        "placement_kind": r.placement_kind,
        "target_cluster": r.cluster.as_str(),
        "target_index": r.index.as_str(),
        "epoch": r.epoch.get(),
        "inject_field_names": fields,
        "routing": r.routing,
    })
}

/// Serializes an [`ErrorContext`] (ids + remediation, never values).
fn error_json(err: &ErrorContext) -> Value {
    let chain = &err.decision_chain;
    json!({
        "code": err.code.as_slug(),
        "retryable": err.retryable,
        "remediation": err.remediation,
        "decision_chain": {
            "principal": chain.principal.as_ref().map(osproxy_core::PrincipalId::as_str),
            "partition": chain.partition.as_ref().map(osproxy_core::PartitionId::as_str),
            "cluster": chain.cluster.as_ref().map(osproxy_core::ClusterId::as_str),
            "index": chain.index.as_ref().map(osproxy_core::IndexName::as_str),
        },
    })
}

/// A bounded, in-memory store of recent request explanations.
///
/// A single-instance affordance backing `/debug/explain/{request_id}` (`docs/05`
/// §5 ring buffer). Oldest entries are evicted past capacity, so memory is
/// bounded regardless of traffic.
#[derive(Debug)]
pub struct ExplainStore {
    capacity: usize,
    entries: Mutex<VecDeque<(RequestId, Value)>>,
}

impl ExplainStore {
    /// Creates a store holding at most `capacity` recent explanations.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: Mutex::new(VecDeque::new()),
        }
    }

    /// Records the explanation for `request_id`, evicting the oldest if full.
    pub fn record(&self, request_id: RequestId, trace: &RequestTrace) {
        let doc = explain_json(&request_id, trace);
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back((request_id, doc));
    }

    /// Looks up the explanation for `request_id`, if still retained.
    #[must_use]
    pub fn get(&self, request_id: &RequestId) -> Option<Value> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|(id, _)| id == request_id)
            .map(|(_, doc)| doc.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{ClassifyInfo, DispatchInfo, EgressInfo, ResolveInfo};
    use osproxy_core::error::DecisionChain;
    use osproxy_core::{
        ClusterId, EndpointKind, Epoch, ErrorCode, FieldName, IndexName, PartitionId,
    };

    fn full_trace() -> RequestTrace {
        let mut t = RequestTrace::new();
        t.record_classify(ClassifyInfo {
            endpoint: EndpointKind::IngestDoc,
            logical_index: IndexName::from("orders"),
        });
        t.record_resolve(ResolveInfo {
            partition: PartitionId::from("acme"),
            placement_kind: "shared_index",
            cluster: ClusterId::from("eu-1"),
            index: IndexName::from("orders-shared"),
            epoch: Epoch::new(3),
            inject_fields: vec![FieldName::from("_tenant")],
            routing: true,
        });
        t.record_dispatch(DispatchInfo {
            cluster: ClusterId::from("eu-1"),
            upstream_status: 201,
            pool_reuse: true,
        });
        t.record_egress(EgressInfo {
            status: 201,
            response_bytes: 42,
        });
        t
    }

    #[test]
    fn explain_document_carries_ids_and_shapes() {
        let rid = RequestId::from("req-9");
        let doc = explain_json(&rid, &full_trace());
        assert_eq!(doc["request_id"], "req-9");
        assert_eq!(doc["outcome"], "ok");
        assert_eq!(doc["spans"]["spi.resolve"]["partition_id"], "acme");
        assert_eq!(doc["spans"]["spi.resolve"]["epoch"], 3);
        assert_eq!(
            doc["spans"]["spi.resolve"]["inject_field_names"][0],
            "_tenant"
        );
        assert_eq!(doc["spans"]["dispatch"]["upstream_status"], 201);
        assert!(doc["error"].is_null());
    }

    #[test]
    fn failure_attaches_error_context() {
        let rid = RequestId::from("req-err");
        let mut t = RequestTrace::new();
        let ctx = ErrorContext::new(
            ErrorCode::PlacementMissing,
            false,
            "register a placement for the partition",
        )
        .with_chain(DecisionChain {
            partition: Some(PartitionId::from("ghost")),
            ..DecisionChain::new()
        });
        t.record_error(ctx);
        let doc = explain_json(&rid, &t);
        assert_eq!(doc["outcome"], "error");
        assert_eq!(doc["error"]["code"], "placement_missing");
        assert_eq!(doc["error"]["decision_chain"]["partition"], "ghost");
        assert_eq!(doc["error"]["retryable"], false);
    }

    #[test]
    fn store_retains_recent_and_evicts_oldest() {
        let store = ExplainStore::new(2);
        store.record(RequestId::from("a"), &full_trace());
        store.record(RequestId::from("b"), &full_trace());
        store.record(RequestId::from("c"), &full_trace());
        assert!(store.get(&RequestId::from("a")).is_none(), "a evicted");
        assert!(store.get(&RequestId::from("b")).is_some());
        assert!(store.get(&RequestId::from("c")).is_some());
    }
}
