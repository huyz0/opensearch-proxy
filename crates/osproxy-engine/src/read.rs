//! The read-path glue for get-by-id, delete-by-id, and search (`docs/04` §4–5).
//!
//! Mirrors [`crate::plan`] on the read side: it turns a resolved routing decision
//! plus the client's request into the op the reader/sink runs, then shapes the
//! upstream response back into the client's logical view (strip injected fields,
//! map physical ids back to logical, present the logical index). Pure and
//! synchronous; the network hop happens in the pipeline.

use osproxy_core::FieldName;
use osproxy_rewrite::{map_logical_to_physical, map_physical_to_logical, strip_fields, wrap_query};
use osproxy_sink::{DocOp, ReadOp, SearchOp, WriteOp};
use osproxy_spi::{BodyTransform, DocIdRule, InjectedField, InjectedValue};
use osproxy_tenancy::Resolved;
use serde_json::Value;

use crate::error::RequestError;

/// What the read path needs from a resolved decision beyond the target: the
/// injected field names to strip from a hit, and the id rule (if any) to map
/// the logical id to the physical id and back.
pub(crate) struct ReadShape {
    /// Names of injected tenancy fields to strip from `_source` on a hit.
    pub inject_names: Vec<FieldName>,
    /// The id rule, present when the placement constructs physical ids.
    pub id_rule: Option<DocIdRule>,
}

/// Builds the [`ReadOp`] for a resolved get-by-id request, returning it with the
/// [`ReadShape`] needed to reshape the response.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] if the id rule cannot map the logical id to
/// a physical id (an irreversible template).
pub(crate) fn build_read_op(
    resolved: &Resolved,
    logical_id: &str,
) -> Result<(ReadOp, ReadShape), RequestError> {
    let shape = read_shape(&resolved.decision.body_transform);
    let (physical_id, routing) = physical_id_and_routing(resolved, logical_id, &shape)?;
    let op = ReadOp::new(resolved.decision.target.clone(), physical_id, routing)
        .with_protocol(resolved.decision.upstream_protocol);
    Ok((op, shape))
}

/// Builds the delete [`WriteOp`] for a resolved delete-by-id request, mapping the
/// client's logical id to the physical id (and setting `_routing`), epoch-stamped
/// like any write (`docs/04` §5, `docs/06` §2).
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] if the id rule cannot map the logical id to
/// a physical id (an irreversible template).
pub(crate) fn build_delete_op(
    resolved: &Resolved,
    logical_id: &str,
) -> Result<WriteOp, RequestError> {
    let shape = read_shape(&resolved.decision.body_transform);
    let (physical_id, routing) = physical_id_and_routing(resolved, logical_id, &shape)?;
    Ok(WriteOp::new(
        resolved.decision.target.clone(),
        DocOp::Delete {
            id: physical_id,
            routing,
        },
        resolved.decision.epoch,
    )
    .with_protocol(resolved.decision.upstream_protocol))
}

/// Maps a logical id to `(physical_id, routing)` for a by-id request: applies the
/// id rule when present (else the client id is already physical), and sets
/// routing to the partition when the rule asks for it.
fn physical_id_and_routing(
    resolved: &Resolved,
    logical_id: &str,
    shape: &ReadShape,
) -> Result<(String, Option<String>), RequestError> {
    let partition = resolved.partition.as_str();
    let physical_id = match &shape.id_rule {
        Some(rule) => map_logical_to_physical(rule.template.as_str(), partition, logical_id)?,
        // No id rule (e.g. a dedicated index): the client id is the physical id.
        None => logical_id.to_owned(),
    };
    let routing = shape
        .id_rule
        .as_ref()
        .filter(|r| r.set_routing)
        .map(|_| partition.to_owned());
    Ok((physical_id, routing))
}

/// Shapes a found upstream document into the client's logical view: presents the
/// logical index and id, drops `_routing`, and strips injected tenancy fields
/// from `_source` (the read-path inverse of the write-path inject, `docs/03`).
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] if the upstream body is not valid JSON, or
/// [`RequestError::Internal`] if re-serialization fails.
pub(crate) fn shape_found(
    upstream_body: &[u8],
    logical_index: &str,
    logical_id: &str,
    inject_names: &[FieldName],
) -> Result<Vec<u8>, RequestError> {
    let mut doc: Value = serde_json::from_slice(upstream_body)
        .map_err(|_| osproxy_rewrite::RewriteError::InvalidJson)?;
    if let Some(obj) = doc.as_object_mut() {
        obj.insert("_index".to_owned(), Value::String(logical_index.to_owned()));
        obj.insert("_id".to_owned(), Value::String(logical_id.to_owned()));
        obj.remove("_routing");
        if let Some(source) = obj.get_mut("_source") {
            strip_fields(source, inject_names);
        }
    }
    serde_json::to_vec(&doc).map_err(|_| RequestError::Internal {
        reason: "serializing read response",
    })
}

/// The OpenSearch-shaped delete response in the client's logical terms: the
/// logical index and id, and a `result` of `deleted` (or `not_found` on a 404).
#[must_use]
pub(crate) fn shape_delete(logical_index: &str, logical_id: &str, status: u16) -> Vec<u8> {
    // 404 → "not_found", any success → "deleted".
    let result = ["deleted", "not_found"][usize::from(status == 404)];
    let doc = serde_json::json!({
        "_index": logical_index,
        "_id": logical_id,
        "result": result,
    });
    serde_json::to_vec(&doc).unwrap_or_else(|_| b"{}".to_vec())
}

/// The OpenSearch-shaped body for a document that does not exist, in the
/// client's logical terms.
#[must_use]
pub(crate) fn not_found_body(logical_index: &str, logical_id: &str) -> Vec<u8> {
    let doc = serde_json::json!({
        "_index": logical_index,
        "_id": logical_id,
        "found": false,
    });
    serde_json::to_vec(&doc).unwrap_or_else(|_| b"{\"found\":false}".to_vec())
}

/// Builds the [`SearchOp`] for a resolved search request: wraps the client query
/// in the mandatory partition filter (`docs/03` §5) and returns it with the
/// [`ReadShape`] needed to strip the hits.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] if the client search body is not a JSON
/// object (or is invalid JSON).
pub(crate) fn build_search_op(
    resolved: &Resolved,
    body: &[u8],
) -> Result<(SearchOp, ReadShape), RequestError> {
    let partition = resolved.partition.as_str();
    let shape = read_shape(&resolved.decision.body_transform);
    let filter = filter_terms(&resolved.decision.body_transform, partition);
    let wrapped = wrap_query(body, &filter)?;
    let op = SearchOp::new(resolved.decision.target.clone(), wrapped)
        .with_protocol(resolved.decision.upstream_protocol);
    Ok((op, shape))
}

/// Shapes a search hits envelope into the client's logical view: every hit's
/// `_source` is stripped of injected tenancy fields, its `_index` reset to the
/// logical index, its `_routing` dropped, and its `_id` mapped back to logical.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] if the upstream body is not valid JSON, or
/// [`RequestError::Internal`] if re-serialization fails.
pub(crate) fn shape_hits(
    upstream_body: &[u8],
    logical_index: &str,
    partition: &str,
    shape: &ReadShape,
) -> Result<Vec<u8>, RequestError> {
    let mut doc: Value = serde_json::from_slice(upstream_body)
        .map_err(|_| osproxy_rewrite::RewriteError::InvalidJson)?;
    if let Some(hits) = doc
        .get_mut("hits")
        .and_then(|h| h.get_mut("hits"))
        .and_then(Value::as_array_mut)
    {
        for hit in hits.iter_mut() {
            shape_hit(hit, logical_index, partition, shape);
        }
    }
    serde_json::to_vec(&doc).map_err(|_| RequestError::Internal {
        reason: "serializing search response",
    })
}

/// Strips one search hit in place into the client's logical view.
fn shape_hit(hit: &mut Value, logical_index: &str, partition: &str, shape: &ReadShape) {
    let Some(obj) = hit.as_object_mut() else {
        return;
    };
    obj.insert("_index".to_owned(), Value::String(logical_index.to_owned()));
    obj.remove("_routing");
    if let Some(rule) = &shape.id_rule {
        if let Some(Value::String(physical)) = obj.get("_id") {
            if let Ok(Some(logical)) =
                map_physical_to_logical(rule.template.as_str(), partition, physical)
            {
                obj.insert("_id".to_owned(), Value::String(logical));
            }
        }
    }
    if let Some(source) = obj.get_mut("_source") {
        strip_fields(source, &shape.inject_names);
    }
}

/// The partition filter terms `(field, value)` for the wrapped query: each
/// injected field with its resolved value, so a search can only match documents
/// carrying this partition's injected fields.
fn filter_terms(transform: &BodyTransform, partition: &str) -> Vec<(FieldName, Value)> {
    let fields = match transform {
        BodyTransform::Inject(fields) | BodyTransform::Both { inject: fields, .. } => {
            fields.as_slice()
        }
        BodyTransform::None | BodyTransform::ConstructId(_) => &[],
    };
    fields
        .iter()
        .map(|field| (field.name.clone(), injected_value(field, partition)))
        .collect()
}

/// The concrete value of an injected field (the adapter resolves these to
/// constants; `PartitionId`/`FromPrincipal` fall back to the partition here for
/// robustness — filtering on the partition is always isolating, never a leak).
fn injected_value(field: &InjectedField, partition: &str) -> Value {
    match &field.value {
        InjectedValue::Constant(v) => v.clone(),
        InjectedValue::PartitionId | InjectedValue::FromPrincipal(_) => {
            Value::String(partition.to_owned())
        }
    }
}

/// Extracts the read shape (injected field names + id rule) from the body
/// transform the routing decision carries.
fn read_shape(transform: &BodyTransform) -> ReadShape {
    match transform {
        BodyTransform::None => ReadShape {
            inject_names: Vec::new(),
            id_rule: None,
        },
        BodyTransform::Inject(fields) => ReadShape {
            inject_names: field_names(fields),
            id_rule: None,
        },
        BodyTransform::ConstructId(rule) => ReadShape {
            inject_names: Vec::new(),
            id_rule: Some(rule.clone()),
        },
        BodyTransform::Both { inject, id } => ReadShape {
            inject_names: field_names(inject),
            id_rule: Some(id.clone()),
        },
    }
}

/// The names of injected fields (never their values).
fn field_names(fields: &[osproxy_spi::InjectedField]) -> Vec<FieldName> {
    fields.iter().map(|f| f.name.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{ClusterId, Epoch, IndexName, PartitionId, Target};
    use osproxy_spi::{IdTemplate, InjectedField, InjectedValue, Protocol, RouteDecision};
    use serde_json::json;

    fn resolved(transform: BodyTransform) -> Resolved {
        Resolved {
            partition: PartitionId::from("acme"),
            decision: RouteDecision {
                target: Target::new(ClusterId::from("eu-1"), IndexName::from("shared")),
                upstream_protocol: Protocol::Http1,
                header_ops: Vec::new(),
                body_transform: transform,
                epoch: Epoch::new(4),
            },
        }
    }

    fn shared_transform() -> BodyTransform {
        BodyTransform::Both {
            inject: vec![InjectedField::new(
                FieldName::from("_tenant"),
                InjectedValue::Constant(json!("acme")),
            )],
            id: DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true),
        }
    }

    #[test]
    fn read_op_maps_logical_id_and_sets_routing() {
        let (op, shape) = build_read_op(&resolved(shared_transform()), "7").unwrap();
        assert_eq!(op.id, "acme:7");
        assert_eq!(op.routing.as_deref(), Some("acme"));
        assert_eq!(op.target.index.as_str(), "shared");
        assert_eq!(shape.inject_names, vec![FieldName::from("_tenant")]);
    }

    #[test]
    fn read_op_without_id_rule_uses_client_id() {
        let (op, _) = build_read_op(&resolved(BodyTransform::None), "raw-id").unwrap();
        assert_eq!(op.id, "raw-id");
        assert!(op.routing.is_none());
    }

    #[test]
    fn found_response_is_the_logical_document() {
        let upstream = br#"{
            "_index": "shared",
            "_id": "acme:7",
            "_routing": "acme",
            "found": true,
            "_source": { "_tenant": "acme", "msg": "hi" }
        }"#;
        let body = shape_found(upstream, "orders", "7", &[FieldName::from("_tenant")]).unwrap();
        let doc: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(doc["_index"], "orders");
        assert_eq!(doc["_id"], "7");
        assert!(doc.get("_routing").is_none());
        assert!(doc["_source"].get("_tenant").is_none());
        assert_eq!(doc["_source"]["msg"], "hi");
    }

    #[test]
    fn not_found_body_is_logical() {
        let doc: Value = serde_json::from_slice(&not_found_body("orders", "7")).unwrap();
        assert_eq!(doc["_index"], "orders");
        assert_eq!(doc["_id"], "7");
        assert_eq!(doc["found"], false);
    }

    #[test]
    fn delete_op_maps_logical_id_and_sets_routing() {
        let op = build_delete_op(&resolved(shared_transform()), "7").unwrap();
        assert_eq!(op.epoch, Epoch::new(4));
        let DocOp::Delete { id, routing } = &op.doc else {
            unreachable!("delete-by-id produces a Delete op")
        };
        assert_eq!(id, "acme:7");
        assert_eq!(routing.as_deref(), Some("acme"));
    }

    #[test]
    fn delete_response_reports_logical_terms() {
        let ok: Value = serde_json::from_slice(&shape_delete("orders", "7", 200)).unwrap();
        assert_eq!(ok["_index"], "orders");
        assert_eq!(ok["_id"], "7");
        assert_eq!(ok["result"], "deleted");
        let miss: Value = serde_json::from_slice(&shape_delete("orders", "7", 404)).unwrap();
        assert_eq!(miss["result"], "not_found");
    }

    #[test]
    fn search_op_wraps_client_query_in_the_partition_filter() {
        let (op, _) = build_search_op(
            &resolved(shared_transform()),
            br#"{"query":{"match_all":{}}}"#,
        )
        .unwrap();
        let q: Value = serde_json::from_slice(&op.body).unwrap();
        assert_eq!(q["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
        assert_eq!(q["query"]["bool"]["must"][0]["match_all"], json!({}));
    }

    #[test]
    fn hits_are_stripped_to_the_logical_view() {
        let upstream = br#"{
            "hits": { "total": { "value": 1 }, "hits": [
                { "_index": "shared", "_id": "acme:7", "_routing": "acme",
                  "_source": { "_tenant": "acme", "msg": "hi" } }
            ] }
        }"#;
        let shape = read_shape(&shared_transform());
        let body = shape_hits(upstream, "orders", "acme", &shape).unwrap();
        let doc: Value = serde_json::from_slice(&body).unwrap();
        let hit = &doc["hits"]["hits"][0];
        assert_eq!(hit["_index"], "orders");
        assert_eq!(hit["_id"], "7");
        assert!(hit.get("_routing").is_none());
        assert!(hit["_source"].get("_tenant").is_none());
        assert_eq!(hit["_source"]["msg"], "hi");
    }
}
