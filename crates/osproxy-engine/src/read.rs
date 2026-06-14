//! The read-path glue for get-by-id (`docs/04` §5).
//!
//! Mirrors [`crate::plan`] on the read side: it turns a resolved routing
//! decision plus the client's **logical** id into the [`ReadOp`] the
//! [`Reader`](osproxy_sink::Reader) fetches, then shapes the upstream document
//! back into the client's logical view — stripping injected tenancy fields,
//! mapping the physical id back to logical, and presenting the logical index.
//! Pure and synchronous; the network hop happens in the pipeline.

use osproxy_core::FieldName;
use osproxy_rewrite::{map_logical_to_physical, strip_fields};
use osproxy_sink::ReadOp;
use osproxy_spi::{BodyTransform, DocIdRule};
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
    let partition = resolved.partition.as_str();
    let shape = read_shape(&resolved.decision.body_transform);

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

    let op = ReadOp::new(resolved.decision.target.clone(), physical_id, routing);
    Ok((op, shape))
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
}
