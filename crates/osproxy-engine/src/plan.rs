//! Turning a resolved routing decision into a concrete [`WriteBatch`].
//!
//! This is the write-path glue: it takes the [`Resolved`] decision from the
//! tenancy router and the original request body, applies the body transform
//! (inject tenancy fields, construct the `_id`, set `_routing`), and produces
//! the epoch-stamped batch the [`Sink`](osproxy_sink::Sink) will deliver
//! (`docs/04`). Pure and synchronous — no network, fully testable.

use osproxy_core::FieldName;
use osproxy_rewrite::{construct_id, inject_fields, RewriteError};
use osproxy_sink::{DocOp, WriteBatch, WriteOp};
use osproxy_spi::{BodyTransform, DocIdRule, InjectedField, InjectedValue};
use osproxy_tenancy::Resolved;
use serde_json::Value;

use crate::error::RequestError;

/// Builds the single-document write batch for a resolved ingest request.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] if the body is not a JSON object, a
/// reserved field collides, or an id template fails to expand;
/// [`RequestError::Internal`] if a decision carries an unresolved injected value
/// (the tenancy adapter resolves these, so this indicates a bug).
pub fn build_write_batch(resolved: &Resolved, body: &[u8]) -> Result<WriteBatch, RequestError> {
    let decision = &resolved.decision;
    let partition = resolved.partition.as_str();

    let mut doc: Value = serde_json::from_slice(body).map_err(|_| RewriteError::InvalidJson)?;
    let id = apply_transform(&mut doc, &decision.body_transform, partition)?;
    let routing = routing_for(&decision.body_transform, partition);

    // Serializing a `Value` back to bytes is infallible for in-memory values.
    let body = serde_json::to_vec(&doc).map_err(|_| RequestError::Internal {
        reason: "serializing transformed document",
    })?;

    let op = WriteOp::new(
        decision.target.clone(),
        DocOp::Index { id, routing, body },
        decision.epoch,
    );
    Ok(WriteBatch::single(op))
}

/// Applies the body transform in place, returning the constructed `_id` (if the
/// transform constructs one).
fn apply_transform(
    doc: &mut Value,
    transform: &BodyTransform,
    partition: &str,
) -> Result<Option<String>, RequestError> {
    match transform {
        BodyTransform::None => Ok(None),
        BodyTransform::Inject(fields) => {
            inject(doc, fields, partition)?;
            Ok(None)
        }
        BodyTransform::ConstructId(rule) => Ok(Some(build_id(rule, doc, partition)?)),
        BodyTransform::Both { inject: fields, id } => {
            inject(doc, fields, partition)?;
            Ok(Some(build_id(id, doc, partition)?))
        }
    }
}

/// Injects the resolved fields into `doc`.
fn inject(doc: &mut Value, fields: &[InjectedField], partition: &str) -> Result<(), RequestError> {
    let resolved = resolve_values(fields, partition)?;
    inject_fields(doc, &resolved).map_err(RequestError::from)
}

/// Constructs the `_id` from a rule.
fn build_id(rule: &DocIdRule, doc: &Value, partition: &str) -> Result<String, RequestError> {
    construct_id(rule.template.as_str(), partition, doc).map_err(RequestError::from)
}

/// The `_routing` value: the partition id, but only when a constructing
/// transform asked for it (`set_routing`).
fn routing_for(transform: &BodyTransform, partition: &str) -> Option<String> {
    let rule = match transform {
        BodyTransform::ConstructId(rule) | BodyTransform::Both { id: rule, .. } => Some(rule),
        BodyTransform::None | BodyTransform::Inject(_) => None,
    };
    rule.filter(|r| r.set_routing).map(|_| partition.to_owned())
}

/// Resolves each injected field's value to a concrete JSON value.
///
/// The tenancy adapter already resolves these to [`InjectedValue::Constant`];
/// [`InjectedValue::PartitionId`] is resolved here too for robustness, and
/// [`InjectedValue::FromPrincipal`] reaching this point is an invariant
/// violation (the engine has no principal here).
fn resolve_values(
    fields: &[InjectedField],
    partition: &str,
) -> Result<Vec<(FieldName, Value)>, RequestError> {
    fields
        .iter()
        .map(|field| {
            let value = match &field.value {
                InjectedValue::Constant(v) => v.clone(),
                InjectedValue::PartitionId => Value::String(partition.to_owned()),
                InjectedValue::FromPrincipal(_) => {
                    return Err(RequestError::Internal {
                        reason: "injected principal value reached the engine unresolved",
                    })
                }
            };
            Ok((field.name.clone(), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{ClusterId, Epoch, IndexName, PartitionId, Target};
    use osproxy_spi::{IdTemplate, Protocol, RouteDecision};
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

    fn index_op(batch: &WriteBatch) -> (&Option<String>, &Option<String>, Value) {
        match &batch.ops()[0].doc {
            DocOp::Index { id, routing, body } => {
                (id, routing, serde_json::from_slice(body).unwrap())
            }
            // The single-doc plan path only produces Index or Delete.
            DocOp::Create { .. } | DocOp::Update { .. } | DocOp::Delete { .. } => {
                unreachable_delete()
            }
        }
    }

    // A helper that returns a value of any type while never being called, so the
    // match above stays total without a panic-family macro (NFR-R1).
    fn unreachable_delete() -> (&'static Option<String>, &'static Option<String>, Value) {
        (&None, &None, Value::Null)
    }

    #[test]
    fn inject_and_construct_id_with_routing() {
        let inject = vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )];
        let id = DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true);
        let r = resolved(BodyTransform::Both { inject, id });
        let batch = build_write_batch(&r, br#"{ "id": 1001, "msg": "hi" }"#).unwrap();

        assert_eq!(batch.ops()[0].epoch, Epoch::new(4));
        let (id, routing, body) = index_op(&batch);
        assert_eq!(id.as_deref(), Some("acme:1001"));
        assert_eq!(routing.as_deref(), Some("acme"));
        assert_eq!(body["_tenant"], json!("acme"));
        assert_eq!(body["msg"], json!("hi"));
    }

    #[test]
    fn inject_only_has_no_id_or_routing() {
        let inject = vec![InjectedField::new(
            FieldName::from("_t"),
            InjectedValue::Constant(json!("acme")),
        )];
        let r = resolved(BodyTransform::Inject(inject));
        let batch = build_write_batch(&r, br#"{ "k": 1 }"#).unwrap();
        let (id, routing, body) = index_op(&batch);
        assert!(id.is_none());
        assert!(routing.is_none());
        assert_eq!(body["_t"], json!("acme"));
    }

    #[test]
    fn construct_id_without_routing() {
        let id = DocIdRule::new(IdTemplate::new("{partition}:{body.k}"));
        let r = resolved(BodyTransform::ConstructId(id));
        let batch = build_write_batch(&r, br#"{ "k": "v" }"#).unwrap();
        let (id, routing, _) = index_op(&batch);
        assert_eq!(id.as_deref(), Some("acme:v"));
        assert!(routing.is_none());
    }

    #[test]
    fn none_transform_passes_body_through() {
        let r = resolved(BodyTransform::None);
        let batch = build_write_batch(&r, br#"{ "k": 1 }"#).unwrap();
        let (id, routing, body) = index_op(&batch);
        assert!(id.is_none());
        assert!(routing.is_none());
        assert_eq!(body, json!({ "k": 1 }));
    }

    #[test]
    fn reserved_field_collision_is_rejected() {
        let inject = vec![InjectedField::new(
            FieldName::from("_t"),
            InjectedValue::Constant(json!("acme")),
        )];
        let r = resolved(BodyTransform::Inject(inject));
        let err = build_write_batch(&r, br#"{ "_t": "evil" }"#).unwrap_err();
        assert!(matches!(
            err,
            RequestError::Rewrite(RewriteError::ReservedFieldCollision { .. })
        ));
    }

    #[test]
    fn malformed_body_is_rejected() {
        let r = resolved(BodyTransform::None);
        let err = build_write_batch(&r, b"not json").unwrap_err();
        assert!(matches!(
            err,
            RequestError::Rewrite(RewriteError::InvalidJson)
        ));
    }

    #[test]
    fn unresolved_principal_value_is_internal_error() {
        let inject = vec![InjectedField::new(
            FieldName::from("_t"),
            InjectedValue::FromPrincipal("tenant".to_owned()),
        )];
        let r = resolved(BodyTransform::Inject(inject));
        let err = build_write_batch(&r, br#"{ "k": 1 }"#).unwrap_err();
        assert!(matches!(err, RequestError::Internal { .. }));
    }
}
