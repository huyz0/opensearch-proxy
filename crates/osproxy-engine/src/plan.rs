//! Turning a resolved routing decision into a concrete [`WriteBatch`].
//!
//! This is the write-path glue: it takes the [`Resolved`] decision from the
//! tenancy router and the original request body, applies the body transform
//! (inject tenancy fields, construct the `_id`, set `_routing`), and produces
//! the epoch-stamped batch the [`Sink`](osproxy_sink::Sink) will deliver
//! (`docs/04`). Pure and synchronous, no network, fully testable.

use osproxy_core::FieldName;
use osproxy_rewrite::{construct_id_bytes, inject_fields_bytes, validate_json};
use osproxy_sink::{DocOp, WriteBatch, WriteOp};
use osproxy_spi::{BodyTransform, DocIdRule, InjectedField, InjectedValue};
use osproxy_tenancy::Resolved;
use serde_json::Value;

use crate::error::RequestError;

/// Builds the single-document write batch for a resolved ingest request.
///
/// Applies the body transform by **scanning and splicing the raw bytes**, never
/// parsing the body into a `Value` tree or re-serializing it (ADR-014): an
/// injected field is written right after the opening `{`, an id is read straight
/// from the bytes, and an untransformed body is forwarded verbatim.
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

    let (id, out_body) = apply_transform(body, &decision.body_transform, partition)?;
    let routing = routing_for(&decision.body_transform, partition);

    let op = WriteOp::new(
        decision.target.clone(),
        DocOp::Index {
            id,
            routing,
            body: out_body,
        },
        decision.epoch,
    )
    .with_protocol(decision.upstream_protocol);
    Ok(WriteBatch::single(op))
}

/// Applies the body transform over the raw bytes, returning the constructed
/// `_id` (if any) and the bytes to write.
fn apply_transform(
    body: &[u8],
    transform: &BodyTransform,
    partition: &str,
) -> Result<(Option<String>, Vec<u8>), RequestError> {
    match transform {
        // Verbatim: forward unchanged, but still reject a malformed body.
        BodyTransform::None => {
            validate_json(body).map_err(RequestError::from)?;
            Ok((None, body.to_vec()))
        }
        BodyTransform::Inject(fields) => Ok((None, inject(body, fields, partition)?)),
        // The id reads client fields from the bytes; the body passes through.
        BodyTransform::ConstructId(rule) => {
            validate_json(body).map_err(RequestError::from)?;
            Ok((Some(build_id(rule, body, partition)?), body.to_vec()))
        }
        // Splice the injected fields (which validates the object), then read the
        // id from the original client bytes (id templates reference client fields).
        BodyTransform::Both { inject: fields, id } => {
            let out = inject(body, fields, partition)?;
            Ok((Some(build_id(id, body, partition)?), out))
        }
    }
}

/// Splices the resolved fields into the body bytes.
fn inject(body: &[u8], fields: &[InjectedField], partition: &str) -> Result<Vec<u8>, RequestError> {
    let resolved = resolve_values(fields, partition)?;
    inject_fields_bytes(body, &resolved).map_err(RequestError::from)
}

/// Constructs the `_id` from a rule by reading scalars straight from the bytes.
fn build_id(rule: &DocIdRule, body: &[u8], partition: &str) -> Result<String, RequestError> {
    construct_id_bytes(rule.template.as_str(), partition, body).map_err(RequestError::from)
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
                InjectedValue::FromPrincipal(_) | InjectedValue::FromHeader(_) => {
                    return Err(RequestError::Internal {
                        reason: "context-derived injected value reached the engine unresolved",
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
    use osproxy_rewrite::RewriteError;
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
            migration: osproxy_spi::MigrationPhase::Settled,
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
