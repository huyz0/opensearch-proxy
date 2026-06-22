//! The read-path glue for get-by-id, delete-by-id, and search (`docs/04` §4–5).
//!
//! Mirrors [`crate::plan`] on the read side: it turns a resolved routing decision
//! plus the client's request into the op the reader/sink runs, then shapes the
//! upstream response back into the client's logical view (strip injected fields,
//! map physical ids back to logical, present the logical index). Pure and
//! synchronous; the network hop happens in the pipeline.

use std::collections::BTreeMap;

use osproxy_core::FieldName;
use osproxy_rewrite::{map_logical_to_physical, map_physical_to_logical, strip_fields, wrap_query};
use osproxy_sink::{DocOp, ReadOp, SearchOp, WriteOp};
use osproxy_spi::{BodyTransform, DocIdRule, InjectedValue};
use osproxy_tenancy::Resolved;
use serde_json::value::RawValue;
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

/// Builds a delete [`WriteOp`] for a document already known by its **physical**
/// id (the `_delete_by_query` expansion, `docs/04` §9): the search ran against the
/// physical index, so its hit ids are physical — only `_routing` is derived from
/// the placement's id rule. Epoch-stamped like any write.
pub(crate) fn build_delete_op_physical(resolved: &Resolved, physical_id: String) -> WriteOp {
    let shape = read_shape(&resolved.decision.body_transform);
    let routing = shape
        .id_rule
        .as_ref()
        .filter(|r| r.set_routing)
        .map(|_| resolved.partition.as_str().to_owned());
    WriteOp::new(
        resolved.decision.target.clone(),
        DocOp::Delete {
            id: physical_id,
            routing,
        },
        resolved.decision.epoch,
    )
    .with_protocol(resolved.decision.upstream_protocol)
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
    let internal = || RequestError::Internal {
        reason: "serializing search response",
    };
    // Parse only the top level; the siblings the proxy never touches — `took`,
    // `_shards`, and especially `aggregations` (which can dwarf the hits) — stay as
    // raw byte spans rather than being materialized into a `Value` tree and
    // re-serialized (the same posture as `wrap_query`). Only the `hits` subtree is
    // shaped.
    let mut top: BTreeMap<String, Box<RawValue>> = match serde_json::from_slice(upstream_body) {
        Ok(top) => top,
        // A valid but non-object body has no hits to shape — pass it through
        // unchanged; only genuinely invalid JSON is an error (as before).
        Err(_) => {
            return if serde_json::from_slice::<&RawValue>(upstream_body).is_ok() {
                Ok(upstream_body.to_vec())
            } else {
                Err(osproxy_rewrite::RewriteError::InvalidJson.into())
            };
        }
    };
    if let Some(hits_raw) = top.remove("hits") {
        let mut hits: Value = serde_json::from_slice(hits_raw.get().as_bytes())
            .map_err(|_| osproxy_rewrite::RewriteError::InvalidJson)?;
        if let Some(arr) = hits.get_mut("hits").and_then(Value::as_array_mut) {
            for hit in arr.iter_mut() {
                shape_hit(hit, logical_index, partition, shape);
            }
        }
        top.insert(
            "hits".to_owned(),
            serde_json::value::to_raw_value(&hits).map_err(|_| internal())?,
        );
    }
    serde_json::to_vec(&top).map_err(|_| internal())
}

/// Strips one search hit in place into the client's logical view. Shared with the
/// streaming search transform ([`crate::search_scan`]), which frames one hit at a
/// time and reuses this exact (audited) per-hit strip rather than re-implementing
/// it — so the isolation boundary lives in one place.
pub(crate) fn shape_hit(hit: &mut Value, logical_index: &str, partition: &str, shape: &ReadShape) {
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
    // Isolation filters on the partition field(s) only. Decorative injected fields
    // (constants, principal/header-derived) are stripped from hits but never
    // filtered: their value can differ between the write and this read, so a term
    // on them would wrongly exclude the tenant's own documents.
    fields
        .iter()
        .filter(|field| matches!(field.value, InjectedValue::PartitionId))
        .map(|field| (field.name.clone(), Value::String(partition.to_owned())))
        .collect()
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
#[path = "read_tests.rs"]
mod tests;
