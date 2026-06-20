//! Per-item preparation for the `_bulk` demux (`docs/04` §3).
//!
//! Splitting one parsed [`BulkItem`] into either a ready-to-dispatch
//! [`Prepared`] write op or a positioned [`ItemFailure`]: resolve the item's
//! partition (caching the placement per partition for the request), apply the
//! tenancy transform (inject fields, construct/​map the id, set routing), and
//! pick the [`DocOp`] for the verb. Kept separate from the orchestration in
//! [`crate::bulk`] so neither file becomes a god module.

use std::collections::HashMap;

use osproxy_core::PartitionId;
use osproxy_rewrite::{
    construct_id_bytes, inject_fields_bytes, inject_update, map_logical_to_physical,
    map_physical_to_logical, BulkAction, BulkItem,
};
use osproxy_sink::{DocOp, WriteOp};
use osproxy_spi::{BodyDoc, BodyTransform, InjectedField, InjectedValue, RequestCtx};
use osproxy_tenancy::{Resolved, Router};
use serde_json::Value;

/// A prepared operation: the write op plus what the response line needs and the
/// partition it resolved for (so the migration write gate can be re-checked at
/// flush, `docs/06` §2).
pub(crate) struct Prepared {
    pub(crate) op: WriteOp,
    pub(crate) partition: PartitionId,
    pub(crate) action: &'static str,
    pub(crate) logical_index: String,
    pub(crate) logical_id: String,
}

/// A per-item failure carrying the response shape (never tenant data).
pub(crate) struct ItemFailure {
    action: &'static str,
    logical_index: String,
    logical_id: Option<String>,
    status: u16,
    error: &'static str,
}

impl ItemFailure {
    /// The positioned `{action: {…, error}}` response line for this failure.
    pub(crate) fn into_line(self) -> Value {
        serde_json::json!({ self.action: {
            "_index": self.logical_index,
            "_id": self.logical_id,
            "status": self.status,
            "error": { "type": self.error },
        }})
    }
}

/// Prepares one bulk operation: resolve its partition, cache the placement per
/// partition, and build the epoch-stamped write op.
pub(crate) async fn prepare<R: Router>(
    router: &R,
    ctx: &RequestCtx<'_>,
    cache: &mut HashMap<(PartitionId, String), Resolved>,
    item: BulkItem,
    retry: crate::RetryPolicy,
) -> Result<Prepared, ItemFailure> {
    let action = item.action.keyword();
    let logical_index = item
        .index
        .clone()
        .unwrap_or_else(|| ctx.logical_index().to_owned());

    let partition = router
        .resolve_partition(
            ctx,
            BodyDoc::new(item.source.as_deref().unwrap_or_default()),
        )
        .map_err(|_| {
            fail(
                action,
                &logical_index,
                item.id.clone(),
                400,
                "partition_unresolved",
            )
        })?;

    let key = (partition.clone(), logical_index.clone());
    let resolved = if let Some(r) = cache.get(&key) {
        r.clone()
    } else {
        let r = crate::retry::with_retry(retry, || {
            router.resolve_placement(ctx, partition.clone(), &logical_index)
        })
        .await
        .map_err(|_| {
            fail(
                action,
                &logical_index,
                item.id.clone(),
                404,
                "placement_missing",
            )
        })?;
        cache.insert(key, r.clone());
        r
    };

    let mut prepared = build_op(&resolved, &item, action, logical_index)?;
    prepared.op = prepared
        .op
        .with_trace(Some(crate::endpoints::wire_trace(ctx)));
    Ok(prepared)
}

/// Builds the write op for a resolved bulk item (index/create, update, delete).
fn build_op(
    resolved: &Resolved,
    item: &BulkItem,
    action: &'static str,
    logical_index: String,
) -> Result<Prepared, ItemFailure> {
    let partition = resolved.partition.as_str();
    let (inject, id_rule) = transform_parts(&resolved.decision.body_transform, partition);
    let rule = id_rule.as_ref();
    let bad = |code| fail(action, &logical_index, item.id.clone(), 400, code);

    let (doc, logical_id) = match item.action {
        BulkAction::Delete => {
            let logical = item.id.clone().ok_or_else(|| bad("delete_without_id"))?;
            let id =
                physical_id(rule, partition, &logical).ok_or_else(|| bad("irreversible_id"))?;
            (
                DocOp::Delete {
                    id,
                    routing: routing_for(rule, partition),
                },
                logical,
            )
        }
        BulkAction::Update => build_update(item, &inject, rule, partition, bad)?,
        BulkAction::Index | BulkAction::Create => {
            let source = item
                .source
                .as_deref()
                .ok_or_else(|| bad("missing_source"))?;
            // Splice the tenancy fields straight into the source bytes and read
            // the id from the original bytes — no `Value` tree (ADR-014).
            let body = inject_fields_bytes(source, &inject)
                .map_err(|_| bad("reserved_field_collision"))?;
            let (id, logical) =
                index_id(rule, partition, item, source).ok_or_else(|| bad("id_construction"))?;
            let routing = id.as_ref().and_then(|_| routing_for(rule, partition));
            // `create` fails-if-exists upstream (op_type=create); `index` replaces.
            let doc = if item.action == BulkAction::Create {
                DocOp::Create { id, routing, body }
            } else {
                DocOp::Index { id, routing, body }
            };
            (doc, logical)
        }
    };

    Ok(Prepared {
        op: WriteOp::new(
            resolved.decision.target.clone(),
            doc,
            resolved.decision.epoch,
        )
        .with_protocol(resolved.decision.upstream_protocol),
        partition: resolved.partition.clone(),
        action,
        logical_index,
        logical_id,
    })
}

/// Builds the update op for a bulk `update`: map the client's logical id to the
/// physical id (an update is always targeted), then inject the tenancy fields
/// into the update's `doc`/`upsert` so an upsert that *creates* the document
/// still carries its isolation fields (`docs/03`, `docs/04` §3).
fn build_update<F: Fn(&'static str) -> ItemFailure>(
    item: &BulkItem,
    inject: &[(osproxy_core::FieldName, Value)],
    rule: Option<&IdRule<'_>>,
    partition: &str,
    bad: F,
) -> Result<(DocOp, String), ItemFailure> {
    let logical = item.id.clone().ok_or_else(|| bad("update_without_id"))?;
    let id = physical_id(rule, partition, &logical).ok_or_else(|| bad("irreversible_id"))?;
    // An `_update` body is structurally nested (`doc`/`upsert` sub-objects), so it
    // takes the `Value` path: the byte splice handles only top-level objects. This
    // is the rarer bulk verb; the hot index/create path stays tree-free (ADR-014).
    let source = item
        .source
        .as_deref()
        .ok_or_else(|| bad("missing_source"))?;
    let mut update: Value = serde_json::from_slice(source).map_err(|_| bad("invalid_json"))?;
    inject_update(&mut update, inject).map_err(|_| bad("reserved_field_collision"))?;
    let body = serde_json::to_vec(&update).map_err(|_| bad("serialize"))?;
    Ok((
        DocOp::Update {
            id,
            routing: routing_for(rule, partition),
            body,
        },
        logical,
    ))
}

/// The physical id and the logical id to echo for an index/create op. Reads any
/// `{body.<path>}` id component straight from the source bytes (no `Value` tree).
fn index_id(
    id_rule: Option<&IdRule<'_>>,
    partition: &str,
    item: &BulkItem,
    source: &[u8],
) -> Option<(Option<String>, String)> {
    match (id_rule, item.id.as_deref()) {
        // Client-supplied id: map logical→physical; echo the client's logical id.
        (Some(rule), Some(logical)) => {
            let physical = map_logical_to_physical(rule.template, partition, logical).ok()?;
            Some((Some(physical), logical.to_owned()))
        }
        // Rule-constructed id: echo the natural key recovered from the physical.
        (Some(rule), None) => {
            let physical = construct_id_bytes(rule.template, partition, source).ok()?;
            let logical = map_physical_to_logical(rule.template, partition, &physical)
                .ok()
                .flatten()
                .unwrap_or_else(|| physical.clone());
            Some((Some(physical), logical))
        }
        // No id rule: the client id is the physical id (or auto-id).
        (None, id) => Some((id.map(str::to_owned), id.unwrap_or("").to_owned())),
    }
}

/// Maps a logical id to a physical id for a delete/update (via the id rule).
fn physical_id(id_rule: Option<&IdRule<'_>>, partition: &str, logical: &str) -> Option<String> {
    match id_rule {
        Some(rule) => map_logical_to_physical(rule.template, partition, logical).ok(),
        None => Some(logical.to_owned()),
    }
}

/// The `_routing` value when the id rule sets routing.
fn routing_for(id_rule: Option<&IdRule<'_>>, partition: &str) -> Option<String> {
    id_rule
        .filter(|r| r.set_routing)
        .map(|_| partition.to_owned())
}

/// The id template + routing flag extracted from a body transform.
struct IdRule<'a> {
    template: &'a str,
    set_routing: bool,
}

/// Splits a body transform into the resolved inject pairs and the id rule.
fn transform_parts<'a>(
    transform: &'a BodyTransform,
    partition: &str,
) -> (Vec<(osproxy_core::FieldName, Value)>, Option<IdRule<'a>>) {
    let (fields, rule): (&[InjectedField], Option<&osproxy_spi::DocIdRule>) = match transform {
        BodyTransform::None => (&[], None),
        BodyTransform::Inject(f) => (f, None),
        BodyTransform::ConstructId(r) => (&[], Some(r)),
        BodyTransform::Both { inject, id } => (inject, Some(id)),
    };
    let inject = fields
        .iter()
        .map(|f| (f.name.clone(), constant(&f.value, partition)))
        .collect();
    let id_rule = rule.map(|r| IdRule {
        template: r.template.as_str(),
        set_routing: r.set_routing,
    });
    (inject, id_rule)
}

/// The concrete value of an injected field. The adapter resolves these to
/// constants; `PartitionId` is resolved here too for robustness so isolation can
/// never depend on an empty value (a `FromPrincipal` value cannot be
/// reconstructed here and falls back to the partition — always isolating).
fn constant(value: &InjectedValue, partition: &str) -> Value {
    match value {
        InjectedValue::Constant(v) => v.clone(),
        // The adapter resolves context-derived values to constants before this
        // point; `PartitionId` resolves here so isolation never depends on an
        // empty value. The decorative variants fall back to the partition only as
        // unreachable robustness.
        InjectedValue::PartitionId
        | InjectedValue::FromPrincipal(_)
        | InjectedValue::FromHeader(_) => Value::String(partition.to_owned()),
    }
}

/// Constructs an [`ItemFailure`].
fn fail(
    action: &'static str,
    logical_index: &str,
    logical_id: Option<String>,
    status: u16,
    error: &'static str,
) -> ItemFailure {
    ItemFailure {
        action,
        logical_index: logical_index.to_owned(),
        logical_id,
        status,
        error,
    }
}
