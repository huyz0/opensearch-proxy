//! Bulk (`_bulk`) demux: the hard path (`docs/04` §3).
//!
//! A single NDJSON body may carry documents for **different partitions →
//! different targets**. We resolve each operation's partition (caching the
//! placement per partition for the request), demux the operations by target,
//! dispatch each target's sub-batch, then **re-interleave** the per-item results
//! in the body's original order — so the client sees a normal OpenSearch bulk
//! response with positional per-item status. A per-item failure (e.g. an
//! unresolved partition) is positioned in place; the bulk as a whole still
//! returns 200 with `errors: true`.

use std::collections::HashMap;

use osproxy_core::{PartitionId, Target};
use osproxy_rewrite::{
    construct_id, inject_fields, map_logical_to_physical, map_physical_to_logical, parse_bulk,
    BulkAction, BulkItem,
};
use osproxy_sink::{DocOp, OpResult, Sink, WriteBatch, WriteOp};
use osproxy_spi::{BodyTransform, InjectedField, InjectedValue, RequestCtx, TenancySpi};
use osproxy_tenancy::{Resolved, TenancyRouter};
use serde_json::{json, Value};

use crate::error::RequestError;
use crate::pipeline::PipelineResponse;

/// Runs a `_bulk` request: parse, demux by target, dispatch, re-interleave.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] only if the whole body is unparseable;
/// per-operation failures are reported positionally in the response, not as a
/// request error.
pub(crate) async fn ingest_bulk<T: TenancySpi, S: Sink>(
    router: &TenancyRouter<T>,
    sink: &S,
    ctx: &RequestCtx<'_>,
) -> Result<PipelineResponse, RequestError> {
    let items = parse_bulk(ctx.body())?;
    let n = items.len();

    // Per-item response line (filled now for failures, after dispatch for the
    // rest) and the per-target demux (ordinals into `prepared`).
    let mut lines: Vec<Value> = vec![Value::Null; n];
    let mut prepared: Vec<Option<Prepared>> = (0..n).map(|_| None).collect();
    let mut by_target: HashMap<Target, Vec<usize>> = HashMap::new();
    let mut cache: HashMap<(PartitionId, String), Resolved> = HashMap::new();

    for (ordinal, item) in items.into_iter().enumerate() {
        match prepare(router, ctx, &mut cache, item).await {
            Ok(p) => {
                by_target
                    .entry(p.op.target.clone())
                    .or_default()
                    .push(ordinal);
                prepared[ordinal] = Some(p);
            }
            Err(fail) => lines[ordinal] = fail.into_line(),
        }
    }

    dispatch_targets(sink, by_target, &prepared, &mut lines).await;

    let errors = lines.iter().any(is_error_line);
    let body = json!({ "took": 0, "errors": errors, "items": lines });
    Ok(PipelineResponse {
        status: 200,
        body: serde_json::to_vec(&body).map_err(|_| RequestError::Internal {
            reason: "serializing bulk response",
        })?,
    })
}

/// A prepared operation: the write op plus what the response line needs.
struct Prepared {
    op: WriteOp,
    action: &'static str,
    logical_index: String,
    logical_id: String,
}

/// A per-item failure carrying the response shape (never tenant data).
struct ItemFailure {
    action: &'static str,
    logical_index: String,
    logical_id: Option<String>,
    status: u16,
    error: &'static str,
}

impl ItemFailure {
    fn into_line(self) -> Value {
        json!({ self.action: {
            "_index": self.logical_index,
            "_id": self.logical_id,
            "status": self.status,
            "error": { "type": self.error },
        }})
    }
}

/// Prepares one bulk operation: resolve its partition, cache the placement per
/// partition, and build the epoch-stamped write op.
async fn prepare<T: TenancySpi>(
    router: &TenancyRouter<T>,
    ctx: &RequestCtx<'_>,
    cache: &mut HashMap<(PartitionId, String), Resolved>,
    item: BulkItem,
) -> Result<Prepared, ItemFailure> {
    let action = item.action.keyword();
    let logical_index = item
        .index
        .clone()
        .unwrap_or_else(|| ctx.logical_index().to_owned());

    if item.action == BulkAction::Update {
        return Err(fail(
            action,
            &logical_index,
            item.id.clone(),
            400,
            "unsupported_in_bulk",
        ));
    }

    let partition = router
        .resolve_partition(ctx, item.source.as_ref())
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
        let r = router
            .resolve_placement(ctx, partition.clone(), &logical_index)
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

    build_op(&resolved, &item, action, logical_index)
}

/// Builds the write op for a resolved bulk item (index/create or delete).
fn build_op(
    resolved: &Resolved,
    item: &BulkItem,
    action: &'static str,
    logical_index: String,
) -> Result<Prepared, ItemFailure> {
    let partition = resolved.partition.as_str();
    let (inject, id_rule) = transform_parts(&resolved.decision.body_transform, partition);
    let target = resolved.decision.target.clone();
    let epoch = resolved.decision.epoch;
    let bad = |code| fail(action, &logical_index, item.id.clone(), 400, code);

    let rule = id_rule.as_ref();
    let (doc, logical_id) = if item.action == BulkAction::Delete {
        let logical = item.id.clone().ok_or_else(|| bad("delete_without_id"))?;
        let id = physical_id(rule, partition, &logical).ok_or_else(|| bad("irreversible_id"))?;
        let routing = routing_for(rule, partition);
        (DocOp::Delete { id, routing }, logical)
    } else {
        let mut source = item.source.clone().ok_or_else(|| bad("missing_source"))?;
        inject_fields(&mut source, &inject).map_err(|_| bad("reserved_field_collision"))?;
        let (id, logical) =
            index_id(rule, partition, item, &source).ok_or_else(|| bad("id_construction"))?;
        let body = serde_json::to_vec(&source).map_err(|_| bad("serialize"))?;
        let routing = id.as_ref().and_then(|_| routing_for(rule, partition));
        (DocOp::Index { id, routing, body }, logical)
    };

    Ok(Prepared {
        op: WriteOp::new(target, doc, epoch),
        action,
        logical_index,
        logical_id,
    })
}

/// Dispatches each target's sub-batch and fills the result lines by ordinal.
async fn dispatch_targets<S: Sink>(
    sink: &S,
    by_target: HashMap<Target, Vec<usize>>,
    prepared: &[Option<Prepared>],
    lines: &mut [Value],
) {
    for (_target, ordinals) in by_target {
        let batch = ordinals
            .iter()
            .fold(WriteBatch::new(), |b, &o| match prepared[o].as_ref() {
                Some(p) => b.with(p.op.clone()),
                None => b,
            });
        match sink.write(batch).await {
            Ok(ack) => {
                for (&ordinal, result) in ordinals.iter().zip(ack.results()) {
                    if let Some(p) = prepared[ordinal].as_ref() {
                        lines[ordinal] = success_line(p, result);
                    }
                }
            }
            Err(_) => {
                for &ordinal in &ordinals {
                    if let Some(p) = prepared[ordinal].as_ref() {
                        lines[ordinal] = upstream_failure_line(p);
                    }
                }
            }
        }
    }
}

/// The response line for a successfully dispatched op (logical id/index).
fn success_line(p: &Prepared, result: &OpResult) -> Value {
    let outcome = if result.created { "created" } else { "updated" };
    json!({ p.action: {
        "_index": p.logical_index,
        "_id": p.logical_id,
        "status": result.status,
        "result": outcome,
    }})
}

/// The response line for an op whose target failed upstream.
fn upstream_failure_line(p: &Prepared) -> Value {
    json!({ p.action: {
        "_index": p.logical_index,
        "_id": p.logical_id,
        "status": 502,
        "error": { "type": "upstream_failed" },
    }})
}

/// The physical id and the logical id to echo for an index/create op.
fn index_id(
    id_rule: Option<&IdRule<'_>>,
    partition: &str,
    item: &BulkItem,
    source: &Value,
) -> Option<(Option<String>, String)> {
    match (id_rule, item.id.as_deref()) {
        // Client-supplied id: map logical→physical; echo the client's logical id.
        (Some(rule), Some(logical)) => {
            let physical = map_logical_to_physical(rule.template, partition, logical).ok()?;
            Some((Some(physical), logical.to_owned()))
        }
        // Rule-constructed id: echo the natural key recovered from the physical.
        (Some(rule), None) => {
            let physical = construct_id(rule.template, partition, source).ok()?;
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

/// Maps a logical id to a physical id for a delete (via the id rule if present).
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
        InjectedValue::PartitionId | InjectedValue::FromPrincipal(_) => {
            Value::String(partition.to_owned())
        }
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

/// Whether a response line carries a per-item error.
fn is_error_line(line: &Value) -> bool {
    line.as_object()
        .and_then(|o| o.values().next())
        .and_then(|v| v.get("error"))
        .is_some()
}
