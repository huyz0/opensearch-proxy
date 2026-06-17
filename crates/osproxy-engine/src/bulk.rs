//! Bulk (`_bulk`) demux: the hard path (`docs/04` §3).
//!
//! A single NDJSON body may carry documents for **different partitions →
//! different targets**. We resolve each operation's partition (caching the
//! placement per partition for the request), demux the operations by target,
//! dispatch each target's sub-batch, then **re-interleave** the per-item results
//! in the body's original order — so the client sees a normal OpenSearch bulk
//! response with positional per-item status. A per-item failure (e.g. an
//! unresolved partition) is positioned in place; the bulk as a whole still
//! returns 200 with `errors: true`. The per-item preparation lives in
//! [`crate::bulkprep`]; this module owns the orchestration and the response.
//!
//! Memory is bounded (NFR-P7): a target's sub-batch is flushed as soon as it
//! reaches [`FLUSH_THRESHOLD`], so the transformed working set stays a bounded
//! multiple of the threshold rather than growing to the whole body.

use std::collections::HashMap;

use futures_util::stream::StreamExt as _;
use osproxy_core::{PartitionId, Target};
use osproxy_rewrite::{parse_bulk, BulkAction};
use osproxy_sink::{OpResult, Sink, SinkError, WriteAck, WriteBatch};
use osproxy_spi::RequestCtx;
use osproxy_tenancy::{Resolved, Router};
use serde_json::{json, Value};

use crate::asyncwrite::{
    op_id_for, unavailable_response, unsupported_async, unsupported_response, QueuedWrite,
    WriteQueue,
};
use crate::bulkprep::{prepare, Prepared};
use crate::error::RequestError;
use crate::pipeline::PipelineResponse;

/// The largest a single target's sub-batch grows before it is flushed mid-stream,
/// bounding the transformed working set held in memory (NFR-P7).
const FLUSH_THRESHOLD: usize = 256;

/// The most per-target sub-batches dispatched at once in the final flush, so a
/// wide fan-out cannot open an unbounded number of upstream requests (NFR-P).
const MAX_DISPATCH_CONCURRENCY: usize = 8;

/// One target's buffered `(ordinal, prepared-op)` entries awaiting dispatch.
type Entries = Vec<(usize, Prepared)>;

/// Runs a `_bulk` request: parse, demux by target, dispatch, re-interleave.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] only if the whole body is unparseable;
/// per-operation failures are reported positionally in the response, not as a
/// request error.
pub(crate) async fn ingest_bulk<R: Router, S: Sink>(
    router: &R,
    sink: &S,
    ctx: &RequestCtx<'_>,
    retry: crate::RetryPolicy,
) -> Result<PipelineResponse, RequestError> {
    let items = parse_bulk(ctx.body())?;
    let n = items.len();

    // Per-item response line (filled now for failures, on flush for the rest) and
    // the per-target demux buffers. A target flushes once it reaches
    // FLUSH_THRESHOLD, so the transformed working set stays bounded (NFR-P7).
    let mut lines: Vec<Value> = vec![Value::Null; n];
    let mut buffers: HashMap<Target, Entries> = HashMap::new();
    let mut cache: HashMap<(PartitionId, String), Resolved> = HashMap::new();

    for (ordinal, item) in items.into_iter().enumerate() {
        match prepare(router, ctx, &mut cache, item, retry).await {
            Ok(p) => {
                let target = p.op.target.clone();
                let buf = buffers.entry(target.clone()).or_default();
                buf.push((ordinal, p));
                if buf.len() >= FLUSH_THRESHOLD {
                    let entries = buffers.remove(&target).unwrap_or_default();
                    flush(router, sink, entries, &mut lines).await;
                }
            }
            Err(fail) => lines[ordinal] = fail.into_line(),
        }
    }

    flush_remaining(router, sink, buffers, &mut lines).await;

    let errors = lines.iter().any(is_error_line);
    let body = json!({ "took": 0, "errors": errors, "items": lines });
    Ok(PipelineResponse {
        status: 200,
        body: serde_json::to_vec(&body).map_err(|_| RequestError::Internal {
            reason: "serializing bulk response",
        })?,
    })
}

/// The async fan-out counterpart of [`ingest_bulk`] (`docs/04` §9): each item is
/// resolved/transformed exactly as the sync path, then **durably enqueued** for
/// downstream fan-out instead of dispatched, and reported positionally as
/// `202 queued` with a per-item `op_id` (`{batch_id}:{ordinal}`).
///
/// Whole-request refusals (no queue, or a query-level unsupported op) return the
/// generic envelope, never a partially-applied bulk. A per-item `update` is
/// rejected in place (`400`): a scripted/partial update is not honorable async.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] only if the whole body is unparseable.
pub(crate) async fn ingest_bulk_async<R: Router>(
    router: &R,
    queue: &dyn WriteQueue,
    ctx: &RequestCtx<'_>,
    retry: crate::RetryPolicy,
) -> Result<PipelineResponse, RequestError> {
    let index = ctx.logical_index();
    // A query-level unsupported op (optimistic concurrency) refuses the whole
    // bulk; a missing queue refuses it too — never accepted-and-dropped.
    if let Some(reason) = unsupported_async(ctx) {
        return Ok(unsupported_response(reason, index));
    }
    if !queue.enabled() {
        return Ok(unavailable_response(index));
    }

    let items = parse_bulk(ctx.body())?;
    let batch_id = op_id_for(ctx, ctx.request_id());
    let mut lines: Vec<Value> = vec![Value::Null; items.len()];
    let mut cache: HashMap<(PartitionId, String), Resolved> = HashMap::new();

    for (ordinal, item) in items.into_iter().enumerate() {
        // A scripted/partial `_update` has no single current document to merge
        // against under fan-out; reject it in place.
        if matches!(item.action, BulkAction::Update) {
            lines[ordinal] = json!({ "update": {
                "_index": item.index.clone().unwrap_or_else(|| index.to_owned()),
                "_id": item.id,
                "status": 400,
                "error": { "type": "unsupported_async" },
            }});
            continue;
        }
        match prepare(router, ctx, &mut cache, item, retry).await {
            Ok(p) => {
                let op_id = format!("{batch_id}:{ordinal}");
                let write = QueuedWrite {
                    op_id: op_id.clone(),
                    partition_key: p.partition.as_str().to_owned(),
                    batch: WriteBatch::single(p.op.clone()),
                };
                lines[ordinal] = match queue.enqueue(write).await {
                    Ok(()) => queued_line(&p, &op_id),
                    Err(_) => enqueue_failed_line(&p),
                };
            }
            Err(fail) => lines[ordinal] = fail.into_line(),
        }
    }

    let errors = lines.iter().any(is_error_line);
    let body = json!({ "took": 0, "errors": errors, "items": lines });
    Ok(PipelineResponse {
        status: 200,
        body: serde_json::to_vec(&body).map_err(|_| RequestError::Internal {
            reason: "serializing bulk response",
        })?,
    })
}

/// The positioned `202 queued` line for an enqueued async op, carrying the
/// per-item `op_id` the client correlates a downstream outcome against.
fn queued_line(p: &Prepared, op_id: &str) -> Value {
    json!({ p.action: {
        "_index": p.logical_index,
        "_id": p.logical_id,
        "op_id": op_id,
        "status": 202,
        "result": "queued",
    }})
}

/// The positioned `503` line for an op the queue refused (retryable; the same
/// `op_id` makes a retry idempotent downstream).
fn enqueue_failed_line(p: &Prepared) -> Value {
    json!({ p.action: {
        "_index": p.logical_index,
        "_id": p.logical_id,
        "status": 503,
        "error": { "type": "enqueue_failed" },
    }})
}

/// Flushes one target's sub-batch in place: re-check the migration write gate
/// per item, dispatch the admitted ops, and apply each result to `lines` by its
/// original ordinal. Awaited inline, so the transformed bytes are freed before
/// parsing resumes (the mid-stream backpressure that bounds memory).
async fn flush<R: Router, S: Sink>(router: &R, sink: &S, entries: Entries, lines: &mut [Value]) {
    let (admitted, rejected) = gate(router, entries).await;
    for (ordinal, p) in &rejected {
        lines[*ordinal] = stale_epoch_line(p);
    }
    apply_results(&admitted, sink.write(build_batch(&admitted)).await, lines);
}

/// Flushes every remaining target's sub-batch **concurrently** (bounded). Each
/// task gates its entries (no `lines` access, so the tasks stay independent) and
/// dispatches the admitted ops; results are applied by ordinal afterward, so
/// completion order cannot disturb re-interleave.
async fn flush_remaining<R: Router, S: Sink>(
    router: &R,
    sink: &S,
    buffers: HashMap<Target, Entries>,
    lines: &mut [Value],
) {
    type Flushed = (Entries, Entries, Result<WriteAck, SinkError>);
    let pending = buffers.into_values().filter(|v| !v.is_empty());
    let results: Vec<Flushed> = futures_util::stream::iter(pending)
        .map(|entries| async move {
            let (admitted, rejected) = gate(router, entries).await;
            let ack = sink.write(build_batch(&admitted)).await;
            (admitted, rejected, ack)
        })
        .buffer_unordered(MAX_DISPATCH_CONCURRENCY)
        .collect()
        .await;

    for (admitted, rejected, ack) in results {
        for (ordinal, p) in &rejected {
            lines[*ordinal] = stale_epoch_line(p);
        }
        apply_results(&admitted, ack, lines);
    }
}

/// Splits a target's entries by the migration write gate (`docs/06` §2),
/// re-checked here at dispatch: `(admitted, rejected)`. A rejected item resolved
/// against a placement that has since advanced or entered cutover — it is held,
/// never dispatched.
async fn gate<R: Router>(router: &R, entries: Entries) -> (Entries, Entries) {
    let mut admitted = Entries::new();
    let mut rejected = Entries::new();
    for (ordinal, p) in entries {
        if router.admit_write(&p.partition, p.op.epoch).await {
            admitted.push((ordinal, p));
        } else {
            rejected.push((ordinal, p));
        }
    }
    (admitted, rejected)
}

/// The response line for an item held by the migration write gate: a positioned,
/// retryable `409` so the client re-resolves and retries just that item.
fn stale_epoch_line(p: &Prepared) -> Value {
    json!({ p.action: {
        "_index": p.logical_index,
        "_id": p.logical_id,
        "status": 409,
        "error": { "type": "stale_epoch" },
    }})
}

/// Builds the [`WriteBatch`] for a target's buffered entries.
fn build_batch(entries: &[(usize, Prepared)]) -> WriteBatch {
    entries
        .iter()
        .fold(WriteBatch::new(), |b, (_, p)| b.with(p.op.clone()))
}

/// Applies a sub-batch's outcome to the response lines by ordinal.
fn apply_results(
    entries: &[(usize, Prepared)],
    result: Result<WriteAck, SinkError>,
    lines: &mut [Value],
) {
    match result {
        Ok(ack) => {
            for ((ordinal, p), op_result) in entries.iter().zip(ack.results()) {
                lines[*ordinal] = success_line(p, op_result);
            }
        }
        Err(_) => {
            for (ordinal, p) in entries {
                lines[*ordinal] = upstream_failure_line(p);
            }
        }
    }
}

/// The response line for a dispatched op. A 2xx/3xx is a positional success
/// (logical id/index); a 4xx upstream rejection (e.g. a `create` id conflict) is
/// surfaced as a positioned, value-free error so the bulk reports `errors:true`.
fn success_line(p: &Prepared, result: &OpResult) -> Value {
    if result.status >= 400 {
        return json!({ p.action: {
            "_index": p.logical_index,
            "_id": p.logical_id,
            "status": result.status,
            "error": { "type": error_type_for(result.status) },
        }});
    }
    let outcome = if result.created { "created" } else { "updated" };
    json!({ p.action: {
        "_index": p.logical_index,
        "_id": p.logical_id,
        "status": result.status,
        "result": outcome,
    }})
}

/// A value-free error type for a 4xx upstream item status.
fn error_type_for(status: u16) -> &'static str {
    match status {
        409 => "conflict",
        404 => "not_found",
        _ => "rejected",
    }
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

/// Whether a response line carries a per-item error.
fn is_error_line(line: &Value) -> bool {
    line.as_object()
        .and_then(|o| o.values().next())
        .and_then(|v| v.get("error"))
        .is_some()
}
