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
use osproxy_rewrite::parse_bulk;
use osproxy_sink::{OpResult, Sink, SinkError, WriteAck, WriteBatch};
use osproxy_spi::{RequestCtx, TenancySpi};
use osproxy_tenancy::{Resolved, TenancyRouter};
use serde_json::{json, Value};

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
pub(crate) async fn ingest_bulk<T: TenancySpi, S: Sink>(
    router: &TenancyRouter<T>,
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

/// Flushes one target's sub-batch in place: re-check the migration write gate
/// per item, dispatch the admitted ops, and apply each result to `lines` by its
/// original ordinal. Awaited inline, so the transformed bytes are freed before
/// parsing resumes (the mid-stream backpressure that bounds memory).
async fn flush<T: TenancySpi, S: Sink>(
    router: &TenancyRouter<T>,
    sink: &S,
    entries: Entries,
    lines: &mut [Value],
) {
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
async fn flush_remaining<T: TenancySpi, S: Sink>(
    router: &TenancyRouter<T>,
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
async fn gate<T: TenancySpi>(router: &TenancyRouter<T>, entries: Entries) -> (Entries, Entries) {
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
