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
        match prepare(router, ctx, &mut cache, item).await {
            Ok(p) => {
                let target = p.op.target.clone();
                let buf = buffers.entry(target.clone()).or_default();
                buf.push((ordinal, p));
                if buf.len() >= FLUSH_THRESHOLD {
                    let entries = buffers.remove(&target).unwrap_or_default();
                    flush(sink, &entries, &mut lines).await;
                }
            }
            Err(fail) => lines[ordinal] = fail.into_line(),
        }
    }

    flush_remaining(sink, buffers, &mut lines).await;

    let errors = lines.iter().any(is_error_line);
    let body = json!({ "took": 0, "errors": errors, "items": lines });
    Ok(PipelineResponse {
        status: 200,
        body: serde_json::to_vec(&body).map_err(|_| RequestError::Internal {
            reason: "serializing bulk response",
        })?,
    })
}

/// Flushes one target's sub-batch in place: dispatch it and apply each result to
/// `lines` by its original ordinal. Awaited inline, so the transformed bytes are
/// freed before parsing resumes (the mid-stream backpressure that bounds memory).
async fn flush<S: Sink>(sink: &S, entries: &[(usize, Prepared)], lines: &mut [Value]) {
    let batch = build_batch(entries);
    apply_results(entries, sink.write(batch).await, lines);
}

/// Flushes every remaining target's sub-batch **concurrently** (bounded), then
/// applies the results by ordinal. Completion order cannot disturb re-interleave —
/// every line is keyed by its original ordinal.
async fn flush_remaining<S: Sink>(
    sink: &S,
    buffers: HashMap<Target, Entries>,
    lines: &mut [Value],
) {
    let pending = buffers.into_values().filter(|v| !v.is_empty());
    let results: Vec<(Entries, Result<WriteAck, SinkError>)> = futures_util::stream::iter(pending)
        .map(|entries| async move {
            let r = sink.write(build_batch(&entries)).await;
            (entries, r)
        })
        .buffer_unordered(MAX_DISPATCH_CONCURRENCY)
        .collect()
        .await;

    for (entries, result) in results {
        apply_results(&entries, result, lines);
    }
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
