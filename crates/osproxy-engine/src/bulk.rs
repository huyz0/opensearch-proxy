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

/// The most per-target sub-batches dispatched at once, so a wide fan-out cannot
/// open an unbounded number of upstream requests (NFR-P, `docs/04` §3).
const MAX_DISPATCH_CONCURRENCY: usize = 8;

/// Dispatches the per-target sub-batches **concurrently** (bounded) and fills the
/// result lines by ordinal.
///
/// Each target's batch is built up front (owning its ops), so the in-flight
/// futures share only `&S`; results are applied to `lines` after the bounded
/// stream drains. Completion order does not matter — every line is keyed by its
/// original ordinal, so re-interleave stays exact regardless of which target
/// finishes first.
async fn dispatch_targets<S: Sink>(
    sink: &S,
    by_target: HashMap<Target, Vec<usize>>,
    prepared: &[Option<Prepared>],
    lines: &mut [Value],
) {
    let batches = by_target.into_values().map(|ordinals| {
        let batch = ordinals
            .iter()
            .fold(WriteBatch::new(), |b, &o| match prepared[o].as_ref() {
                Some(p) => b.with(p.op.clone()),
                None => b,
            });
        (ordinals, batch)
    });

    let results: Vec<(Vec<usize>, Result<WriteAck, SinkError>)> =
        futures_util::stream::iter(batches)
            .map(|(ordinals, batch)| async move { (ordinals, sink.write(batch).await) })
            .buffer_unordered(MAX_DISPATCH_CONCURRENCY)
            .collect()
            .await;

    for (ordinals, result) in results {
        match result {
            Ok(ack) => {
                for (&ordinal, op_result) in ordinals.iter().zip(ack.results()) {
                    if let Some(p) = prepared[ordinal].as_ref() {
                        lines[ordinal] = success_line(p, op_result);
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
