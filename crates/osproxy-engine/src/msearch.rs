//! Multi-search (`_msearch`) demux: the search counterpart of `_bulk`
//! (`docs/04` §4).
//!
//! One `_msearch` body runs many searches that may target **different logical
//! indices → different targets**. The partition is the caller's (one principal →
//! one partition), resolved once; then per search we resolve its placement
//! (cached per logical index), wrap the client query in the mandatory partition
//! filter, and run it **bounded-concurrently**, like the bulk dispatch. The
//! per-search responses are re-interleaved into `responses[]` in input order and
//! each stripped to the client's logical view (injected fields removed, physical
//! ids mapped back). A per-search failure is positioned in place; the request as
//! a whole still returns 200.

use std::collections::HashMap;

use futures_util::stream::StreamExt as _;
use osproxy_rewrite::{parse_msearch, MsearchItem};
use osproxy_sink::{Reader, SearchOp, SearchOutcome, SinkError};
use osproxy_spi::RequestCtx;
use osproxy_tenancy::{Resolved, Router};
use serde_json::{json, Value};

use crate::error::RequestError;
use crate::pipeline::PipelineResponse;
use crate::read::{shape_hits, ReadShape};
use crate::retry::with_retry;

/// The most concurrent in-flight upstream searches, bounding a wide `_msearch`
/// fan-out just as the bulk dispatch bounds its per-target writes (NFR-P).
const MAX_SEARCH_CONCURRENCY: usize = 8;

/// Runs an `_msearch` request: parse, per-search resolve, concurrent run, re-interleave.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] if the body is unparseable, or
/// [`RequestError::Spi`] if the caller's partition cannot be resolved (a
/// request-level failure). Per-search failures are reported positionally in
/// `responses[]`, not as a request error.
pub(crate) async fn multi_search<R: Router, S: Reader>(
    router: &R,
    sink: &S,
    ctx: &RequestCtx<'_>,
    retry: crate::RetryPolicy,
) -> Result<PipelineResponse, RequestError> {
    let items = parse_msearch(ctx.body())?;
    let n = items.len();

    // The partition is the caller's, resolved once (an `_msearch` resolves no
    // partition from its query bodies). A failure here is request-level.
    let partition = router.resolve_partition(ctx, None)?;

    let mut responses: Vec<Value> = vec![Value::Null; n];
    let mut prepared: Vec<Option<Prepared>> = (0..n).map(|_| None).collect();
    let mut cache: HashMap<String, Resolved> = HashMap::new();

    for (ordinal, item) in items.into_iter().enumerate() {
        match prepare(router, ctx, &partition, &mut cache, &item, retry).await {
            Ok(p) => prepared[ordinal] = Some(p),
            Err(line) => responses[ordinal] = line,
        }
    }

    run_all(sink, &prepared, &mut responses).await;

    let body = json!({ "responses": responses });
    Ok(PipelineResponse {
        status: 200,
        body: serde_json::to_vec(&body).map_err(|_| RequestError::Internal {
            reason: "serializing msearch response",
        })?,
    })
}

/// A prepared search: the wrapped op plus what its response needs to be shaped.
struct Prepared {
    op: SearchOp,
    shape: ReadShape,
    logical_index: String,
    partition: String,
}

/// Prepares one search: resolve its placement (cached per logical index), then
/// wrap the client query in the mandatory partition filter.
async fn prepare<R: Router>(
    router: &R,
    ctx: &RequestCtx<'_>,
    partition: &osproxy_core::PartitionId,
    cache: &mut HashMap<String, Resolved>,
    item: &MsearchItem,
    retry: crate::RetryPolicy,
) -> Result<Prepared, Value> {
    let logical_index = item
        .index
        .clone()
        .unwrap_or_else(|| ctx.logical_index().to_owned());

    let resolved = if let Some(r) = cache.get(&logical_index) {
        r.clone()
    } else {
        // Retry a momentarily-unavailable placement backend with bounded backoff,
        // matching the single-doc/bulk resolve paths (`docs/06` §3a).
        let r = with_retry(retry, || {
            router.resolve_placement(ctx, partition.clone(), &logical_index)
        })
        .await
        .map_err(|_| error_response(400, "placement_missing"))?;
        cache.insert(logical_index.clone(), r.clone());
        r
    };

    let (op, shape) = crate::read::build_search_op(&resolved, &item.query)
        .map_err(|_| error_response(400, "invalid_query"))?;
    Ok(Prepared {
        op: op.with_trace(Some(crate::endpoints::wire_trace(ctx))),
        shape,
        logical_index,
        partition: resolved.partition.as_str().to_owned(),
    })
}

/// Runs the prepared searches **concurrently** (bounded) and fills `responses`
/// by ordinal, so completion order cannot disturb the re-interleave.
async fn run_all<S: Reader>(sink: &S, prepared: &[Option<Prepared>], responses: &mut [Value]) {
    // Collect the owned ops up front so the in-flight futures borrow only `&S`.
    let ops: Vec<(usize, SearchOp)> = prepared
        .iter()
        .enumerate()
        .filter_map(|(ordinal, slot)| slot.as_ref().map(|p| (ordinal, p.op.clone())))
        .collect();

    let results: Vec<(usize, Result<SearchOutcome, SinkError>)> = futures_util::stream::iter(ops)
        .map(|(ordinal, op)| async move { (ordinal, sink.search(op).await) })
        .buffer_unordered(MAX_SEARCH_CONCURRENCY)
        .collect()
        .await;

    for (ordinal, result) in results {
        if let Some(p) = prepared[ordinal].as_ref() {
            responses[ordinal] = shape_response(p, result);
        }
    }
}

/// Shapes one search outcome into its `responses[]` entry: the hits stripped to
/// the client's logical view with a `status`, or a positioned error response.
fn shape_response(p: &Prepared, result: Result<SearchOutcome, SinkError>) -> Value {
    let Ok(outcome) = result else {
        return error_response(502, "upstream_failed");
    };
    let shaped = shape_hits(&outcome.body, &p.logical_index, &p.partition, &p.shape);
    match shaped
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
    {
        Some(mut value) => {
            if let Some(obj) = value.as_object_mut() {
                obj.insert("status".to_owned(), json!(outcome.status));
            }
            value
        }
        None => error_response(502, "malformed_upstream"),
    }
}

/// A per-search error entry (a value-free error type and a status).
fn error_response(status: u16, error: &'static str) -> Value {
    json!({ "error": { "type": error }, "status": status })
}
