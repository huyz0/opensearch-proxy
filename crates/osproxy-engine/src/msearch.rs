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

use std::collections::{BTreeMap, HashMap};

use futures_util::stream::StreamExt as _;
use osproxy_rewrite::{parse_msearch, MsearchItem};
use osproxy_sink::{Reader, SearchOp, SearchOutcome, SinkError};
use osproxy_spi::{BodyDoc, RequestCtx};
use osproxy_tenancy::{Resolved, Router};
use serde_json::json;
use serde_json::value::RawValue;

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
    let partition = router.resolve_partition(ctx, BodyDoc::new(&[]))?;

    // Each entry is one already-shaped sub-response as raw JSON bytes (placeholder
    // overwritten for every ordinal). Keeping them raw means a sub-response's
    // untouched siblings, `aggregations` especially, are never re-materialized
    // when assembling the envelope (ADR-014, the read-path raw posture).
    let mut responses: Vec<Vec<u8>> = vec![b"null".to_vec(); n];
    let mut prepared: Vec<Option<Prepared>> = (0..n).map(|_| None).collect();
    let mut cache: HashMap<String, Resolved> = HashMap::new();

    for (ordinal, item) in items.into_iter().enumerate() {
        match prepare(router, ctx, &partition, &mut cache, &item, retry).await {
            Ok(p) => prepared[ordinal] = Some(p),
            Err(line) => responses[ordinal] = line,
        }
    }

    run_all(sink, &prepared, &mut responses).await;

    Ok(PipelineResponse {
        status: 200,
        body: assemble_responses(&responses),
        content_type: None,
    })
}

/// Assembles the `{"responses":[…]}` envelope from the per-search raw byte
/// entries, concatenated directly, since each entry is already valid JSON, so no
/// sub-response is parsed back into a `Value` to nest it.
fn assemble_responses(responses: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::with_capacity(16 + responses.iter().map(Vec::len).sum::<usize>());
    body.extend_from_slice(b"{\"responses\":[");
    for (i, entry) in responses.iter().enumerate() {
        if i > 0 {
            body.push(b',');
        }
        body.extend_from_slice(entry);
    }
    body.extend_from_slice(b"]}");
    body
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
) -> Result<Prepared, Vec<u8>> {
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
async fn run_all<S: Reader>(sink: &S, prepared: &[Option<Prepared>], responses: &mut [Vec<u8>]) {
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

/// Shapes one search outcome into its `responses[]` entry (raw JSON bytes): the
/// hits stripped to the client's logical view with a `status` added, or a
/// positioned error response.
fn shape_response(p: &Prepared, result: Result<SearchOutcome, SinkError>) -> Vec<u8> {
    let Ok(outcome) = result else {
        return error_response(502, "upstream_failed");
    };
    match shape_hits(&outcome.body, &p.logical_index, &p.partition, &p.shape) {
        Ok(shaped) => with_status(&shaped, outcome.status)
            .unwrap_or_else(|| error_response(502, "malformed_upstream")),
        Err(_) => error_response(502, "malformed_upstream"),
    }
}

/// Adds a top-level `status` to an already-shaped sub-response, keeping its other
/// keys (including a raw `aggregations`) as raw byte spans, never re-materialized.
/// `None` if the shaped body is not a JSON object.
fn with_status(shaped: &[u8], status: u16) -> Option<Vec<u8>> {
    let mut top: BTreeMap<String, Box<RawValue>> = serde_json::from_slice(shaped).ok()?;
    top.insert(
        "status".to_owned(),
        serde_json::value::to_raw_value(&status).ok()?,
    );
    serde_json::to_vec(&top).ok()
}

/// A per-search error entry (a value-free error type and a status) as raw bytes.
fn error_response(status: u16, error: &'static str) -> Vec<u8> {
    serde_json::to_vec(&json!({ "error": { "type": error }, "status": status }))
        .unwrap_or_else(|_| br#"{"error":{"type":"internal"},"status":500}"#.to_vec())
}
