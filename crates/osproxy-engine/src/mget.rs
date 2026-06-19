//! Multi-get (`_mget`) demux: the read counterpart of `_bulk` (`docs/04` §5).
//!
//! One `_mget` body fetches many documents that may live in **different logical
//! indices → different targets**. The partition is the caller's (one principal →
//! one partition), so we resolve it once, then per requested document resolve its
//! placement (cached per logical index), map the logical id to the physical id,
//! and issue the `get` — **bounded-concurrently**, like the bulk dispatch. The
//! per-document results are re-interleaved into `docs[]` in the body's original
//! order and each shaped back into the client's logical view (injected tenancy
//! fields stripped, physical id mapped back). A per-document failure is
//! positioned in place; the request as a whole still returns 200.

use std::collections::HashMap;

use futures_util::stream::StreamExt as _;
use osproxy_rewrite::{parse_mget, MgetItem};
use osproxy_sink::{ReadOp, ReadOutcome, Reader, SinkError};
use osproxy_spi::RequestCtx;
use osproxy_tenancy::{Resolved, Router};
use serde_json::{json, Value};

use crate::error::RequestError;
use crate::pipeline::PipelineResponse;
use crate::read::{build_read_op, not_found_body, shape_found, ReadShape};
use crate::retry::with_retry;

/// The most concurrent in-flight upstream gets, bounding a wide `_mget` fan-out
/// just as the bulk dispatch bounds its per-target writes (NFR-P, `docs/04` §5).
const MAX_FETCH_CONCURRENCY: usize = 8;

/// Runs an `_mget` request: parse, per-doc resolve, concurrent fetch, re-interleave.
///
/// # Errors
///
/// Returns [`RequestError::Rewrite`] if the body is unparseable, or
/// [`RequestError::Spi`] if the caller's partition cannot be resolved (a
/// request-level failure). Per-document failures are reported positionally in
/// `docs[]`, not as a request error.
pub(crate) async fn multi_get<R: Router, S: Reader>(
    router: &R,
    sink: &S,
    ctx: &RequestCtx<'_>,
    retry: crate::RetryPolicy,
) -> Result<PipelineResponse, RequestError> {
    let items = parse_mget(ctx.body())?;
    let n = items.len();

    // The partition is the caller's, resolved once (an `_mget` carries no
    // per-doc source to resolve from). A failure here is request-level.
    let partition = router.resolve_partition(ctx, None)?;

    let mut docs: Vec<Value> = vec![Value::Null; n];
    let mut prepared: Vec<Option<Prepared>> = (0..n).map(|_| None).collect();
    let mut cache: HashMap<String, Resolved> = HashMap::new();

    for (ordinal, item) in items.into_iter().enumerate() {
        match prepare(router, ctx, &partition, &mut cache, &item, retry).await {
            Ok(p) => prepared[ordinal] = Some(p),
            Err(line) => docs[ordinal] = line,
        }
    }

    fetch_all(sink, &prepared, &mut docs).await;

    let body = json!({ "docs": docs });
    Ok(PipelineResponse {
        status: 200,
        body: serde_json::to_vec(&body).map_err(|_| RequestError::Internal {
            reason: "serializing mget response",
        })?,
    })
}

/// A prepared fetch: the read op plus what the response doc needs to be shaped.
struct Prepared {
    op: ReadOp,
    shape: ReadShape,
    logical_index: String,
    logical_id: String,
}

/// Prepares one requested document: resolve its placement (cached per logical
/// index), then build the read op mapping the logical id to the physical id.
async fn prepare<R: Router>(
    router: &R,
    ctx: &RequestCtx<'_>,
    partition: &osproxy_core::PartitionId,
    cache: &mut HashMap<String, Resolved>,
    item: &MgetItem,
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
        .map_err(|_| error_doc(&logical_index, &item.id, "placement_missing"))?;
        cache.insert(logical_index.clone(), r.clone());
        r
    };

    let (op, shape) = build_read_op(&resolved, &item.id)
        .map_err(|_| error_doc(&logical_index, &item.id, "irreversible_id"))?;
    Ok(Prepared {
        op: op.with_trace(Some(crate::endpoints::wire_trace(ctx))),
        shape,
        logical_index,
        logical_id: item.id.clone(),
    })
}

/// Issues the prepared gets **concurrently** (bounded) and fills `docs` by
/// ordinal, so completion order cannot disturb the re-interleave.
async fn fetch_all<S: Reader>(sink: &S, prepared: &[Option<Prepared>], docs: &mut [Value]) {
    // Collect the owned ops up front so the in-flight futures borrow only `&S`
    // (a reference into `prepared` in the async block defeats lifetime inference).
    let ops: Vec<(usize, ReadOp)> = prepared
        .iter()
        .enumerate()
        .filter_map(|(ordinal, slot)| slot.as_ref().map(|p| (ordinal, p.op.clone())))
        .collect();

    let results: Vec<(usize, Result<ReadOutcome, SinkError>)> = futures_util::stream::iter(ops)
        .map(|(ordinal, op)| async move { (ordinal, sink.get(op).await) })
        .buffer_unordered(MAX_FETCH_CONCURRENCY)
        .collect()
        .await;

    for (ordinal, result) in results {
        if let Some(p) = prepared[ordinal].as_ref() {
            docs[ordinal] = shape_result(p, result);
        }
    }
}

/// Shapes one fetch outcome into its `docs[]` entry, in the client's logical
/// view: a hit is the stored document with injected fields stripped, a miss is a
/// `found: false` doc, and an upstream error is positioned as an error doc.
fn shape_result(p: &Prepared, result: Result<ReadOutcome, SinkError>) -> Value {
    match result {
        Ok(outcome) if outcome.found => {
            let shaped = shape_found(
                &outcome.body,
                &p.logical_index,
                &p.logical_id,
                &p.shape.inject_names,
            );
            shaped
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                .unwrap_or_else(|| error_doc(&p.logical_index, &p.logical_id, "malformed_upstream"))
        }
        Ok(_) => serde_json::from_slice(&not_found_body(&p.logical_index, &p.logical_id))
            .unwrap_or_else(|_| error_doc(&p.logical_index, &p.logical_id, "malformed_upstream")),
        Err(_) => error_doc(&p.logical_index, &p.logical_id, "upstream_failed"),
    }
}

/// A per-document error entry (logical index/id + a value-free error type).
fn error_doc(logical_index: &str, logical_id: &str, error: &'static str) -> Value {
    json!({
        "_index": logical_index,
        "_id": logical_id,
        "error": { "type": error },
    })
}
