//! The `_delete_by_query` async fan-out expansion (`docs/04` §9, ADR-010).
//!
//! Delete-by-query has no synchronous implementation in the proxy: it is a
//! query-driven mutation the fan-out queue cannot carry as a single op. In async
//! mode, with expansion opted in, the proxy instead runs the **partition-scoped**
//! query itself (the same mandatory isolation filter as a normal search), caps
//! the match set, and enqueues a concrete delete per matched physical id, so the
//! op stream stays self-contained and idempotent. Anything else is rejected:
//! sync mode, expansion disabled, no queue, or a match set over the cap.

use osproxy_observe::RequestTrace;
use osproxy_sink::{Reader, Sink, WriteBatch};
use osproxy_spi::RequestCtx;
use osproxy_tenancy::Router;
use serde_json::{json, Value};

use crate::asyncwrite::{
    op_id_for, unavailable_response, unsupported_response, QueuedWrite, WriteMode,
};
use crate::endpoints::wire_trace;
use crate::error::RequestError;
use crate::observe::resolve_info;
use crate::pipeline::{Pipeline, PipelineResponse};
use crate::read::{build_delete_op_physical, build_search_op};

/// The most documents one `_delete_by_query` may match before it is refused. A
/// single request must not expand into an unbounded number of enqueued deletes.
const DBQ_MAX_MATCHES: u64 = 10_000;

impl<R: Router, S: Sink + Reader> Pipeline<R, S> {
    /// Runs the `_delete_by_query` expansion (`docs/04` §9). See the module docs.
    pub(crate) async fn delete_by_query(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let index = ctx.logical_index();
        if self.write_mode(ctx) != WriteMode::Async {
            return Ok(unsupported_response(
                "delete_by_query is only supported in async write mode",
                index,
            ));
        }
        if !self.delete_by_query_expansion {
            return Ok(unsupported_response(
                "delete_by_query expansion is not enabled on this proxy",
                index,
            ));
        }
        if !self.write_queue.enabled() {
            return Ok(unavailable_response(index));
        }

        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let doc = self.run_match_search(&resolved, ctx).await?;
        let total = doc["hits"]["total"]["value"].as_u64().unwrap_or(0);
        if total > DBQ_MAX_MATCHES {
            return Ok(unsupported_response(
                "delete_by_query match set exceeds the proxy cap",
                index,
            ));
        }
        let ids: Vec<String> = doc["hits"]["hits"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|hit| hit["_id"].as_str().map(str::to_owned))
            .collect();

        let (deleted, failures) = self.enqueue_deletes(&resolved, ctx, ids).await;

        // A delete-by-query-shaped acknowledgement: `deleted` counts what was
        // durably enqueued (not yet applied, async semantics), `total` what
        // matched. No version conflicts can arise at enqueue time.
        let body = json!({
            "took": 0,
            "timed_out": false,
            "total": total,
            "deleted": deleted,
            "version_conflicts": 0,
            "batches": 1,
            "failures": failures,
        });
        Ok(PipelineResponse {
            status: 200,
            body: serde_json::to_vec(&body).map_err(|_| RequestError::Internal {
                reason: "serializing delete-by-query response",
            })?,
            content_type: None,
        })
    }

    /// Runs the partition-scoped query (the same mandatory isolation filter as a
    /// normal search), capped and ids-only, and parses the hits envelope.
    async fn run_match_search(
        &self,
        resolved: &osproxy_tenancy::Resolved,
        ctx: &RequestCtx<'_>,
    ) -> Result<Value, RequestError> {
        let (mut search_op, _shape) = build_search_op(resolved, ctx.body())?;
        search_op.body = cap_ids_only(&search_op.body)?;
        let outcome = self
            .sink
            .search(search_op.with_trace(Some(wire_trace(ctx))))
            .await?;
        serde_json::from_slice(&outcome.body)
            .map_err(|_| osproxy_rewrite::RewriteError::InvalidJson.into())
    }

    /// Enqueues a concrete delete per matched physical id, keyed/ordered by
    /// partition, returning `(deleted, failures)`.
    async fn enqueue_deletes(
        &self,
        resolved: &osproxy_tenancy::Resolved,
        ctx: &RequestCtx<'_>,
        ids: Vec<String>,
    ) -> (u64, Vec<Value>) {
        let batch_id = op_id_for(ctx, ctx.request_id());
        let partition = resolved.partition.as_str().to_owned();
        let mut deleted = 0u64;
        let mut failures: Vec<Value> = Vec::new();
        for (i, physical_id) in ids.into_iter().enumerate() {
            let op = build_delete_op_physical(resolved, physical_id);
            let write = QueuedWrite {
                op_id: format!("{batch_id}:{i}"),
                partition_key: partition.clone(),
                batch: WriteBatch::single(op),
            };
            match self.write_queue.enqueue(write).await {
                Ok(()) => deleted += 1,
                Err(_) => failures.push(json!({ "status": 503, "type": "enqueue_failed" })),
            }
        }
        (deleted, failures)
    }
}

/// Caps a partition-filtered search body to at most `DBQ_MAX_MATCHES + 1` hits
/// (so the count can be detected as over the cap) and fetches ids only
/// (`_source: false`), with an accurate total. The query (and its mandatory
/// isolation filter) is preserved untouched.
fn cap_ids_only(body: &[u8]) -> Result<Vec<u8>, RequestError> {
    let mut doc: Value =
        serde_json::from_slice(body).map_err(|_| osproxy_rewrite::RewriteError::InvalidJson)?;
    let obj = doc.as_object_mut().ok_or(RequestError::Internal {
        reason: "search body is not an object",
    })?;
    obj.insert("size".to_owned(), json!(DBQ_MAX_MATCHES + 1));
    obj.insert("_source".to_owned(), json!(false));
    obj.insert("track_total_hits".to_owned(), json!(true));
    serde_json::to_vec(&doc).map_err(|_| RequestError::Internal {
        reason: "serializing delete-by-query search body",
    })
}
