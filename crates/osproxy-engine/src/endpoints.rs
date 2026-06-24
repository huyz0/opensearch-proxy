//! The per-endpoint handlers the [`Pipeline`] dispatches to.
//!
//! Each method runs one classified request to completion, resolve, transform,
//! dispatch, reverse-transform, recording the per-stage shape-only spans into
//! the request trace. The orchestration (classification, trace assembly, the
//! `/debug/explain` store) lives in [`crate::pipeline`]; this module is the body
//! of each endpoint, kept separate so neither file becomes a god module.
//
// JUSTIFY(file-length): one cohesive family, the per-endpoint handler bodies the
// pipeline dispatches to (ingest/get/delete/search/count/cursor) plus their two
// small response shapers. They share the resolve -> dispatch -> shape skeleton
// and the `wire_trace` helper; splitting them across files would scatter that one
// pattern for no readability gain. Tests live in `read_tests.rs`/`pipeline_tests.rs`.

use osproxy_core::{ClusterId, TraceContext};
use osproxy_observe::DispatchInfo;
use osproxy_sink::{CursorOp, Reader, Sink, WriteAck, WriteBatch};
use osproxy_spi::RequestCtx;
use osproxy_tenancy::{Resolved, Router};

use crate::asyncwrite::WriteMode;
use crate::cursor::{
    cursor_request, forwardable_query, has_scroll_id, pit_id_in_body, rewrite_cursor_body,
    wrap_scroll_id_in_response,
};
use crate::error::RequestError;
use crate::observe::{dispatch_info, read_dispatch_info, resolve_info, rewrite_info};
use crate::pipeline::{Pipeline, PipelineResponse};
use crate::plan::build_write_batch;
use crate::read::{
    build_delete_op, build_read_op, build_search_op, not_found_body, shape_delete, shape_found,
    shape_hits,
};
use crate::retry::with_retry;
use osproxy_observe::RequestTrace;

impl<R: Router, S: Sink + Reader> Pipeline<R, S> {
    /// The single-document ingest path (`docs/04` §2).
    pub(crate) async fn ingest_doc(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let batch = build_write_batch(&resolved, ctx.body())?;
        trace.record_rewrite(rewrite_info(&resolved, &batch));

        if self.write_mode(ctx) == WriteMode::Async {
            return Ok(self.enqueue_async(ctx, &resolved, batch).await);
        }

        self.gate_write(&resolved).await?;
        let up_trace = self.upstream_trace(ctx);
        let ack = self
            .sink
            .write(
                batch
                    .with_trace(up_trace.as_ref())
                    .with_forward_headers(ctx.forward_headers()),
            )
            .await?;
        trace.record_dispatch(dispatch_info(&resolved, &ack));
        Ok(response_for(&resolved, &ack))
    }

    /// Resolves `ctx`'s routing, retrying a momentarily-unavailable placement
    /// backend with bounded backoff (`docs/06` §3a) before surfacing the error.
    pub(crate) async fn resolve_with_retry(
        &self,
        ctx: &RequestCtx<'_>,
    ) -> Result<Resolved, RequestError> {
        with_retry(self.retry, || self.router.resolve(ctx))
            .await
            .map_err(Into::into)
    }

    /// The migration write gate (`docs/06` §2), applied at dispatch after the
    /// decision was stamped: if the partition's placement advanced (or entered
    /// cutover) in the meantime, hold the write with a retryable stale-epoch
    /// error so the client re-resolves rather than committing to the wrong place.
    async fn gate_write(&self, resolved: &Resolved) -> Result<(), RequestError> {
        let epoch = resolved.decision.epoch;
        if self.router.admit_write(&resolved.partition, epoch).await {
            Ok(())
        } else {
            Err(RequestError::StaleEpoch { stamped: epoch })
        }
    }

    /// The bulk-ingest path (`docs/04` §3): parse the NDJSON body, demux the
    /// operations by target, dispatch, and re-interleave the per-item results.
    ///
    /// Bulk spans many partitions/targets, so the per-operation outcome lives
    /// positionally in the response body rather than in a single resolve/dispatch
    /// span; `handle` still records the classify and egress shapes.
    pub(crate) async fn ingest_bulk(
        &self,
        ctx: &RequestCtx<'_>,
        _trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        if self.write_mode(ctx) == WriteMode::Async {
            return crate::bulk::ingest_bulk_async(
                &self.router,
                self.write_queue.as_ref(),
                ctx,
                self.retry,
                self.upstream_trace(ctx),
            )
            .await;
        }
        crate::bulk::ingest_bulk(
            &self.router,
            &self.sink,
            ctx,
            self.retry,
            self.upstream_trace(ctx),
        )
        .await
    }

    /// The get-by-id read path (`docs/04` §5): resolve the partition, map the
    /// client's logical id to the physical id, fetch it, and shape the stored
    /// document back into the client's logical view (injected fields stripped).
    pub(crate) async fn get_by_id(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let logical_id = ctx.doc_id().ok_or(RequestError::Internal {
            reason: "get-by-id reached the engine without a document id",
        })?;
        let (read_op, shape) = build_read_op(&resolved, logical_id)?;

        let outcome = self
            .sink
            .get(
                read_op
                    .with_trace(self.upstream_trace(ctx))
                    .with_forward_headers(ctx.forward_headers().to_vec()),
            )
            .await?;
        trace.record_dispatch(read_dispatch_info(
            &resolved,
            outcome.status,
            outcome.pool_reuse,
        ));

        if outcome.found {
            let body = shape_found(
                &outcome.body,
                ctx.logical_index(),
                logical_id,
                &shape.inject_names,
            )?;
            Ok(PipelineResponse {
                status: 200,
                body,
                content_type: None,
            })
        } else {
            Ok(PipelineResponse {
                status: 404,
                body: not_found_body(ctx.logical_index(), logical_id),
                content_type: None,
            })
        }
    }

    /// The multi-get path (`docs/04` §5): the read counterpart of `_bulk`.
    /// Resolves the caller's partition once, then per requested document resolves
    /// its placement, maps the logical id to the physical id, fetches it
    /// (bounded-concurrently), and re-interleaves the shaped docs in input order.
    ///
    /// Like bulk, the per-document outcome is positional in the body, so no
    /// single resolve/dispatch span is recorded; classify and egress still are.
    pub(crate) async fn multi_get(
        &self,
        ctx: &RequestCtx<'_>,
        _trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        crate::mget::multi_get(
            &self.router,
            &self.sink,
            ctx,
            self.retry,
            self.upstream_trace(ctx),
        )
        .await
    }

    /// The delete-by-id path (`docs/04` §5): resolve the partition, map the
    /// client's logical id to the physical id, and issue an epoch-stamped delete
    /// to the single target. The response is shaped back to the logical id/index.
    pub(crate) async fn delete_by_id(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let logical_id = ctx.doc_id().ok_or(RequestError::Internal {
            reason: "delete-by-id reached the engine without a document id",
        })?;
        let op = build_delete_op(&resolved, logical_id)?;

        if self.write_mode(ctx) == WriteMode::Async {
            let batch = WriteBatch::single(op);
            return Ok(self.enqueue_async(ctx, &resolved, batch).await);
        }

        self.gate_write(&resolved).await?;
        let up_trace = self.upstream_trace(ctx);
        let ack = self
            .sink
            .write(
                WriteBatch::single(op)
                    .with_trace(up_trace.as_ref())
                    .with_forward_headers(ctx.forward_headers()),
            )
            .await?;
        trace.record_dispatch(dispatch_info(&resolved, &ack));

        let status = ack.results().first().map_or(200, |r| r.status);
        Ok(PipelineResponse {
            status,
            body: shape_delete(ctx.logical_index(), logical_id, status),
            content_type: None,
        })
    }

    /// The search/read path (`docs/04` §4): resolve the partition, wrap the
    /// client query in the mandatory partition filter, dispatch to the single
    /// target, and strip the injected tenancy fields from every hit so the
    /// client sees only its own logical documents.
    pub(crate) async fn search(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        // A search pinned to a point-in-time routes to the PIT's cluster, but
        // still applies the partition filter + field strip (isolation, NFR-S4).
        if self.cursor_signer.is_some() {
            if let Some(wrapped) = pit_id_in_body(ctx.body()) {
                return self.pit_search(ctx, trace, &wrapped).await;
            }
        }
        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let (search_op, shape) = build_search_op(&resolved, ctx.body())?;
        let outcome = self
            .sink
            .search(
                search_op
                    // Forward only allow-listed cursor params (e.g. `scroll=1m`)
                    // so a scroll-opening search actually opens one upstream.
                    .with_query(forwardable_query(ctx.query()))
                    .with_trace(self.upstream_trace(ctx))
                    .with_forward_headers(ctx.forward_headers().to_vec()),
            )
            .await?;
        trace.record_dispatch(read_dispatch_info(
            &resolved,
            outcome.status,
            outcome.pool_reuse,
        ));

        let body = shape_hits(
            &outcome.body,
            ctx.logical_index(),
            resolved.partition.as_str(),
            &shape,
        )?;
        // If this search opened a scroll, its response carries a `_scroll_id`;
        // wrap it with the resolved cluster so the continue lands on the same
        // place (`docs/03` §6). A plain search has none, so this is a no-op.
        let body = self.wrap_scroll_id(body, &resolved.decision.target.cluster);
        Ok(PipelineResponse {
            status: outcome.status,
            body,
            content_type: None,
        })
    }

    /// Wraps a `_scroll_id` in a search response with `cluster` when cursor
    /// affinity is enabled, so a continued scroll returns to the same cluster. A
    /// response without a `_scroll_id`, or affinity off, is returned unchanged,
    /// and the cheap byte pre-check skips the JSON parse for plain searches.
    fn wrap_scroll_id(&self, body: Vec<u8>, cluster: &ClusterId) -> Vec<u8> {
        let Some(signer) = &self.cursor_signer else {
            return body;
        };
        if !has_scroll_id(&body) {
            return body;
        }
        wrap_scroll_id_in_response(body, signer.as_ref(), cluster)
    }

    /// The multi-search path (`docs/04` §4): the search counterpart of `_bulk`.
    /// Resolves the caller's partition once, then per search resolves its
    /// placement, wraps the client query in the mandatory partition filter, runs
    /// it (bounded-concurrently), and re-interleaves the stripped responses in
    /// input order. Per-search outcome is positional in the body, so no single
    /// resolve/dispatch span is recorded; classify and egress still are.
    pub(crate) async fn multi_search(
        &self,
        ctx: &RequestCtx<'_>,
        _trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        crate::msearch::multi_search(
            &self.router,
            &self.sink,
            ctx,
            self.retry,
            self.upstream_trace(ctx),
        )
        .await
    }

    /// The count path (`docs/04` §4): same mandatory partition filter as search,
    /// but the upstream returns only a total, so there is nothing to strip, the
    /// count is already scoped to the caller's partition.
    pub(crate) async fn count(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let (search_op, _shape) = build_search_op(&resolved, ctx.body())?;
        let outcome = self
            .sink
            .count(
                search_op
                    .with_trace(self.upstream_trace(ctx))
                    .with_forward_headers(ctx.forward_headers().to_vec()),
            )
            .await?;
        trace.record_dispatch(read_dispatch_info(
            &resolved,
            outcome.status,
            outcome.pool_reuse,
        ));

        let body = format!(r#"{{"count":{}}}"#, outcome.count).into_bytes();
        Ok(PipelineResponse {
            status: outcome.status,
            body,
            content_type: None,
        })
    }

    /// The cursor (scroll/PIT) continue/clear path (`docs/03` §6): recover the
    /// pinned cluster from the request's signed affinity envelope and forward the
    /// raw op there, **bypassing partition resolution**. Fails closed with
    /// `CursorUnresolvable` when affinity is off or the envelope does not verify,
    /// never a blind cross-cluster dispatch.
    pub(crate) async fn cursor(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let Some(signer) = &self.cursor_signer else {
            return Err(RequestError::Cursor {
                reason: "cursor affinity is not enabled",
            });
        };
        // A cursor request with a logical index is a PIT create
        // (`/{index}/_search/point_in_time`): it resolves the index's cluster and
        // wraps the returned `pit_id`, rather than recovering a cluster from an
        // existing cursor.
        if !ctx.logical_index().is_empty() {
            return self.pit_create(ctx, trace).await;
        }
        // A `pit_id` array in the body is a PIT close (`DELETE
        // /_search/point_in_time`): each id may pin a different cluster, so it is
        // grouped and fanned out rather than routed as one scroll cursor.
        if let Some(pit_ids) = crate::cursor::pit_ids_in_delete_body(ctx.body()) {
            return self.pit_delete(ctx, trace, &pit_ids).await;
        }
        let req = cursor_request(ctx).ok_or(RequestError::Cursor {
            reason: "no cursor id in the request",
        })?;
        let (cluster, real_id) = osproxy_core::cursor::unwrap(signer.as_ref(), &req.wrapped)
            .ok_or(RequestError::Cursor {
                reason: "cursor envelope is invalid or expired",
            })?;
        // Forward the body form upstream with the real id substituted, so a large
        // cursor id never rides in a URL path (`docs/03` §6).
        let body = rewrite_cursor_body(ctx.body(), req.id_field, &real_id);
        let op = CursorOp::new(cluster.clone(), ctx.method(), req.upstream_path, body)
            .with_endpoint(self.router.cluster_endpoint(&cluster))
            .with_trace(self.upstream_trace(ctx))
            .with_forward_headers(ctx.forward_headers().to_vec());
        let outcome = self.sink.cursor(op).await?;
        // A scroll continue's response carries the *next* page's `_scroll_id`;
        // re-wrap it with the same cluster so the client's next continue verifies
        // (`docs/03` §6). PIT close responses carry none, so this is a no-op there.
        let resp_body = self.wrap_scroll_id(outcome.body, &cluster);
        trace.record_dispatch(DispatchInfo {
            cluster,
            upstream_status: outcome.status,
            pool_reuse: outcome.pool_reuse,
        });
        Ok(PipelineResponse {
            status: outcome.status,
            body: resp_body,
            content_type: None,
        })
    }
}

/// The W3C trace context to forward to the upstream for this request: continues
/// the client's incoming `traceparent` (keeping the trace connected end-to-end),
/// falls back to a **B3** context (Zipkin/Istio) when only that is present, or
/// mints a new root when neither is, always with a fresh span id for the proxy's
/// hop (`docs/05` §2). Pure identity, never carries request values.
pub(crate) fn wire_trace(ctx: &RequestCtx<'_>) -> TraceContext {
    let b3 = b3_single(ctx);
    TraceContext::propagate_with_b3(
        ctx.headers().get("traceparent"),
        ctx.headers().get("tracestate"),
        b3.as_deref(),
        ctx.request_id(),
    )
}

/// The caller's B3 context as a single-header value: the `b3` header if present,
/// else assembled from the multi-header `X-B3-TraceId`/`X-B3-SpanId`/`X-B3-Sampled`
/// form so both B3 shapes continue the trace. `None` if no B3 trace+span is given.
fn b3_single(ctx: &RequestCtx<'_>) -> Option<String> {
    let h = ctx.headers();
    if let Some(single) = h.get("b3") {
        return Some(single.to_owned());
    }
    let trace = h.get("x-b3-traceid")?;
    let span = h.get("x-b3-spanid")?;
    let mut out = format!("{trace}-{span}");
    if let Some(sampled) = h.get("x-b3-sampled") {
        out.push('-');
        out.push_str(sampled);
    }
    Some(out)
}

/// Shapes a write acknowledgement into an OpenSearch-style ingest response.
///
/// For a single-document write the ack carries one result; its status and the
/// created/updated outcome are surfaced as the client would expect, with the
/// `_id` mapped back to the client's logical id (`docs/03` §4). The body is built
/// with `serde_json` so an id carrying JSON-special characters is escaped, never
/// hand-interpolated.
fn response_for(resolved: &Resolved, ack: &WriteAck) -> PipelineResponse {
    let Some(result) = ack.results().first() else {
        // No operations is not reachable from the single-doc path, but never
        // panic: report an empty 200 rather than unwrapping (NFR-R1).
        return PipelineResponse {
            status: 200,
            body: b"{}".to_vec(),
            content_type: None,
        };
    };
    let outcome = if result.created { "created" } else { "updated" };
    let logical_id = crate::read::logical_write_id(resolved, &result.id);
    let body = serde_json::to_vec(&serde_json::json!({
        "_id": logical_id,
        "result": outcome,
    }))
    .unwrap_or_else(|_| b"{}".to_vec());
    PipelineResponse {
        status: result.status,
        body,
        content_type: None,
    }
}
