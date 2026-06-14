//! The per-endpoint handlers the [`Pipeline`] dispatches to.
//!
//! Each method runs one classified request to completion — resolve, transform,
//! dispatch, reverse-transform — recording the per-stage shape-only spans into
//! the request trace. The orchestration (classification, trace assembly, the
//! `/debug/explain` store) lives in [`crate::pipeline`]; this module is the body
//! of each endpoint, kept separate so neither file becomes a god module.

use osproxy_sink::{Reader, Sink, WriteAck, WriteBatch};
use osproxy_spi::{RequestCtx, TenancySpi};

use crate::error::RequestError;
use crate::observe::{dispatch_info, read_dispatch_info, resolve_info, rewrite_info};
use crate::pipeline::{Pipeline, PipelineResponse};
use crate::plan::build_write_batch;
use crate::read::{
    build_delete_op, build_read_op, build_search_op, not_found_body, shape_delete, shape_found,
    shape_hits,
};
use osproxy_observe::RequestTrace;

impl<T: TenancySpi, S: Sink + Reader> Pipeline<T, S> {
    /// The single-document ingest path (`docs/04` §2).
    pub(crate) async fn ingest_doc(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.router.resolve(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let batch = build_write_batch(&resolved, ctx.body())?;
        trace.record_rewrite(rewrite_info(&resolved, &batch));

        let ack = self.sink.write(batch).await?;
        trace.record_dispatch(dispatch_info(&resolved, &ack));
        Ok(response_for(&ack))
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
        crate::bulk::ingest_bulk(&self.router, &self.sink, ctx).await
    }

    /// The get-by-id read path (`docs/04` §5): resolve the partition, map the
    /// client's logical id to the physical id, fetch it, and shape the stored
    /// document back into the client's logical view (injected fields stripped).
    pub(crate) async fn get_by_id(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.router.resolve(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let logical_id = ctx.doc_id().ok_or(RequestError::Internal {
            reason: "get-by-id reached the engine without a document id",
        })?;
        let (read_op, shape) = build_read_op(&resolved, logical_id)?;

        let outcome = self.sink.get(read_op).await?;
        trace.record_dispatch(read_dispatch_info(&resolved, outcome.status));

        if outcome.found {
            let body = shape_found(
                &outcome.body,
                ctx.logical_index(),
                logical_id,
                &shape.inject_names,
            )?;
            Ok(PipelineResponse { status: 200, body })
        } else {
            Ok(PipelineResponse {
                status: 404,
                body: not_found_body(ctx.logical_index(), logical_id),
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
        crate::mget::multi_get(&self.router, &self.sink, ctx).await
    }

    /// The delete-by-id path (`docs/04` §5): resolve the partition, map the
    /// client's logical id to the physical id, and issue an epoch-stamped delete
    /// to the single target. The response is shaped back to the logical id/index.
    pub(crate) async fn delete_by_id(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.router.resolve(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let logical_id = ctx.doc_id().ok_or(RequestError::Internal {
            reason: "delete-by-id reached the engine without a document id",
        })?;
        let op = build_delete_op(&resolved, logical_id)?;

        let ack = self.sink.write(WriteBatch::single(op)).await?;
        trace.record_dispatch(dispatch_info(&resolved, &ack));

        let status = ack.results().first().map_or(200, |r| r.status);
        Ok(PipelineResponse {
            status,
            body: shape_delete(ctx.logical_index(), logical_id, status),
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
        let resolved = self.router.resolve(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let (search_op, shape) = build_search_op(&resolved, ctx.body())?;
        let outcome = self.sink.search(search_op).await?;
        trace.record_dispatch(read_dispatch_info(&resolved, outcome.status));

        let body = shape_hits(
            &outcome.body,
            ctx.logical_index(),
            resolved.partition.as_str(),
            &shape,
        )?;
        Ok(PipelineResponse {
            status: outcome.status,
            body,
        })
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
        crate::msearch::multi_search(&self.router, &self.sink, ctx).await
    }

    /// The count path (`docs/04` §4): same mandatory partition filter as search,
    /// but the upstream returns only a total, so there is nothing to strip — the
    /// count is already scoped to the caller's partition.
    pub(crate) async fn count(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.router.resolve(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let (search_op, _shape) = build_search_op(&resolved, ctx.body())?;
        let outcome = self.sink.count(search_op).await?;
        trace.record_dispatch(read_dispatch_info(&resolved, outcome.status));

        let body = format!(r#"{{"count":{}}}"#, outcome.count).into_bytes();
        Ok(PipelineResponse {
            status: outcome.status,
            body,
        })
    }
}

/// Shapes a write acknowledgement into an OpenSearch-style ingest response.
///
/// For a single-document write the ack carries one result; its status and the
/// created/updated outcome are surfaced as the client would expect.
fn response_for(ack: &WriteAck) -> PipelineResponse {
    let Some(result) = ack.results().first() else {
        // No operations is not reachable from the single-doc path, but never
        // panic: report an empty 200 rather than unwrapping (NFR-R1).
        return PipelineResponse {
            status: 200,
            body: b"{}".to_vec(),
        };
    };
    let outcome = if result.created { "created" } else { "updated" };
    let body = format!(r#"{{"_id":"{}","result":"{outcome}"}}"#, result.id).into_bytes();
    PipelineResponse {
        status: result.status,
        body,
    }
}
