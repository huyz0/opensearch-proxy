//! Point-in-time (PIT) cursor handlers (`docs/03` §6), the search and create
//! paths, split from [`crate::endpoints`] to keep that module within budget.
//!
//! Unlike a scroll continue (a pure passthrough), a PIT search must **resolve the
//! partition to apply the mandatory filter and strip the injected fields** even
//! while pinning the PIT's cluster, pinning must never bypass tenant isolation
//! (NFR-S4).

use osproxy_observe::{DispatchInfo, RequestTrace};
use osproxy_sink::{CursorOp, Reader, Sink};
use osproxy_spi::RequestCtx;
use osproxy_tenancy::Router;

use std::collections::BTreeMap;

use osproxy_core::ClusterId;

use crate::cursor::{forwardable_query, rewrite_pit_id, wrap_pit_id_in_response};
use crate::endpoints::wire_trace;
use crate::error::RequestError;
use crate::observe::resolve_info;
use crate::pipeline::{Pipeline, PipelineResponse};
use crate::read::{build_search_op, shape_hits};

impl<R: Router, S: Sink + Reader> Pipeline<R, S> {
    /// A point-in-time search: route to the PIT's pinned cluster (recovered from
    /// the body's signed `pit.id`), but **still resolve the partition to apply the
    /// mandatory filter and strip the injected fields** (isolation, NFR-S4). Fails
    /// closed if the PIT envelope does not verify.
    pub(crate) async fn pit_search(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
        wrapped_pit: &str,
    ) -> Result<PipelineResponse, RequestError> {
        let signer = self.cursor_signer.as_ref().ok_or(RequestError::Cursor {
            reason: "cursor affinity is not enabled",
        })?;
        let (cluster, real_pit) = osproxy_core::cursor::unwrap(signer.as_ref(), wrapped_pit)
            .ok_or(RequestError::Cursor {
                reason: "pit envelope is invalid or expired",
            })?;
        // Resolve for the partition filter + strip shape (isolation still applies).
        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));
        let (search_op, shape) = build_search_op(&resolved, ctx.body())?;
        // The filtered body still carries the wrapped pit id, substitute the real
        // one, then route to the PIT's cluster (not the resolved target).
        let body = rewrite_pit_id(search_op.body, &real_pit);
        let op = CursorOp::new(cluster.clone(), ctx.method(), "/_search", body)
            .with_endpoint(self.router.cluster_endpoint(&cluster))
            .with_trace(Some(wire_trace(ctx)));
        let outcome = self.sink.cursor(op).await?;
        trace.record_dispatch(DispatchInfo {
            cluster: cluster.clone(),
            upstream_status: outcome.status,
            pool_reuse: outcome.pool_reuse,
        });
        // Strip the injected tenancy fields from every hit, same as any search.
        let stripped = shape_hits(
            &outcome.body,
            ctx.logical_index(),
            resolved.partition.as_str(),
            &shape,
        )?;
        // OpenSearch echoes a refreshed `pit_id` on every PIT search; re-wrap it
        // with the same cluster so a client that follows the returned id (the
        // documented pattern) still routes back here (`docs/03` §6).
        let body = wrap_pit_id_in_response(stripped, signer.as_ref(), &cluster);
        Ok(PipelineResponse {
            status: outcome.status,
            body,
            content_type: None,
        })
    }

    /// A point-in-time create (`POST /{index}/_search/point_in_time`): resolve the
    /// index's cluster (like a search), open the PIT there, and **wrap the returned
    /// `pit_id`** with that cluster so later PIT searches/closes route back to it
    /// (`docs/03` §6).
    pub(crate) async fn pit_create(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));
        let target = &resolved.decision.target;
        let op = CursorOp::new(
            target.cluster.clone(),
            ctx.method(),
            format!("/{}/_search/point_in_time", target.index.as_str()),
            ctx.body().to_vec(),
        )
        // PIT create resolved a placement, so the endpoint rides on its target.
        .with_endpoint(target.endpoint.clone())
        // Forward `keep_alive` (allow-listed) so the PIT gets the requested TTL.
        .with_query(forwardable_query(ctx.query()))
        .with_trace(Some(wire_trace(ctx)));
        let outcome = self.sink.cursor(op).await?;
        let body = match &self.cursor_signer {
            Some(signer) => wrap_pit_id_in_response(outcome.body, signer.as_ref(), &target.cluster),
            None => outcome.body,
        };
        trace.record_dispatch(DispatchInfo {
            cluster: target.cluster.clone(),
            upstream_status: outcome.status,
            pool_reuse: outcome.pool_reuse,
        });
        Ok(PipelineResponse {
            status: outcome.status,
            body,
            content_type: None,
        })
    }

    /// A point-in-time close (`DELETE /_search/point_in_time`, body
    /// `{"pit_id":[<wrapped>, ...]}`): recover each id's pinned cluster from its
    /// signed envelope, group by cluster, and forward one close per cluster with
    /// the real ids, never a blind cross-cluster delete. Fails closed if any
    /// envelope does not verify. The per-cluster `pits[]` results are merged so
    /// the client sees one OpenSearch-shaped close response.
    pub(crate) async fn pit_delete(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
        wrapped_ids: &[String],
    ) -> Result<PipelineResponse, RequestError> {
        let signer = self.cursor_signer.as_ref().ok_or(RequestError::Cursor {
            reason: "cursor affinity is not enabled",
        })?;
        // Group the real ids by their pinned cluster; a foreign/forged envelope
        // fails the whole close closed rather than reaching the wrong cluster.
        let mut by_cluster: BTreeMap<ClusterId, Vec<String>> = BTreeMap::new();
        for wrapped in wrapped_ids {
            let (cluster, real) = osproxy_core::cursor::unwrap(signer.as_ref(), wrapped).ok_or(
                RequestError::Cursor {
                    reason: "pit envelope is invalid or expired",
                },
            )?;
            by_cluster.entry(cluster).or_default().push(real);
        }
        let mut merged_pits: Vec<serde_json::Value> = Vec::new();
        let mut status = 200;
        for (cluster, real_ids) in by_cluster {
            let body = serde_json::json!({ "pit_id": real_ids });
            let op = CursorOp::new(
                cluster.clone(),
                ctx.method(),
                "/_search/point_in_time",
                serde_json::to_vec(&body).unwrap_or_default(),
            )
            .with_endpoint(self.router.cluster_endpoint(&cluster))
            .with_trace(Some(wire_trace(ctx)));
            let outcome = self.sink.cursor(op).await?;
            if outcome.status >= 400 {
                status = outcome.status;
            }
            trace.record_dispatch(DispatchInfo {
                cluster,
                upstream_status: outcome.status,
                pool_reuse: outcome.pool_reuse,
            });
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&outcome.body) {
                if let Some(pits) = v.get("pits").and_then(serde_json::Value::as_array) {
                    merged_pits.extend(pits.iter().cloned());
                }
            }
        }
        let body = serde_json::to_vec(&serde_json::json!({ "pits": merged_pits }))
            .unwrap_or_else(|_| br#"{"pits":[]}"#.to_vec());
        Ok(PipelineResponse {
            status,
            body,
            content_type: None,
        })
    }
}
