//! Tenant-agnostic passthrough: forward a request verbatim to one cluster.
//!
//! When a [`PassthroughPolicy`] is set, the pipeline skips tenancy entirely
//! (no partition resolve, no body rewrite, no isolation) and forwards the raw
//! request to the configured cluster, returning the upstream response unchanged.
//! This is the transparent-proxy mode a capture/migration proxy runs in.
//!
//! It reuses the same verbatim-forward primitive the admin and cursor paths use
//! (a [`CursorOp`]): method, path, body, and query go upstream as-is, and the
//! response comes back untouched. The forward still flows through the pipeline's
//! trace, metrics, and pooling, so observability and connection reuse are intact.

use osproxy_core::ClusterId;
use osproxy_observe::{DispatchInfo, RequestTrace};
use osproxy_sink::{CursorOp, Reader, Sink};
use osproxy_tenancy::Router;

use crate::endpoints::wire_trace;
use crate::error::RequestError;
use crate::pipeline::{Pipeline, PipelineResponse};
use osproxy_spi::RequestCtx;

/// Where a passthrough proxy forwards every request: one cluster and its base URL.
#[derive(Clone, Debug)]
pub struct PassthroughPolicy {
    /// The cluster every request is forwarded to.
    pub cluster: ClusterId,
    /// The cluster's base URL (the sink pools it like any endpoint).
    pub endpoint: Option<String>,
}

impl PassthroughPolicy {
    /// A policy forwarding every request to `cluster` at `endpoint`.
    #[must_use]
    pub fn new(cluster: ClusterId, endpoint: impl Into<String>) -> Self {
        Self {
            cluster,
            endpoint: Some(endpoint.into()),
        }
    }
}

impl<R: Router, S: Sink + Reader> Pipeline<R, S> {
    /// Forwards `ctx` verbatim to the passthrough cluster and returns the raw
    /// upstream response. Reuses the cursor verbatim-forward op; the sink guards
    /// the path against traversal at the same choke point as admin/cursor.
    pub(crate) async fn forward(
        &self,
        ctx: &RequestCtx<'_>,
        policy: &PassthroughPolicy,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let op = CursorOp::new(
            policy.cluster.clone(),
            ctx.method(),
            ctx.path().to_owned(),
            ctx.body().to_vec(),
        )
        .with_endpoint(policy.endpoint.clone())
        .with_query(ctx.query().map(str::to_owned))
        .with_protocol(ctx.protocol())
        .with_trace(Some(wire_trace(ctx)));
        let outcome = self.sink.cursor(op).await?;
        trace.record_dispatch(DispatchInfo {
            cluster: policy.cluster.clone(),
            upstream_status: outcome.status,
            pool_reuse: outcome.pool_reuse,
        });
        Ok(PipelineResponse {
            status: outcome.status,
            body: outcome.body,
        })
    }
}
