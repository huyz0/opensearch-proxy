//! Administrative pass-through (`docs/03` §6), `_cat`/`_cluster`/`_nodes`.
//!
//! These endpoints carry no tenancy semantics, so the proxy cannot filter or
//! strip them; per `docs/decisions/006` and `docs/specs/opensearch-endpoints.md`
//! the only safe choices are **reject** (the default) or **pass through to an
//! operator-allow-listed cluster**, with the operator accepting that admin
//! output is cluster-wide (not tenant-scoped). The policy is opt-in: without one,
//! every admin request is rejected exactly like an unsupported endpoint.

use osproxy_core::ClusterId;
use osproxy_observe::{DispatchInfo, RequestTrace};
use osproxy_sink::{CursorOp, Reader, Sink};
use osproxy_spi::{RequestCtx, SpiError};
use osproxy_tenancy::Router;

use crate::endpoints::wire_trace;
use crate::error::RequestError;
use crate::pipeline::{Pipeline, PipelineResponse};

/// The operator's admin pass-through policy: the cluster that answers admin
/// requests and the path prefixes permitted through. A request whose path does
/// not match an allowed prefix is rejected, so enabling pass-through for
/// `_cat/health` does not silently open `_cluster/settings`.
#[derive(Clone, Debug)]
pub struct AdminPolicy {
    cluster: ClusterId,
    allowed_prefixes: Vec<String>,
    endpoint: Option<String>,
}

impl AdminPolicy {
    /// A policy forwarding any path matching one of `allowed_prefixes` to
    /// `cluster`. Prefixes are matched against the raw request path (e.g.
    /// `/_cat/`, `/_cluster/health`); an empty list allows nothing.
    #[must_use]
    pub fn new(cluster: ClusterId, allowed_prefixes: Vec<String>) -> Self {
        Self {
            cluster,
            allowed_prefixes,
            endpoint: None,
        }
    }

    /// Sets the admin cluster's base URL (builder style). The admin cluster is
    /// operator infrastructure, not a tenancy placement, so its endpoint is given
    /// here; without it the sink falls back to the tenancy's `cluster_endpoint`.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Whether `path` is allow-listed for pass-through. A path containing a `..`
    /// segment is never allowed: the prefix is an authorization boundary, and
    /// upstream `..` resolution would otherwise let `/_cat/../_cluster/settings`
    /// slip past a `/_cat/`-only allow-list.
    #[must_use]
    fn allows(&self, path: &str) -> bool {
        if path.split('/').any(|seg| seg == "..") {
            return false;
        }
        self.allowed_prefixes.iter().any(|p| path.starts_with(p))
    }
}

impl<R: Router, S: Sink + Reader> Pipeline<R, S> {
    /// Forwards an allow-listed admin request verbatim to the policy's cluster,
    /// or rejects it (the default when no policy is configured, and for any path
    /// not on the allow-list). Admin output is not tenancy-filtered, so the full
    /// path and query are forwarded, there is no body partition filter to bypass.
    pub(crate) async fn admin(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let Some(policy) = self.admin_policy.as_ref().filter(|p| p.allows(ctx.path())) else {
            // No policy, or the path is not allow-listed: reject like an
            // unsupported endpoint (fail-closed, `docs/decisions/006`).
            return Err(RequestError::Spi(SpiError::UnsupportedEndpoint {
                endpoint: ctx.endpoint(),
            }));
        };
        // The admin cluster's endpoint: the operator-supplied one, else the
        // tenancy's lookup for that cluster id.
        let endpoint = policy
            .endpoint
            .clone()
            .or_else(|| self.router.cluster_endpoint(&policy.cluster));
        let op = CursorOp::new(
            policy.cluster.clone(),
            ctx.method(),
            ctx.path().to_owned(),
            ctx.body().to_vec(),
        )
        .with_endpoint(endpoint)
        .with_query(ctx.query().map(str::to_owned))
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
            // Admin output (`_cat` etc.) is often `text/plain`; forward the
            // upstream content type rather than mislabeling it `application/json`.
            content_type: outcome.content_type,
        })
    }
}
