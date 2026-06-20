//! Tenant-agnostic passthrough: forward a request verbatim to one cluster.
//!
//! When a [`PassthroughPolicy`] is set and [matches](PassthroughPolicy::matches)
//! a request, the pipeline skips tenancy entirely (no partition resolve, no body
//! rewrite, no isolation) and forwards the raw request to the configured cluster,
//! returning the upstream response unchanged.
//!
//! The match is **per request, by logical index**, so one proxy serves both
//! modes at once: list the indices that are not (yet) onboarded into tenancy and
//! those flow through verbatim, while everything else is tenant-isolated. This is
//! the composable migration shape — legacy indices pass through, tenanted indices
//! do not — not a global "isolation off" switch. It is **fail-closed**: an index
//! that does not match keeps full tenancy. Matching is on the operator-configured
//! index list only, never a client-supplied header, so a client cannot opt itself
//! out of isolation. An empty match list means *every* request passes through (the
//! whole-instance transparent/capture proxy).
//!
//! It reuses the same verbatim-forward primitive the admin and cursor paths use
//! (a [`CursorOp`]): method, path, body, and query go upstream as-is, and the
//! response comes back untouched. The forward still flows through the pipeline's
//! trace, metrics, and pooling, so observability and connection reuse are intact.

use osproxy_core::ClusterId;
use osproxy_observe::{DispatchInfo, RequestTrace};
use osproxy_sink::{CursorOp, ForwardOp, Reader, Sink, UpstreamBody};
use osproxy_tenancy::Router;

use crate::endpoints::wire_trace;
use crate::error::RequestError;
use crate::pipeline::{Pipeline, PipelineResponse};
use osproxy_spi::RequestCtx;

/// Where a passthrough proxy forwards a matching request: one cluster and its
/// base URL, plus the logical-index prefixes that select which requests pass
/// through verbatim (empty ⇒ all of them).
#[derive(Clone, Debug)]
pub struct PassthroughPolicy {
    /// The cluster a matching request is forwarded to.
    pub cluster: ClusterId,
    /// The cluster's base URL (the sink pools it like any endpoint).
    pub endpoint: Option<String>,
    /// Logical-index prefixes that route verbatim. Empty means *every* request
    /// passes through (whole-instance transparent proxy); non-empty means only
    /// requests whose logical index matches a prefix pass through, the rest stay
    /// tenant-isolated (fail-closed). Operator-configured, never client-supplied.
    index_prefixes: Vec<String>,
}

impl PassthroughPolicy {
    /// A policy forwarding *every* request to `cluster` at `endpoint` (the
    /// whole-instance transparent proxy). Add [`with_index_prefixes`] to pass
    /// through only selected indices and tenant-isolate the rest.
    ///
    /// [`with_index_prefixes`]: PassthroughPolicy::with_index_prefixes
    #[must_use]
    pub fn new(cluster: ClusterId, endpoint: impl Into<String>) -> Self {
        Self {
            cluster,
            endpoint: Some(endpoint.into()),
            index_prefixes: Vec::new(),
        }
    }

    /// Restricts passthrough to requests whose logical index starts with one of
    /// `prefixes`; all other requests keep full tenancy. An empty list (the
    /// default) passes everything through.
    #[must_use]
    pub fn with_index_prefixes(mut self, prefixes: Vec<String>) -> Self {
        self.index_prefixes = prefixes;
        self
    }

    /// Whether `ctx` should be forwarded verbatim. Matches when no prefixes are
    /// configured (whole-instance passthrough) or the request's logical index
    /// starts with a configured prefix; otherwise the request stays tenanted.
    #[must_use]
    pub fn matches(&self, ctx: &RequestCtx<'_>) -> bool {
        self.matches_index(ctx.logical_index())
    }

    /// Whether a request for `logical_index` should be forwarded verbatim. The
    /// body-free half of [`matches`](Self::matches), so the transport can decide
    /// to **stream** a passthrough request before buffering its body (ADR-014
    /// stage 2).
    #[must_use]
    pub fn matches_index(&self, logical_index: &str) -> bool {
        self.index_prefixes.is_empty()
            || self
                .index_prefixes
                .iter()
                .any(|p| logical_index.starts_with(p.as_str()))
    }

    /// The cluster + base URL a matching request forwards to.
    fn target(&self) -> (ClusterId, Option<String>) {
        (self.cluster.clone(), self.endpoint.clone())
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

    /// Forwards `ctx` verbatim with its body supplied as a **stream**, piped
    /// straight to the upstream without buffering (ADR-014 stage 2). The streaming
    /// twin of [`forward`](Self::forward): same destination and verbatim semantics,
    /// but the body never lands in memory. The response is still read buffered.
    pub(crate) async fn forward_stream(
        &self,
        ctx: &RequestCtx<'_>,
        policy: &PassthroughPolicy,
        body: UpstreamBody,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let (cluster, endpoint) = policy.target();
        let op = ForwardOp::new(cluster.clone(), ctx.method(), ctx.path().to_owned())
            .with_endpoint(endpoint)
            .with_query(ctx.query().map(str::to_owned))
            .with_protocol(ctx.protocol())
            .with_trace(Some(wire_trace(ctx)));
        let outcome = self.sink.forward_stream(op, body).await?;
        trace.record_dispatch(DispatchInfo {
            cluster,
            upstream_status: outcome.status,
            pool_reuse: outcome.pool_reuse,
        });
        Ok(PipelineResponse {
            status: outcome.status,
            body: outcome.body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{EndpointKind, PrincipalId, RequestId};
    use osproxy_spi::{HeaderView, HttpMethod, Principal, Protocol};

    fn ctx_for<'a>(
        principal: &'a Principal,
        rid: &'a RequestId,
        headers: &'a [(String, String)],
        logical_index: &'a str,
    ) -> RequestCtx<'a> {
        RequestCtx::new(
            principal,
            rid,
            HttpMethod::Post,
            EndpointKind::IngestDoc,
            Protocol::Http1,
            logical_index,
            HeaderView::new(headers),
            b"",
        )
    }

    fn matches_index(policy: &PassthroughPolicy, logical_index: &str) -> bool {
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("r");
        let headers = vec![];
        policy.matches(&ctx_for(&principal, &rid, &headers, logical_index))
    }

    #[test]
    fn a_prefix_free_policy_passes_every_request_through() {
        let policy = PassthroughPolicy::new(ClusterId::from("c"), "http://c:9200");
        assert!(matches_index(&policy, "anything"));
        assert!(matches_index(&policy, "orders"));
    }

    #[test]
    fn a_prefix_policy_passes_only_matching_indices_and_isolates_the_rest() {
        // The migration shape: legacy indices pass through, everything else stays
        // tenanted (fail-closed — a non-match keeps tenancy).
        let policy = PassthroughPolicy::new(ClusterId::from("c"), "http://c:9200")
            .with_index_prefixes(vec!["legacy-".to_owned(), "raw_".to_owned()]);
        assert!(matches_index(&policy, "legacy-orders"), "prefix match");
        assert!(matches_index(&policy, "raw_events"), "second prefix match");
        assert!(!matches_index(&policy, "orders"), "tenanted index isolated");
        assert!(
            !matches_index(&policy, "not-legacy-orders"),
            "prefix must anchor at the start, not match mid-string"
        );
    }
}
