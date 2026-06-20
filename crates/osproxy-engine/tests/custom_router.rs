//! The pipeline is generic over the [`Router`] seam, not nailed to the in-tree
//! [`TenancyRouter`]: a consumer can supply their own routing implementation and
//! drive the engine with it. Proves the seam is a real substitution point by
//! pinning every request to a fixed target through a hand-rolled `Router` and
//! confirming the write lands there.

#![allow(clippy::unwrap_used)]

use osproxy_core::{
    ClusterId, EndpointKind, Epoch, IndexName, PartitionId, PrincipalId, RequestId, Target,
};
use osproxy_engine::Pipeline;
use osproxy_sink::MemorySink;
use osproxy_spi::{
    BodyDoc, HeaderView, HttpMethod, MigrationPhase, Principal, Protocol, RequestCtx,
    RouteDecision, SpiError,
};
use osproxy_tenancy::{Resolved, Router};

/// A minimal router with no tenancy logic at all: every request resolves to one
/// fixed cluster + index. The point is that the engine accepts it.
struct PinRouter {
    target: Target,
}

impl PinRouter {
    fn resolved(&self) -> Resolved {
        Resolved {
            partition: PartitionId::from("pinned"),
            decision: RouteDecision::passthrough(self.target.clone(), Protocol::Http1, Epoch::ZERO),
            migration: MigrationPhase::Settled,
        }
    }
}

impl Router for PinRouter {
    async fn resolve(&self, _ctx: &RequestCtx<'_>) -> Result<Resolved, SpiError> {
        Ok(self.resolved())
    }

    fn resolve_partition(
        &self,
        _ctx: &RequestCtx<'_>,
        _body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        Ok(PartitionId::from("pinned"))
    }

    async fn resolve_placement(
        &self,
        _ctx: &RequestCtx<'_>,
        _partition: PartitionId,
        _logical_index: &str,
    ) -> Result<Resolved, SpiError> {
        Ok(self.resolved())
    }

    async fn admit_write(&self, _partition: &PartitionId, _epoch: Epoch) -> bool {
        true
    }
}

#[tokio::test]
async fn the_engine_runs_on_a_custom_router_implementation() {
    let target = Target::new(ClusterId::from("custom"), IndexName::from("pinned-index"));
    let pipeline = Pipeline::new(PinRouter { target }, MemorySink::new());

    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers: Vec<(String, String)> = vec![];
    let body = br#"{"msg":"hi"}"#;
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        body,
    );

    let resp = pipeline.handle(&ctx).await.unwrap();
    assert!(resp.status >= 200 && resp.status < 300);

    // The custom router's fixed target is where the write landed.
    let recorded = pipeline.sink().recorded();
    assert_eq!(recorded.len(), 1, "one write committed");
    let target = &recorded[0].ops()[0].target;
    assert_eq!(target.cluster.as_str(), "custom");
    assert_eq!(target.index.as_str(), "pinned-index");
}
