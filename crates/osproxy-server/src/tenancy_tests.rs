// Unit tests for the reference tenancy's dedicated-index/dedicated-cluster
// modes, the no-body-rewrite routing paths that `SharedIndex` (exercised
// end-to-end elsewhere) does not.
use super::*;
use osproxy_core::PartitionId;
use osproxy_spi::SpiError;

fn tenancy(mode: PlacementMode) -> ReferenceTenancy {
    ReferenceTenancy::new(
        ClusterId::from("eu-1"),
        IndexName::from("shared"),
        "http://cluster.local:9200",
    )
    .with_placement_mode(mode)
}

#[test]
fn dedicated_index_injects_nothing_and_has_no_doc_id_rule() {
    let t = tenancy(PlacementMode::DedicatedIndex);
    assert!(t.doc_id_rule().is_none(), "no body rewrite in this mode");
    assert!(t.injected_fields().is_empty());
}

#[test]
fn dedicated_cluster_injects_nothing_and_has_no_doc_id_rule() {
    let t = tenancy(PlacementMode::DedicatedCluster);
    assert!(t.doc_id_rule().is_none());
    assert!(t.injected_fields().is_empty());
}

#[test]
fn shared_index_injects_the_tenant_field_and_has_a_partition_scoped_id_rule() {
    let t = tenancy(PlacementMode::SharedIndex);
    let rule = t.doc_id_rule().expect("shared index needs a scoped id");
    assert!(rule.template.references_partition());
    let fields = t.injected_fields();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name.as_str(), "_tenant");
}

#[tokio::test]
async fn dedicated_index_placement_scopes_the_physical_index_by_partition() {
    let t = tenancy(PlacementMode::DedicatedIndex);
    let at = t.placement_for(&PartitionId::from("acme")).await.unwrap();
    let osproxy_spi::Placement::DedicatedIndex { cluster, index } = at.placement else {
        unreachable!("DedicatedIndex mode always resolves to a DedicatedIndex placement");
    };
    assert_eq!(cluster.as_str(), "eu-1");
    assert_eq!(index.as_str(), "shared-acme");
    assert_eq!(at.endpoint.as_deref(), Some("http://cluster.local:9200"));
}

#[tokio::test]
async fn dedicated_cluster_placement_carries_only_the_cluster() {
    let t = tenancy(PlacementMode::DedicatedCluster);
    let at = t.placement_for(&PartitionId::from("acme")).await.unwrap();
    let osproxy_spi::Placement::DedicatedCluster { cluster } = at.placement else {
        unreachable!("DedicatedCluster mode always resolves to a DedicatedCluster placement");
    };
    assert_eq!(cluster.as_str(), "eu-1");
}

#[tokio::test]
async fn shared_index_placement_carries_the_injected_field() {
    let t = tenancy(PlacementMode::SharedIndex);
    let at = t.placement_for(&PartitionId::from("acme")).await.unwrap();
    let osproxy_spi::Placement::SharedIndex {
        cluster,
        index,
        inject,
    } = at.placement
    else {
        unreachable!("SharedIndex mode always resolves to a SharedIndex placement");
    };
    assert_eq!(cluster.as_str(), "eu-1");
    assert_eq!(index.as_str(), "shared");
    assert_eq!(inject.len(), 1);
}

#[test]
fn cluster_endpoint_resolves_only_the_configured_cluster() {
    let t = tenancy(PlacementMode::SharedIndex);
    assert_eq!(
        t.cluster_endpoint(&ClusterId::from("eu-1")),
        Some("http://cluster.local:9200".to_owned())
    );
    assert_eq!(t.cluster_endpoint(&ClusterId::from("us-1")), None);
}

#[test]
fn placement_mode_default_is_shared_index() {
    assert_eq!(PlacementMode::default(), PlacementMode::SharedIndex);
}

/// A no-op result type check: `resolve_partition` on this tenancy delegates to
/// the shared helper, exercised elsewhere; here we just confirm the error
/// surfaces as `PartitionUnresolved` when neither source is present.
#[test]
fn resolve_partition_errors_when_neither_source_is_present() {
    use osproxy_core::{PrincipalId, RequestId};
    use osproxy_spi::{BodyDoc, HeaderView, HttpMethod, Principal, Protocol, RequestCtx};

    let t = tenancy(PlacementMode::SharedIndex);
    let principal = Principal::new(PrincipalId::from("p1"));
    let rid = RequestId::from("req-1");
    let headers: Vec<(String, String)> = Vec::new();
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Put,
        osproxy_core::EndpointKind::IngestDoc,
        Protocol::Http1,
        "shared",
        HeaderView::new(&headers),
        b"{}",
    );
    let err = t
        .resolve_partition(&ctx, BodyDoc::new(ctx.body()))
        .unwrap_err();
    assert!(matches!(err, SpiError::PartitionUnresolved { .. }));
}
