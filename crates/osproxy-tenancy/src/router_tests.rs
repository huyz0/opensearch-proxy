use super::*;
use osproxy_core::{ClusterId, EndpointKind, PrincipalId, RequestId};
use osproxy_spi::{
    BodyDoc, DocIdRule, HeaderView, HttpMethod, IdTemplate, PartitionKeySpec, PlacementAt,
    Principal, Protocol, SensitivitySpec,
};

/// A `SharedIndex` tenancy whose `doc_id_rule` is configurable, to prove the
/// by-id isolation invariant is enforced regardless of the rule's presence.
struct SharedTenancy {
    id_rule: Option<DocIdRule>,
    routing_hint: Option<String>,
}

impl TenancySpi for SharedTenancy {
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        crate::resolve_partition_spec(&PartitionKeySpec::Header("x-tenant".to_owned()), ctx, body)
    }
    fn doc_id_rule(&self) -> Option<DocIdRule> {
        self.id_rule.clone()
    }
    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![InjectedField::new(
            osproxy_core::FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )]
    }
    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::none()
    }
    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(
            Placement::SharedIndex {
                cluster: ClusterId::from("c"),
                index: IndexName::from("shared"),
                inject: self.injected_fields(),
            },
            Epoch::new(1),
        ))
    }
    fn routing_hint(&self, _partition: &PartitionId) -> Option<String> {
        self.routing_hint.clone()
    }
}

async fn resolve_shared(id_rule: Option<DocIdRule>) -> Result<Resolved, SpiError> {
    resolve_shared_with_hint(id_rule, None).await
}

async fn resolve_shared_with_hint(
    id_rule: Option<DocIdRule>,
    routing_hint: Option<String>,
) -> Result<Resolved, SpiError> {
    let router = TenancyRouter::new(SharedTenancy {
        id_rule,
        routing_hint,
    });
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Get,
        EndpointKind::GetById,
        Protocol::Http1,
        "shared",
        HeaderView::new(&headers),
        b"",
    );
    router
        .resolve_placement(&ctx, PartitionId::from("acme"), "shared")
        .await
}

#[tokio::test]
async fn shared_index_without_an_id_rule_is_rejected() {
    // No id rule ⇒ by-id paths would use the raw client id, colliding across
    // tenants. Must fail closed (docs/03 §4), not silently route.
    let err = resolve_shared(None).await.unwrap_err();
    assert!(matches!(err, SpiError::IdRuleMissingPartition));
}

#[tokio::test]
async fn shared_index_with_a_partition_free_id_rule_is_rejected() {
    let rule = DocIdRule::new(IdTemplate::new("{body.id}"));
    let err = resolve_shared(Some(rule)).await.unwrap_err();
    assert!(matches!(err, SpiError::IdRuleMissingPartition));
}

#[tokio::test]
async fn shared_index_with_a_partition_scoped_id_rule_is_accepted() {
    let rule = DocIdRule::new(IdTemplate::new("{partition}:{body.id}"));
    let resolved = resolve_shared(Some(rule)).await.expect("accepted");
    assert!(matches!(
        resolved.decision.body_transform,
        BodyTransform::Both { .. }
    ));
}

#[tokio::test]
async fn routing_hint_defaults_to_none_when_the_spi_does_not_override_it() {
    let rule = DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true);
    let resolved = resolve_shared(Some(rule)).await.expect("accepted");
    assert_eq!(resolved.routing_hint, None);
}

#[tokio::test]
async fn routing_hint_carries_the_spis_override_through_to_resolved() {
    let rule = DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true);
    let resolved = resolve_shared_with_hint(Some(rule), Some("shard-3".to_owned()))
        .await
        .expect("accepted");
    assert_eq!(resolved.routing_hint.as_deref(), Some("shard-3"));
}

/// A tenancy that derives the partition by running code over an encoded
/// header (here, splitting `"<tenant>.<sig>"` and taking the claim) rather
/// than naming a header for the proxy to read verbatim.
struct EncodedHeaderTenancy;

impl TenancySpi for EncodedHeaderTenancy {
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        // Decode an encoded header ourselves first; take the claim before the
        // signature separator.
        if let Some(raw) = ctx.headers().get("x-tenant-token") {
            let claim = raw.split_once('.').map_or(raw, |(c, _sig)| c);
            if !claim.is_empty() {
                return Ok(PartitionId::from(claim));
            }
        }
        // The declarative source resolves a *different*, wrong id; reaching it
        // would prove the decode path did not take precedence.
        crate::resolve_partition_spec(&PartitionKeySpec::Header("x-wrong".to_owned()), ctx, body)
    }
    fn doc_id_rule(&self) -> Option<DocIdRule> {
        None
    }
    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![]
    }
    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::none()
    }
    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(
            Placement::DedicatedCluster {
                cluster: ClusterId::from("c"),
            },
            Epoch::new(1),
        ))
    }
}

#[tokio::test]
async fn a_code_extractor_decodes_the_partition_and_wins_over_the_declarative_source() {
    let router = TenancyRouter::new(EncodedHeaderTenancy);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers = vec![
        ("x-tenant-token".to_owned(), "acme.deadbeefsig".to_owned()),
        ("x-wrong".to_owned(), "intruder".to_owned()),
    ];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Get,
        EndpointKind::GetById,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        b"",
    );
    let partition = router
        .resolve_partition(&ctx, BodyDoc::new(ctx.body()))
        .expect("extracted");
    assert_eq!(partition, PartitionId::from("acme"));
}

#[tokio::test]
async fn resolve_rejects_a_tenancy_unaware_endpoint() {
    let router = TenancyRouter::new(SharedTenancy {
        id_rule: None,
        routing_hint: None,
    });
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers: Vec<(String, String)> = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Get,
        EndpointKind::Admin,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        b"",
    );
    let err = router.resolve(&ctx).await.unwrap_err();
    assert!(matches!(err, SpiError::UnsupportedEndpoint { .. }));
}

#[tokio::test]
async fn dedicated_cluster_target_keeps_the_logical_index_name_unchanged() {
    let router = TenancyRouter::new(EncodedHeaderTenancy);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers = vec![("x-tenant-token".to_owned(), "acme.sig".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Get,
        EndpointKind::GetById,
        Protocol::Http1,
        "my-logical-index",
        HeaderView::new(&headers),
        b"",
    );
    let resolved = router
        .resolve_placement(&ctx, PartitionId::from("acme"), "my-logical-index")
        .await
        .expect("resolves");
    assert_eq!(resolved.decision.target.index.as_str(), "my-logical-index");
}

#[tokio::test]
async fn admit_write_delegates_to_the_tenancy_spi() {
    let router = TenancyRouter::new(SharedTenancy {
        id_rule: None,
        routing_hint: None,
    });
    // The reference `SharedTenancy` in this test module has no custom
    // `admit_write`, so the `TenancySpi` default (always admit) applies.
    assert!(
        router
            .admit_write(&PartitionId::from("acme"), Epoch::new(1))
            .await
    );
}

/// Calls `cluster_endpoint` through the `Router` trait bound (not the inherent
/// `TenancyRouter` method), so both call paths are covered by one helper.
fn cluster_endpoint_via_trait<R: Router>(router: &R, cluster: &ClusterId) -> Option<String> {
    router.cluster_endpoint(cluster)
}

#[test]
fn cluster_endpoint_defaults_to_none_when_the_tenancy_does_not_override_it() {
    let router = TenancyRouter::new(SharedTenancy {
        id_rule: None,
        routing_hint: None,
    });
    let c = ClusterId::from("c");
    assert_eq!(router.cluster_endpoint(&c), None);
    assert_eq!(cluster_endpoint_via_trait(&router, &c), None);
}

#[test]
fn spi_accessor_returns_the_wrapped_tenancy() {
    let router = TenancyRouter::new(SharedTenancy {
        id_rule: None,
        routing_hint: None,
    });
    // Just needs to compile and return something usable; `SharedTenancy` has no
    // public state to assert on, so call a trait method through it.
    assert_eq!(router.spi().injected_fields().len(), 1);
}

/// A `DedicatedIndex` tenancy whose `upstream_credentials` is configurable, to
/// prove the SPI's credential resolution reaches the routed `Target`.
struct CredentialedTenancy {
    credentials: Option<osproxy_core::UpstreamCredentials>,
}

impl TenancySpi for CredentialedTenancy {
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        crate::resolve_partition_spec(&PartitionKeySpec::Header("x-tenant".to_owned()), ctx, body)
    }
    fn doc_id_rule(&self) -> Option<DocIdRule> {
        None
    }
    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![]
    }
    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(
            Placement::DedicatedIndex {
                cluster: ClusterId::from("c1"),
                index: IndexName::from("acme-idx"),
            },
            Epoch::new(1),
        ))
    }
    fn upstream_credentials(
        &self,
        cluster: &ClusterId,
    ) -> Option<osproxy_core::UpstreamCredentials> {
        (cluster.as_str() == "c1")
            .then(|| self.credentials.clone())
            .flatten()
    }
}

#[tokio::test]
async fn upstream_credentials_from_the_spi_reach_the_target() {
    let creds = osproxy_core::UpstreamCredentials::bearer("service-token");
    let router = TenancyRouter::new(CredentialedTenancy {
        credentials: Some(creds.clone()),
    });
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        b"",
    );
    let resolved = router.resolve(&ctx).await.expect("resolves");
    assert_eq!(resolved.decision.target.credentials, Some(creds));
}

#[test]
fn upstream_credentials_defaults_to_none_when_the_spi_does_not_override_it() {
    let router = TenancyRouter::new(SharedTenancy {
        id_rule: None,
        routing_hint: None,
    });
    assert_eq!(router.upstream_credentials(&ClusterId::from("c")), None);
}

#[path = "router_inject_tests.rs"]
mod inject_tests;
