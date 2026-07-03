use super::*;
use osproxy_core::{ClusterId, EndpointKind, FieldName, PrincipalId, RequestId};
use osproxy_spi::{
    BodyDoc, DocIdRule, HeaderView, HttpMethod, IdTemplate, PartitionKeySpec, PlacementAt,
    Principal, Protocol, SensitivitySpec,
};

/// A `SharedIndex` tenancy whose `doc_id_rule` is configurable, to prove the
/// by-id isolation invariant is enforced regardless of the rule's presence.
struct SharedTenancy {
    id_rule: Option<DocIdRule>,
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
}

async fn resolve_shared(id_rule: Option<DocIdRule>) -> Result<Resolved, SpiError> {
    let router = TenancyRouter::new(SharedTenancy { id_rule });
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

#[test]
fn resolve_inject_keeps_the_partition_field_symbolic_and_resolves_a_header_field() {
    // A SharedIndex inject list: the isolation field (PartitionId) plus a
    // decorative field whose value comes from a request header.
    let fields = vec![
        InjectedField::new(FieldName::from("_tenant"), InjectedValue::PartitionId),
        InjectedField::new(
            FieldName::from("_region"),
            InjectedValue::FromHeader("x-region".to_owned()),
        ),
    ];
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers = vec![("x-region".to_owned(), "eu".to_owned())];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        b"{}",
    );

    let resolved = resolve_inject(&fields, &PartitionId::from("acme"), &ctx).expect("resolves");
    // The partition field stays symbolic so the read path filters on it.
    assert_eq!(resolved[0].value, InjectedValue::PartitionId);
    // The header field is resolved to a concrete constant from this request.
    assert_eq!(
        resolved[1].value,
        InjectedValue::Constant(serde_json::Value::from("eu"))
    );
}

#[test]
fn resolve_inject_errors_when_a_required_header_is_absent() {
    let fields = vec![InjectedField::new(
        FieldName::from("_region"),
        InjectedValue::FromHeader("x-region".to_owned()),
    )];
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers: Vec<(String, String)> = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        b"{}",
    );
    let err = resolve_inject(&fields, &PartitionId::from("acme"), &ctx).unwrap_err();
    assert!(matches!(err, SpiError::HeaderMissing { header } if header == "x-region"));
}

#[test]
fn resolve_inject_resolves_or_errors_on_a_principal_attribute() {
    let fields = vec![InjectedField::new(
        FieldName::from("_region"),
        InjectedValue::FromPrincipal("region".to_owned()),
    )];
    let rid = RequestId::from("r1");
    let headers: Vec<(String, String)> = vec![];

    // Present: resolves to a constant.
    let with_attr = Principal::new(PrincipalId::from("svc"))
        .with_attr(osproxy_spi::PrincipalAttr::new("region", "eu"));
    let ctx = RequestCtx::new(
        &with_attr,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        b"{}",
    );
    let resolved = resolve_inject(&fields, &PartitionId::from("acme"), &ctx).expect("resolves");
    assert_eq!(
        resolved[0].value,
        InjectedValue::Constant(serde_json::Value::from("eu"))
    );

    // Absent: a config/identity mismatch, surfaced as a routing failure.
    let without_attr = Principal::new(PrincipalId::from("svc"));
    let ctx = RequestCtx::new(
        &without_attr,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        b"{}",
    );
    let err = resolve_inject(&fields, &PartitionId::from("acme"), &ctx).unwrap_err();
    assert!(matches!(err, SpiError::PrincipalAttrMissing { attr } if attr == "region"));
}

#[tokio::test]
async fn resolve_rejects_a_tenancy_unaware_endpoint() {
    let router = TenancyRouter::new(SharedTenancy { id_rule: None });
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
    let router = TenancyRouter::new(SharedTenancy { id_rule: None });
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
    let router = TenancyRouter::new(SharedTenancy { id_rule: None });
    let c = ClusterId::from("c");
    assert_eq!(router.cluster_endpoint(&c), None);
    assert_eq!(cluster_endpoint_via_trait(&router, &c), None);
}

#[test]
fn spi_accessor_returns_the_wrapped_tenancy() {
    let router = TenancyRouter::new(SharedTenancy { id_rule: None });
    // Just needs to compile and return something usable; `SharedTenancy` has no
    // public state to assert on, so call a trait method through it.
    assert_eq!(router.spi().injected_fields().len(), 1);
}

/// A `DedicatedIndex` tenancy that also injects a decorative field, a legal
/// but unusual `TenancySpi` combination, to prove `build_transform` still
/// applies no inject/id-rule outside `SharedIndex`.
struct DedicatedIndexWithInject;

impl TenancySpi for DedicatedIndexWithInject {
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
        vec![InjectedField::new(
            FieldName::from("_region"),
            InjectedValue::Constant(serde_json::Value::from("eu")),
        )]
    }
    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::none()
    }
    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(
            Placement::DedicatedIndex {
                cluster: ClusterId::from("c"),
                index: IndexName::from(format!("idx-{}", partition.as_str())),
            },
            Epoch::new(1),
        ))
    }
}

#[tokio::test]
async fn dedicated_index_target_pins_the_placements_physical_index() {
    let router = TenancyRouter::new(DedicatedIndexWithInject);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r1");
    let headers = vec![("x-tenant".to_owned(), "acme".to_owned())];
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
    let resolved = router
        .resolve_placement(&ctx, PartitionId::from("acme"), "logical")
        .await
        .expect("DedicatedIndex needs no id rule");
    assert_eq!(resolved.decision.target.index.as_str(), "idx-acme");
    // Dedicated modes never rewrite the body: `build_transform` applies inject
    // only to `SharedIndex`, even when `injected_fields()` reports one.
    assert!(matches!(
        resolved.decision.body_transform,
        BodyTransform::None
    ));
}
