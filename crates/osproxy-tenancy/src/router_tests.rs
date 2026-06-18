use super::*;
use osproxy_core::{ClusterId, EndpointKind, FieldName, PrincipalId, RequestId};
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, PartitionKeySpec, PlacementAt, Principal,
    Protocol, SensitivitySpec,
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
        doc: Option<&serde_json::Value>,
    ) -> Result<PartitionId, SpiError> {
        crate::resolve_partition_spec(&PartitionKeySpec::Header("x-tenant".to_owned()), ctx, doc)
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
        doc: Option<&serde_json::Value>,
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
        crate::resolve_partition_spec(&PartitionKeySpec::Header("x-wrong".to_owned()), ctx, doc)
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
    let partition = router.resolve_partition(&ctx, None).expect("extracted");
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
