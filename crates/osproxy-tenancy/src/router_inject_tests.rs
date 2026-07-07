//! Split out of `router_tests.rs` to stay under the file-length budget:
//! `resolve_inject`'s value-resolution cases and the `DedicatedIndex`
//! placement-with-inject case, neither of which shares fixtures with the
//! rest of that module.

use super::*;
use osproxy_core::{ClusterId, EndpointKind, FieldName, PrincipalId, RequestId};
use osproxy_spi::{
    BodyDoc, DocIdRule, HeaderView, HttpMethod, PartitionKeySpec, PlacementAt, Principal, Protocol,
    SensitivitySpec,
};

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
