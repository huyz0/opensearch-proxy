//! End-to-end adapter test: a `TenancySpi` backed by a real [`PlacementTable`],
//! driven through [`TenancyRouter`] over the [`RoutingSpi`] contract.
//!
//! Exercises the M1 spine short of the wire: partition resolution, placement
//! lookup, target derivation, injected-field resolution, and the shared-index
//! id-rule invariant.

use std::sync::Arc;

use osproxy_core::{
    ClusterId, EndpointKind, FieldName, IndexName, PartitionId, PrincipalId, RequestId,
};
use osproxy_spi::{
    BodyTransform, DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue,
    JsonPath, PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx,
    RoutingSpi, SensitivitySpec, SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter};

/// A tenancy implementation: partition is `tenant_id` in the body, every doc
/// gets a `_tenant` field, and the `_id` is `{partition}:{body.id}`. Placement
/// comes from a shared in-memory table.
struct SharedTenancy {
    table: Arc<PlacementTable>,
    id_rule: Option<DocIdRule>,
}

impl TenancySpi for SharedTenancy {
    fn partition_key(&self) -> PartitionKeySpec {
        PartitionKeySpec::BodyField(JsonPath::new("tenant_id"))
    }
    fn doc_id_rule(&self) -> Option<DocIdRule> {
        self.id_rule.clone()
    }
    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )]
    }
    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::none()
    }
    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        self.table
            .get(partition)
            .ok_or_else(|| SpiError::PlacementMissing {
                partition: partition.clone(),
            })
    }
}

fn ingest_ctx<'a>(
    principal: &'a Principal,
    rid: &'a RequestId,
    headers: &'a [(String, String)],
    body: &'a [u8],
) -> RequestCtx<'a> {
    RequestCtx::new(
        principal,
        rid,
        HttpMethod::Put,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "orders-logical",
        HeaderView::new(headers),
        body,
    )
}

fn shared(cluster: &str, index: &str, inject: Vec<InjectedField>) -> Placement {
    Placement::SharedIndex {
        cluster: ClusterId::from(cluster),
        index: IndexName::from(index),
        inject,
    }
}

#[tokio::test]
async fn shared_index_ingest_resolves_target_inject_and_id() {
    let table = Arc::new(PlacementTable::new());
    let inject = vec![InjectedField::new(
        FieldName::from("_tenant"),
        InjectedValue::PartitionId,
    )];
    let epoch = table.set(
        PartitionId::from("acme"),
        shared("eu-1", "orders-shared", inject),
    );

    let router = TenancyRouter::new(SharedTenancy {
        table: Arc::clone(&table),
        id_rule: Some(DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true)),
    });

    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("req-1");
    let headers = vec![];
    let body = br#"{ "tenant_id": "acme", "id": 1001, "msg": "hi" }"#;
    let ctx = ingest_ctx(&principal, &rid, &headers, body);

    let resolved = router.resolve(&ctx).await.unwrap();
    assert_eq!(resolved.partition, PartitionId::from("acme"));

    let d = &resolved.decision;
    assert_eq!(
        d.target,
        osproxy_core::Target::new(ClusterId::from("eu-1"), IndexName::from("orders-shared"))
    );
    assert_eq!(d.epoch, epoch);

    assert!(
        matches!(&d.body_transform, BodyTransform::Both { .. }),
        "expected Both, got {:?}",
        d.body_transform
    );
    if let BodyTransform::Both { inject, id } = &d.body_transform {
        // The partition isolation field stays symbolic (`PartitionId`) through
        // resolution, so the read path can filter on it; downstream stages
        // resolve it to the concrete partition when injecting.
        assert_eq!(inject.len(), 1);
        assert_eq!(inject[0].name, FieldName::from("_tenant"));
        assert_eq!(inject[0].value, InjectedValue::PartitionId);
        assert!(id.set_routing);
        assert!(id.template.references_partition());
    }
}

#[tokio::test]
async fn missing_placement_is_reported() {
    let table = Arc::new(PlacementTable::new());
    let router = TenancyRouter::new(SharedTenancy {
        table,
        id_rule: None,
    });
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![];
    let ctx = ingest_ctx(&principal, &rid, &headers, br#"{ "tenant_id": "ghost" }"#);

    let err = router.route(&ctx).await.unwrap_err();
    assert!(
        matches!(&err, SpiError::PlacementMissing { partition }
            if *partition == PartitionId::from("ghost")),
        "expected PlacementMissing, got {err:?}"
    );
}

#[tokio::test]
async fn shared_index_id_rule_without_partition_is_rejected() {
    let table = Arc::new(PlacementTable::new());
    table.set(PartitionId::from("acme"), shared("eu-1", "shared", vec![]));
    let router = TenancyRouter::new(SharedTenancy {
        table,
        // Omits {partition}: would risk cross-tenant id collisions.
        id_rule: Some(DocIdRule::new(IdTemplate::new("{body.id}"))),
    });
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![];
    let ctx = ingest_ctx(
        &principal,
        &rid,
        &headers,
        br#"{ "tenant_id": "acme", "id": 1 }"#,
    );

    assert!(matches!(
        router.route(&ctx).await,
        Err(SpiError::IdRuleMissingPartition)
    ));
}

#[tokio::test]
async fn unresolved_partition_is_reported() {
    let table = Arc::new(PlacementTable::new());
    let router = TenancyRouter::new(SharedTenancy {
        table,
        id_rule: None,
    });
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers = vec![];
    // No tenant_id in the body.
    let ctx = ingest_ctx(&principal, &rid, &headers, br#"{ "msg": "hi" }"#);

    assert!(matches!(
        router.route(&ctx).await,
        Err(SpiError::PartitionUnresolved { .. })
    ));
}
