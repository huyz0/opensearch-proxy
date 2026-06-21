use super::*;
use osproxy_core::{ClusterId, Epoch, IndexName, PartitionId, Target};
use osproxy_spi::{IdTemplate, InjectedField, InjectedValue, Protocol, RouteDecision};
use serde_json::json;

fn resolved(transform: BodyTransform) -> Resolved {
    Resolved {
        partition: PartitionId::from("acme"),
        decision: RouteDecision {
            target: Target::new(ClusterId::from("eu-1"), IndexName::from("shared")),
            upstream_protocol: Protocol::Http1,
            header_ops: Vec::new(),
            body_transform: transform,
            epoch: Epoch::new(4),
        },
        migration: osproxy_spi::MigrationPhase::Settled,
    }
}

fn shared_transform() -> BodyTransform {
    BodyTransform::Both {
        // The partition field stays `PartitionId` through resolution: it is
        // the isolation key the read path filters on.
        inject: vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )],
        id: DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true),
    }
}

#[test]
fn read_op_maps_logical_id_and_sets_routing() {
    let (op, shape) = build_read_op(&resolved(shared_transform()), "7").unwrap();
    assert_eq!(op.id, "acme:7");
    assert_eq!(op.routing.as_deref(), Some("acme"));
    assert_eq!(op.target.index.as_str(), "shared");
    assert_eq!(shape.inject_names, vec![FieldName::from("_tenant")]);
}

#[test]
fn read_op_without_id_rule_uses_client_id() {
    let (op, _) = build_read_op(&resolved(BodyTransform::None), "raw-id").unwrap();
    assert_eq!(op.id, "raw-id");
    assert!(op.routing.is_none());
}

#[test]
fn found_response_is_the_logical_document() {
    let upstream = br#"{
        "_index": "shared",
        "_id": "acme:7",
        "_routing": "acme",
        "found": true,
        "_source": { "_tenant": "acme", "msg": "hi" }
    }"#;
    let body = shape_found(upstream, "orders", "7", &[FieldName::from("_tenant")]).unwrap();
    let doc: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc["_index"], "orders");
    assert_eq!(doc["_id"], "7");
    assert!(doc.get("_routing").is_none());
    assert!(doc["_source"].get("_tenant").is_none());
    assert_eq!(doc["_source"]["msg"], "hi");
}

#[test]
fn not_found_body_is_logical() {
    let doc: Value = serde_json::from_slice(&not_found_body("orders", "7")).unwrap();
    assert_eq!(doc["_index"], "orders");
    assert_eq!(doc["_id"], "7");
    assert_eq!(doc["found"], false);
}

#[test]
fn delete_op_maps_logical_id_and_sets_routing() {
    let op = build_delete_op(&resolved(shared_transform()), "7").unwrap();
    assert_eq!(op.epoch, Epoch::new(4));
    let DocOp::Delete { id, routing } = &op.doc else {
        unreachable!("delete-by-id produces a Delete op")
    };
    assert_eq!(id, "acme:7");
    assert_eq!(routing.as_deref(), Some("acme"));
}

#[test]
fn delete_response_reports_logical_terms() {
    let ok: Value = serde_json::from_slice(&shape_delete("orders", "7", 200)).unwrap();
    assert_eq!(ok["_index"], "orders");
    assert_eq!(ok["_id"], "7");
    assert_eq!(ok["result"], "deleted");
    let miss: Value = serde_json::from_slice(&shape_delete("orders", "7", 404)).unwrap();
    assert_eq!(miss["result"], "not_found");
}

#[test]
fn search_op_wraps_client_query_in_the_partition_filter() {
    let (op, _) = build_search_op(
        &resolved(shared_transform()),
        br#"{"query":{"match_all":{}}}"#,
    )
    .unwrap();
    let q: Value = serde_json::from_slice(&op.body).unwrap();
    assert_eq!(q["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
    assert_eq!(q["query"]["bool"]["must"][0]["match_all"], json!({}));
}

#[test]
fn a_decorative_injected_field_is_stripped_but_never_filtered() {
    // A SharedIndex placement injecting the partition field plus a decorative,
    // context-derived field (resolved to a constant). The read must filter on
    // the partition field ONLY (filtering the decorative value would exclude
    // the tenant's own docs), yet strip BOTH from hits.
    let transform = BodyTransform::Both {
        inject: vec![
            InjectedField::new(FieldName::from("_tenant"), InjectedValue::PartitionId),
            InjectedField::new(
                FieldName::from("_region"),
                InjectedValue::Constant(json!("eu")),
            ),
        ],
        id: DocIdRule::new(IdTemplate::new("{partition}:{body.id}")),
    };
    let (op, shape) =
        build_search_op(&resolved(transform), br#"{"query":{"match_all":{}}}"#).unwrap();

    // Only the partition term is in the filter.
    let q: Value = serde_json::from_slice(&op.body).unwrap();
    let filter = q["query"]["bool"]["filter"].as_array().unwrap();
    assert_eq!(filter.len(), 1, "exactly one isolation term: {q}");
    assert_eq!(filter[0]["term"]["_tenant"], "acme");
    assert!(
        filter[0]["term"].get("_region").is_none(),
        "the decorative field must not be filtered: {q}"
    );
    // But both injected fields are stripped from hits.
    assert_eq!(
        shape.inject_names,
        vec![FieldName::from("_tenant"), FieldName::from("_region")]
    );
}

#[test]
fn hits_are_stripped_to_the_logical_view() {
    let upstream = br#"{
        "hits": { "total": { "value": 1 }, "hits": [
            { "_index": "shared", "_id": "acme:7", "_routing": "acme",
              "_source": { "_tenant": "acme", "msg": "hi" } }
        ] }
    }"#;
    let shape = read_shape(&shared_transform());
    let body = shape_hits(upstream, "orders", "acme", &shape).unwrap();
    let doc: Value = serde_json::from_slice(&body).unwrap();
    let hit = &doc["hits"]["hits"][0];
    assert_eq!(hit["_index"], "orders");
    assert_eq!(hit["_id"], "7");
    assert!(hit.get("_routing").is_none());
    assert!(hit["_source"].get("_tenant").is_none());
    assert_eq!(hit["_source"]["msg"], "hi");
}

#[test]
fn top_level_siblings_including_aggregations_pass_through_untouched() {
    // The strip shapes only `hits`; `took`, `_shards`, and `aggregations` (which
    // can dwarf the hits) are forwarded verbatim — never materialized/re-serialized
    // (ADR-014: the read-path counterpart of wrap_query's raw-sibling posture).
    let upstream = br#"{
        "took": 5,
        "_shards": { "total": 3, "successful": 3 },
        "hits": { "total": { "value": 1 }, "hits": [
            { "_index": "shared", "_id": "acme:7", "_routing": "acme",
              "_source": { "_tenant": "acme", "msg": "hi" } }
        ] },
        "aggregations": { "by_day": { "buckets": [ { "key": 1, "doc_count": 9 } ] } }
    }"#;
    let shape = read_shape(&shared_transform());
    let body = shape_hits(upstream, "orders", "acme", &shape).unwrap();
    let doc: Value = serde_json::from_slice(&body).unwrap();

    // Hits shaped...
    assert_eq!(doc["hits"]["hits"][0]["_index"], "orders");
    assert!(doc["hits"]["hits"][0]["_source"].get("_tenant").is_none());
    // ...siblings preserved exactly.
    assert_eq!(doc["took"], 5);
    assert_eq!(doc["_shards"]["successful"], 3);
    assert_eq!(doc["aggregations"]["by_day"]["buckets"][0]["doc_count"], 9);
}

#[test]
fn a_non_object_response_passes_through_unchanged() {
    // A valid but non-object body has no hits to shape; only invalid JSON errors.
    let shape = read_shape(&shared_transform());
    assert_eq!(
        shape_hits(b"[1,2,3]", "orders", "acme", &shape).unwrap(),
        b"[1,2,3]"
    );
    assert!(shape_hits(b"not json", "orders", "acme", &shape).is_err());
}
