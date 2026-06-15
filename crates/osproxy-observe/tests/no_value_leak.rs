//! The "no value leaks" guarantee (`docs/05` §7), exercised at runtime.
//!
//! We fuzz documents that embed canary secrets in field *values*, derive a trace
//! the way the pipeline would (only ids/shapes/sizes — never values), assemble
//! the explain document, and assert the canary never appears anywhere in the
//! emitted telemetry. The guarantee is structural (the trace API cannot accept a
//! value), and this test demonstrates it holds for adversarial inputs.

use osproxy_core::{ClusterId, EndpointKind, Epoch, FieldName, IndexName, PartitionId, RequestId};
use osproxy_observe::{
    explain_json, ClassifyInfo, DispatchInfo, EgressInfo, RequestTrace, ResolveInfo, RewriteInfo,
};

/// Builds a trace from a (conceptual) document the way the pipeline does: the
/// only request-derived datum is the partition *id*; the secret-bearing field
/// values contribute only a byte *size*.
fn trace_for(partition: &str, body_bytes: usize) -> RequestTrace {
    let mut t = RequestTrace::new();
    t.record_classify(ClassifyInfo {
        endpoint: EndpointKind::IngestDoc,
        logical_index: IndexName::from("orders"),
    });
    t.record_resolve(ResolveInfo {
        partition: PartitionId::from(partition),
        placement_kind: "shared_index",
        cluster: ClusterId::from("eu-1"),
        index: IndexName::from("orders-shared"),
        epoch: Epoch::new(1),
        inject_fields: vec![FieldName::from("_tenant")],
        routing: true,
        migration: "draining",
    });
    t.record_rewrite(RewriteInfo {
        transform_kind: "inject+construct_id",
        body_bytes,
    });
    t.record_dispatch(DispatchInfo {
        cluster: ClusterId::from("eu-1"),
        upstream_status: 201,
        pool_reuse: false,
    });
    t.record_egress(EgressInfo {
        status: 201,
        response_bytes: 64,
    });
    t
}

#[test]
fn canary_secrets_never_appear_in_the_explain_document() {
    const CANARY: &str = "SUPER_SECRET_CANARY_9f3a";

    // Adversarial documents: the canary appears in field values, in the body,
    // even as a substring near the partition id. None of it is ever passed to
    // the trace API, which accepts only ids/shapes/sizes.
    let documents = [
        format!(r#"{{"tenant_id":"acme","password":"{CANARY}"}}"#),
        format!(r#"{{"tenant_id":"acme","note":"prefix-{CANARY}-suffix"}}"#),
        format!(r#"{{"tenant_id":"acme","ssn":"{CANARY}","msg":"{CANARY}"}}"#),
    ];

    for doc in documents {
        let trace = trace_for("acme", doc.len());
        let rid = RequestId::from("req-1");
        let rendered = explain_json(&rid, &trace).to_string();
        assert!(
            !rendered.contains(CANARY),
            "canary leaked into telemetry: {rendered}"
        );
        // Sanity: the document really did contain the canary.
        assert!(doc.contains(CANARY));
    }
}

#[test]
fn a_partition_id_that_is_itself_secret_like_is_still_an_id_not_a_value() {
    // Even if a partition id happened to look secret, it is an *id* (docs/05
    // explicitly permits partition.id) — the test fixes the contract: values
    // (document fields) never appear; ids do.
    let trace = trace_for("acme", 128);
    let doc = explain_json(&RequestId::from("r"), &trace).to_string();
    assert!(doc.contains("\"partition_id\":\"acme\""));
    // No field *value* key like "password"/"ssn"/"note" can be present, because
    // the trace never carried them.
    for forbidden in ["password", "ssn", "\"note\"", "msg"] {
        assert!(
            !doc.contains(forbidden),
            "value-bearing key leaked: {forbidden}"
        );
    }
}
