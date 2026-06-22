//! Property tests for the no-materialization body path (ADR-014): the byte-level
//! splice/extract primitives must agree with the `Value`-tree primitives they
//! replace, with `serde_json` as the oracle. These are the isolation-critical
//! guarantees, a spliced body must parse back to exactly what the tree path
//! would have produced, and a spoofed reserved field must be rejected identically
//! whether the client sends it plainly or escaped.

#![allow(clippy::unwrap_used)]

use osproxy_core::FieldName;
use osproxy_rewrite::{
    construct_id, construct_id_bytes, inject_fields, inject_fields_bytes, strip_fields,
};
use proptest::prelude::*;
use serde_json::{Map, Value};

/// An arbitrary JSON object whose keys never start with `_` (injected fields are
/// `_`-prefixed), so generated client docs can't collide with tenancy fields.
fn client_object() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        "[a-zA-Z0-9 ]{0,8}".prop_map(Value::from),
    ];
    let value = prop_oneof![
        leaf.clone(),
        prop::collection::vec(leaf.clone(), 0..3).prop_map(Value::Array),
        prop::collection::vec(("[a-z]{1,4}", leaf), 0..3).prop_map(into_object),
    ];
    prop::collection::vec(("[a-z]{1,6}", value), 0..6).prop_map(into_object)
}

fn into_object(entries: Vec<(impl Into<String>, Value)>) -> Value {
    let mut obj = Map::new();
    for (k, v) in entries {
        obj.insert(k.into(), v);
    }
    Value::Object(obj)
}

/// A set of distinct `_`-prefixed injected fields with string values.
fn injected_fields() -> impl Strategy<Value = Vec<(FieldName, Value)>> {
    prop::collection::vec(("_[a-z]{1,6}", "[a-z0-9]{0,8}"), 0..4).prop_map(|pairs| {
        let mut seen = std::collections::HashSet::new();
        pairs
            .into_iter()
            .filter(|(k, _)| seen.insert(k.clone()))
            .map(|(k, v)| (FieldName::from(k.as_str()), Value::from(v)))
            .collect()
    })
}

proptest! {
    /// The byte splice produces the same document the `Value` inject would, for
    /// any client object and field set, order-independent (compared as parsed
    /// `Value`s).
    #[test]
    fn inject_bytes_matches_value_inject(
        original in client_object(),
        fields in injected_fields(),
    ) {
        let body = serde_json::to_vec(&original).unwrap();

        let mut via_tree = original.clone();
        inject_fields(&mut via_tree, &fields).unwrap();

        let spliced = inject_fields_bytes(&body, &fields).unwrap();
        let via_bytes: Value = serde_json::from_slice(&spliced).unwrap();

        prop_assert_eq!(via_bytes, via_tree);
    }

    /// Splice-inject then strip recovers the original document exactly.
    #[test]
    fn inject_bytes_then_strip_is_identity(
        original in client_object(),
        fields in injected_fields(),
    ) {
        let body = serde_json::to_vec(&original).unwrap();
        let spliced = inject_fields_bytes(&body, &fields).unwrap();
        let mut doc: Value = serde_json::from_slice(&spliced).unwrap();
        let names: Vec<_> = fields.iter().map(|(n, _)| n.clone()).collect();
        strip_fields(&mut doc, &names);
        prop_assert_eq!(doc, original);
    }

    /// A client field that collides with an injected name is rejected by the byte
    /// path exactly when the tree path rejects it, including when the client
    /// escapes the key to try to smuggle it past the scan.
    #[test]
    fn spoofed_reserved_field_is_rejected_like_the_tree(
        field in "_[a-z]{1,6}",
        escape in any::<bool>(),
    ) {
        let name = FieldName::from(field.as_str());
        // Build a body that contains the reserved field, optionally with its first
        // character written as a `\u00XX` escape so the raw bytes differ but the
        // decoded key is identical.
        let body = if escape {
            let first = field.as_bytes()[0];
            let rest = &field[1..];
            format!(r#"{{"\u{first:04x}{rest}":"evil"}}"#).into_bytes()
        } else {
            format!(r#"{{"{field}":"evil"}}"#).into_bytes()
        };
        let err = inject_fields_bytes(&body, &[(name, Value::from("acme"))]);
        prop_assert!(err.is_err(), "escaped={escape} body must be rejected");
    }
}

// A document with one top-level scalar key, plus a template referencing it.
proptest! {
    #[test]
    fn construct_id_bytes_matches_value_construct(
        key in "[a-z]{1,6}",
        natural in "[a-zA-Z0-9]{1,10}",
        extra in client_object(),
    ) {
        // Merge the natural-key field into an arbitrary object.
        let mut obj = match extra {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        obj.insert(key.clone(), Value::from(natural.clone()));
        let doc = Value::Object(obj);
        let body = serde_json::to_vec(&doc).unwrap();

        let template = format!("{{partition}}:{{body.{key}}}");
        let via_tree = construct_id(&template, "acme", &doc).unwrap();
        let via_bytes = construct_id_bytes(&template, "acme", &body).unwrap();
        prop_assert_eq!(via_bytes, via_tree);
    }
}
