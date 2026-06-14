//! Round-trip symmetry property (write side), per the M1 exit criteria
//! (`docs/11`).
//!
//! The headline correctness property of the shared-index model: whatever the
//! ingest path injects, the read path strips, recovering the client's original
//! document exactly. Here we prove the write-side inverse — inject-then-strip is
//! the identity — over arbitrary documents and injected field sets. The full
//! write+read round-trip through the proxy is proven in M2.

use osproxy_core::FieldName;
use osproxy_rewrite::{inject_fields, strip_fields};
use proptest::prelude::*;
use serde_json::{Map, Value};

/// An arbitrary JSON object whose keys never start with `_` (our injected
/// fields are `_`-prefixed), so generated client docs can't collide with
/// injected tenancy fields.
fn client_object() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        "[a-z]{0,8}".prop_map(Value::from),
    ];
    prop::collection::vec(("[a-z]{1,6}", leaf), 0..6).prop_map(|entries| {
        let mut obj = Map::new();
        for (k, v) in entries {
            obj.insert(k, v);
        }
        Value::Object(obj)
    })
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
    #[test]
    fn inject_then_strip_is_identity(
        original in client_object(),
        fields in injected_fields(),
    ) {
        let mut doc = original.clone();
        inject_fields(&mut doc, &fields).expect("client keys never collide with _-fields");

        // Every injected field is present after inject.
        for (name, value) in &fields {
            prop_assert_eq!(&doc[name.as_str()], value);
        }

        let names: Vec<_> = fields.iter().map(|(n, _)| n.clone()).collect();
        let removed = strip_fields(&mut doc, &names);
        prop_assert_eq!(removed, fields.len());
        prop_assert_eq!(doc, original);
    }
}
