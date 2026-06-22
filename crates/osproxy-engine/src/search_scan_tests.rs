// Test scaffolding for the streaming hit-transform scanner.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::read::{shape_hits, ReadShape};
use osproxy_core::FieldName;
use osproxy_spi::{DocIdRule, IdTemplate};
use serde_json::Value;

/// The same shape the shared-index placement produces: strip `_tenant`, invert
/// the `{partition}:{body.id}` id rule, drop `_routing`.
fn make_shape() -> ReadShape {
    ReadShape {
        inject_names: vec![FieldName::from("_tenant")],
        id_rule: Some(DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true)),
    }
}

fn shaper() -> HitShaper {
    HitShaper {
        logical_index: "orders".to_owned(),
        partition: "acme".to_owned(),
        shape: make_shape(),
    }
}

/// Runs the scanner over `body` split into chunks at `cut` (one split point),
/// returning the assembled streamed output.
fn run_split(body: &[u8], cut: usize) -> Vec<u8> {
    let mut scanner = SearchHitsScanner::new(shaper());
    let mut out = scanner.feed(&body[..cut]);
    out.extend(scanner.feed(&body[cut..]));
    out.extend(scanner.finish());
    out
}

/// Asserts that, for **every** single split point, the streamed output is
/// semantically equal to the buffered `shape_hits` oracle. Semantic (not byte)
/// equality is the right oracle: the buffered path re-serializes the `hits`
/// object and sorts top-level keys, while streaming forwards siblings verbatim —
/// equal JSON, different bytes. Equality to the audited oracle guarantees the
/// strip happened identically, so no injected field can leak.
fn assert_matches_oracle_for_all_splits(body: &[u8]) {
    let oracle = shape_hits(body, "orders", "acme", &make_shape()).expect("oracle ok");
    let oracle_val: Value = serde_json::from_slice(&oracle).expect("oracle is json");
    for cut in 0..=body.len() {
        let streamed = run_split(body, cut);
        let streamed_val: Value = serde_json::from_slice(&streamed)
            .unwrap_or_else(|e| panic!("streamed not json at cut {cut}: {e}\n{streamed:?}"));
        assert_eq!(
            streamed_val,
            oracle_val,
            "streamed != oracle at split {cut} of {}",
            body.len()
        );
    }
}

#[test]
fn strips_hits_and_preserves_siblings_across_every_split() {
    let body = br#"{
        "took": 5,
        "_shards": { "total": 3, "successful": 3 },
        "hits": { "total": { "value": 1 }, "max_score": 1.0, "hits": [
            { "_index": "shared", "_id": "acme:7", "_routing": "acme",
              "_source": { "_tenant": "acme", "msg": "hi" } },
            { "_index": "shared", "_id": "acme:8", "_routing": "acme",
              "_source": { "_tenant": "acme", "msg": "yo" } }
        ] },
        "aggregations": { "by_day": { "buckets": [ { "key": 1, "doc_count": 9 } ] } }
    }"#;
    assert_matches_oracle_for_all_splits(body);
    // Direct isolation check: the injected field never appears in the output.
    let streamed = run_split(body, body.len() / 2);
    assert!(
        !contains(&streamed, b"_tenant"),
        "injected field leaked: {}",
        String::from_utf8_lossy(&streamed)
    );
}

#[test]
fn empty_hits_array() {
    assert_matches_oracle_for_all_splits(
        br#"{"took":1,"hits":{"total":{"value":0},"hits":[]},"aggregations":{}}"#,
    );
}

#[test]
fn no_hits_key_passes_through() {
    assert_matches_oracle_for_all_splits(br#"{"took":1,"_shards":{"total":1}}"#);
}

#[test]
fn hits_value_not_an_object_passes_through() {
    // A `hits` whose value is not an object has no `.hits` array to shape; the
    // buffered path leaves it unchanged, and so must streaming.
    assert_matches_oracle_for_all_splits(br#"{"took":1,"hits":42}"#);
}

#[test]
fn root_hits_directly_an_array_passes_through() {
    // A degenerate root `hits` whose value is *directly* an array (not the real
    // `hits.hits` nesting OpenSearch emits) must NOT be shaped: the buffered oracle
    // only shapes the nested `hits.hits`, so a root-level array is forwarded
    // verbatim — `_tenant` and all. The scanner must agree (the array is entered
    // only at the inner level), or the two paths would diverge on this input.
    assert_matches_oracle_for_all_splits(
        br#"{"hits":[{"_index":"shared","_id":"acme:7","_source":{"_tenant":"acme"}}]}"#,
    );
}

#[test]
fn source_string_containing_structural_bytes() {
    // A `_source` string value that contains `]`, `}`, `"hits"`, and escaped
    // quotes must not confuse element framing.
    let body = br#"{"hits":{"hits":[
        {"_index":"shared","_id":"acme:1","_source":{"_tenant":"acme",
         "note":"close ] brace } and \"hits\":[fake] inside a string"}}
    ]}}"#;
    assert_matches_oracle_for_all_splits(body);
}

#[test]
fn sibling_object_with_its_own_hits_array_is_not_matched() {
    // A non-`hits` sibling that itself contains a `hits` array must be skipped
    // verbatim — only the top-level `hits.hits` is the target.
    let body = br#"{
        "decoy": { "hits": [ { "_source": { "_tenant": "acme" } } ] },
        "hits": { "hits": [ { "_index": "shared", "_id": "acme:7",
            "_source": { "_tenant": "acme", "msg": "real" } } ] }
    }"#;
    assert_matches_oracle_for_all_splits(body);
    // The decoy's `_tenant` is inside a skipped sibling, so it is forwarded
    // verbatim; only the real hit is stripped. Confirm the real hit lost it.
    let streamed = run_split(body, 3);
    let val: Value = serde_json::from_slice(&streamed).unwrap();
    assert!(val["hits"]["hits"][0]["_source"].get("_tenant").is_none());
    assert_eq!(val["hits"]["hits"][0]["_index"], "orders");
}

#[test]
fn id_is_mapped_back_to_logical() {
    let body = br#"{"hits":{"hits":[
        {"_index":"shared","_id":"acme:42","_routing":"acme","_source":{"_tenant":"acme"}}
    ]}}"#;
    let streamed = run_split(body, body.len());
    let val: Value = serde_json::from_slice(&streamed).unwrap();
    assert_eq!(val["hits"]["hits"][0]["_id"], "42");
}

#[test]
fn scalar_and_string_hit_elements_do_not_panic() {
    // Degenerate (non-object) elements cannot occur from OpenSearch, but the
    // scanner must frame them without panicking; they pass through unshaped,
    // matching the buffered path (`shape_hit` no-ops on a non-object).
    assert_matches_oracle_for_all_splits(br#"{"hits":{"hits":[1,"two",true,null]}}"#);
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Runs the scanner over `body` split into chunks of the given `sizes` (cycled
/// to cover the whole body), exercising arbitrary frame boundaries.
fn run_chunked(body: &[u8], sizes: &[usize]) -> Vec<u8> {
    let mut scanner = SearchHitsScanner::new(shaper());
    let mut out = Vec::new();
    let mut i = 0;
    let mut k = 0;
    while i < body.len() {
        let n = if sizes.is_empty() {
            body.len()
        } else {
            sizes[k % sizes.len()].max(1)
        };
        let end = (i + n).min(body.len());
        out.extend(scanner.feed(&body[i..end]));
        i = end;
        k += 1;
    }
    out.extend(scanner.finish());
    out
}

mod fuzz {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{json, Map};

    /// A bounded arbitrary JSON value — strings include `"`, `\`, `]`, `}` and
    /// other structural bytes, so the scanner's string/escape handling is fuzzed.
    fn json_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| json!(n)),
            ".{0,12}".prop_map(Value::String),
        ];
        leaf.prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                prop::collection::vec(("[a-z_]{1,6}", inner), 0..4)
                    .prop_map(|kvs| Value::Object(kvs.into_iter().collect())),
            ]
        })
    }

    /// One hit: physical `_index`/`_id`/`_routing` plus a `_source` object that
    /// always carries the injected `_tenant` (the thing the strip must remove)
    /// alongside arbitrary other fields.
    fn hit() -> impl Strategy<Value = Value> {
        (
            "[0-9]{1,4}",
            prop::collection::vec(("[a-z]{1,6}", json_value()), 0..4),
        )
            .prop_map(|(id, extra)| {
                let mut source = Map::new();
                source.insert("_tenant".to_owned(), json!("acme"));
                for (k, v) in extra {
                    source.insert(k, v);
                }
                json!({
                    "_index": "shared",
                    "_id": format!("acme:{id}"),
                    "_routing": "acme",
                    "_source": Value::Object(source),
                })
            })
    }

    /// The value placed at the top-level `hits` key. Weighted toward the real
    /// OpenSearch shape (`{total, hits: [..]}`), but deliberately also generates
    /// the degenerate shapes the buffered oracle handles by *not* shaping — a root
    /// `hits` that is directly an array, an inner `hits` that is not an array, and
    /// an arbitrary scalar/value — so the streamed↔buffered equality is fuzzed over
    /// them too (the regression that motivated `(2, b'[')` lived in exactly this
    /// class and was previously only a hand-written case).
    fn hits_field() -> impl Strategy<Value = Value> {
        prop_oneof![
            // The real shape: `hits.hits` is the array the proxy shapes.
            8 => prop::collection::vec(hit(), 0..5)
                .prop_map(|hits: Vec<Value>| json!({ "total": { "value": hits.len() }, "hits": hits })),
            // A root `hits` that is *directly* an array — no `hits.hits` to shape,
            // so it must pass through verbatim (`_tenant` and all).
            1 => prop::collection::vec(hit(), 0..5).prop_map(Value::Array),
            // An object whose inner `hits` is not an array (object/scalar): nothing
            // to shape.
            1 => json_value().prop_map(|inner| json!({ "total": 1, "hits": inner })),
            // An arbitrary value (scalar, array, or object that may itself nest a
            // `hits` key the scanner must not mistake for the target).
            1 => json_value(),
        ]
    }

    /// A full search-response envelope. Most cases are a well-formed object with
    /// optional siblings (`_shards`, an arbitrary `aggregations` that may itself
    /// nest a `hits` array, which must be skipped) wrapping an optional [`hits_field`];
    /// a fraction are a non-object root (array/scalar) with nothing to shape, to
    /// exercise the `SeekRoot`→passthrough path. Every case is valid JSON, so the
    /// oracle never errors and the assertion is pure streamed↔buffered equality.
    fn envelope() -> impl Strategy<Value = Value> {
        let structured = (
            proptest::option::of(hits_field()),
            proptest::option::of(json_value()),
            proptest::option::of(json_value()),
        )
            .prop_map(|(hits, shards, aggs)| {
                let mut top = Map::new();
                top.insert("took".to_owned(), json!(5));
                if let Some(s) = shards {
                    top.insert("_shards".to_owned(), s);
                }
                if let Some(h) = hits {
                    top.insert("hits".to_owned(), h);
                }
                if let Some(a) = aggs {
                    top.insert("aggregations".to_owned(), a);
                }
                Value::Object(top)
            });
        prop_oneof![
            10 => structured,
            // A non-object root: valid JSON with no object to scan, forwarded whole.
            1 => json_value(),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(400))]

        /// The keystone isolation guarantee: for any envelope and any chunk split,
        /// the streamed transform is semantically identical to the audited buffered
        /// `shape_hits` oracle — so no framing bug can leak an injected field or
        /// otherwise diverge from the proven path.
        #[test]
        fn streamed_matches_buffered_oracle(
            env in envelope(),
            sizes in prop::collection::vec(1usize..=9, 0..30),
        ) {
            let body = serde_json::to_vec(&env).unwrap();
            let oracle = shape_hits(&body, "orders", "acme", &make_shape()).expect("oracle ok");
            let oracle_val: Value = serde_json::from_slice(&oracle).unwrap();

            let streamed = run_chunked(&body, &sizes);
            let streamed_val: Value = serde_json::from_slice(&streamed)
                .expect("streamed output is valid json");
            prop_assert_eq!(streamed_val, oracle_val);
        }
    }
}
