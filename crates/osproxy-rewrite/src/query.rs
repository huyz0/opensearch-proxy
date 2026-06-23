//! Wrapping a client search body in the mandatory partition filter.
//!
//! The shared-index isolation guarantee (`docs/03` §5) is enforced by nesting
//! the *entire* client query inside a `bool` whose `filter` pins the partition
//! field(s). Because the client query becomes the `must` clause of a bool the
//! proxy constructs, there is no syntactic way for it to escape the sibling
//! `filter`, the filter is not a suggestion the client can override, it is a
//! structural enclosure. This is the read-path counterpart of the write-path
//! field injection.

use std::collections::BTreeMap;

use osproxy_core::FieldName;
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::error::RewriteError;

/// Wraps the `query` of a client search body so every match is additionally
/// constrained by `filter` term(s) the client cannot remove.
///
/// The client's original query (or an implicit `match_all` when absent) becomes
/// the `must` clause of a freshly constructed `bool`; the partition `filter`
/// terms become its `filter` clause. All other top-level keys (`size`, `sort`,
/// `_source`, `aggs`, …) are preserved untouched.
///
/// When `filter` is non-empty (a shared index, where isolation depends on the
/// filter), the body is also screened for constructs that escape it, a `global`
/// aggregation or a `suggest` block, and rejected with [`RewriteError::Unfilterable`]
/// (`docs/03` §5, NFR-S4). With an empty `filter` (a dedicated index/cluster, the
/// whole target belongs to the partition) nothing is screened.
///
/// # Errors
///
/// Returns [`RewriteError::InvalidJson`] if `body` is non-empty but not valid
/// JSON, [`RewriteError::NotAnObject`] if it is not a JSON object, or
/// [`RewriteError::Unfilterable`] if a partition filter is in force and the body
/// carries a construct that would bypass it.
///
/// # Examples
///
/// ```
/// use osproxy_core::FieldName;
/// use serde_json::{json, Value};
/// use osproxy_rewrite::wrap_query;
///
/// let wrapped = wrap_query(
///     br#"{"query":{"match":{"msg":"hi"}}}"#,
///     &[(FieldName::from("_tenant"), Value::from("acme"))],
/// )
/// .unwrap();
/// let doc: Value = serde_json::from_slice(&wrapped).unwrap();
/// assert_eq!(doc["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
/// assert_eq!(doc["query"]["bool"]["must"][0]["match"]["msg"], "hi");
/// ```
pub fn wrap_query(body: &[u8], filter: &[(FieldName, Value)]) -> Result<Vec<u8>, RewriteError> {
    // Parse only the top level. Untouched sibling keys (`size`, `sort`, `aggs`, …)
    // and the client's own query stay as raw byte spans rather than being
    // materialized into `Value` trees, serde still fully validates the JSON and
    // proves the body is an object, so the isolation guarantee is unchanged; we
    // only avoid re-allocating subtrees the proxy does not inspect.
    let mut top = parse_top(body)?;

    // Isolation depends on the filter only when there is one (a shared index). If
    // so, refuse any sibling construct that OpenSearch evaluates outside the
    // query, a `global` aggregation or a `suggest` block, since the wrapping
    // `bool.filter` cannot constrain it (NFR-S4, `docs/03` §5).
    if !filter.is_empty() {
        reject_unfilterable(&top)?;
    }

    // The client's query becomes the inner `must`; absent means match-all. It is
    // re-embedded verbatim (its raw bytes), never re-serialized.
    let client_query = top.remove("query");
    let query = build_filtered_query(client_query.as_deref(), filter)?;
    top.insert("query".to_owned(), query);

    // RawValue values serialize as their raw bytes, so the preserved siblings are
    // copied out verbatim without a second parse.
    serde_json::to_vec(&top).map_err(|_| RewriteError::InvalidJson)
}

/// Builds the `{"bool":{"must":[…],"filter":[…]}}` subtree, embedding the client
/// query (if any) verbatim and the partition `filter` terms, as one [`RawValue`].
fn build_filtered_query(
    client_query: Option<&RawValue>,
    filter: &[(FieldName, Value)],
) -> Result<Box<RawValue>, RewriteError> {
    let mut q = Vec::with_capacity(64 + client_query.map_or(0, |q| q.get().len()));
    q.extend_from_slice(br#"{"bool":{"must":"#);
    match client_query {
        // The client query is a single `must` clause, embedded byte-for-byte.
        Some(raw) => {
            q.push(b'[');
            q.extend_from_slice(raw.get().as_bytes());
            q.push(b']');
        }
        None => q.extend_from_slice(b"[]"),
    }
    q.extend_from_slice(br#","filter":["#);
    for (i, (name, value)) in filter.iter().enumerate() {
        if i > 0 {
            q.push(b',');
        }
        q.extend_from_slice(br#"{"term":"#);
        // Serialize `{<name>: <value>}` with serde so the field name and value are
        // correctly quoted/escaped, never hand-rolled.
        let mut term = Map::with_capacity(1);
        term.insert(name.as_str().to_owned(), value.clone());
        serde_json::to_writer(&mut q, &term).map_err(|_| RewriteError::InvalidJson)?;
        q.push(b'}');
    }
    q.extend_from_slice(b"]}}");

    let s = String::from_utf8(q).map_err(|_| RewriteError::InvalidJson)?;
    RawValue::from_string(s).map_err(|_| RewriteError::InvalidJson)
}

/// Parses the search body's top-level object, with each value left as a raw byte
/// span. An empty body is an empty object (a bare `_search` is a match-all). A
/// valid-but-non-object body is [`RewriteError::NotAnObject`]; malformed JSON is
/// [`RewriteError::InvalidJson`].
fn parse_top(body: &[u8]) -> Result<BTreeMap<String, Box<RawValue>>, RewriteError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(BTreeMap::new());
    }
    match serde_json::from_slice::<BTreeMap<String, Box<RawValue>>>(body) {
        Ok(map) => Ok(map),
        // The map parse fails both for non-object JSON and for malformed JSON;
        // re-validate (cold path) as a raw value to tell the two apart so the
        // caller still gets a precise error.
        Err(_) => match serde_json::from_slice::<&RawValue>(body) {
            Ok(_) => Err(RewriteError::NotAnObject),
            Err(_) => Err(RewriteError::InvalidJson),
        },
    }
}

/// Rejects search bodies whose top level carries a construct that escapes the
/// partition filter (`docs/03` §5): a `suggest` block, or an `aggs`/`aggregations`
/// tree containing a `global` aggregation. Only the small request-side agg
/// *definition* is parsed (never the response), so the no-materialization posture
/// holds. Fail-closed: a malformed agg subtree is itself rejected.
fn reject_unfilterable(top: &BTreeMap<String, Box<RawValue>>) -> Result<(), RewriteError> {
    if top.contains_key("suggest") {
        return Err(RewriteError::Unfilterable {
            construct: "suggest",
        });
    }
    for key in ["aggs", "aggregations"] {
        if let Some(raw) = top.get(key) {
            let aggs: Value = serde_json::from_slice(raw.get().as_bytes())
                .map_err(|_| RewriteError::InvalidJson)?;
            if contains_global_agg(&aggs) {
                return Err(RewriteError::Unfilterable {
                    construct: "global aggregation",
                });
            }
        }
    }
    Ok(())
}

/// Whether an aggregations object contains a `global` bucket at any nesting
/// depth. An aggregation is `{"<name>": {"<type>": …, "aggs": {…}}}`; a `global`
/// agg is the one whose body has a `"global"` key. Recurses through nested
/// `aggs`/`aggregations` so a `global` buried under other buckets is still found.
fn contains_global_agg(aggs: &Value) -> bool {
    let Some(obj) = aggs.as_object() else {
        return false;
    };
    obj.values().any(|agg| {
        agg.as_object().is_some_and(|agg| {
            agg.contains_key("global")
                || ["aggs", "aggregations"]
                    .iter()
                    .filter_map(|k| agg.get(*k))
                    .any(contains_global_agg)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter() -> Vec<(FieldName, Value)> {
        vec![(FieldName::from("_tenant"), Value::from("acme"))]
    }

    #[test]
    fn client_query_is_nested_under_must_with_filter_sibling() {
        let wrapped = wrap_query(br#"{"query":{"match":{"msg":"hi"}}}"#, &filter()).unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(doc["query"]["bool"]["must"][0]["match"]["msg"], "hi");
        assert_eq!(doc["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
    }

    #[test]
    fn absent_query_becomes_filtered_match_all() {
        let wrapped = wrap_query(br#"{"size":5}"#, &filter()).unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        // No client query => empty `must`, but the filter still pins the tenant.
        assert_eq!(doc["query"]["bool"]["must"].as_array().unwrap().len(), 0);
        assert_eq!(doc["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
        // Unrelated top-level keys survive.
        assert_eq!(doc["size"], 5);
    }

    #[test]
    fn empty_body_is_a_filtered_match_all() {
        let wrapped = wrap_query(b"", &filter()).unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(doc["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
    }

    #[test]
    fn multiple_filter_terms_are_all_applied() {
        let wrapped = wrap_query(
            b"{}",
            &[
                (FieldName::from("_tenant"), Value::from("acme")),
                (FieldName::from("_region"), Value::from("eu")),
            ],
        )
        .unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        let terms = doc["query"]["bool"]["filter"].as_array().unwrap();
        assert_eq!(terms.len(), 2);
    }

    #[test]
    fn a_nested_query_key_is_not_confused_with_the_top_level_one() {
        // Only the *top-level* `query` is lifted into the bool. A `query` key
        // nested inside a sibling subtree must ride along untouched.
        let wrapped = wrap_query(
            br#"{"query":{"match":{"msg":"hi"}},"aggs":{"q":{"terms":{"field":"query"}}}}"#,
            &filter(),
        )
        .unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(doc["query"]["bool"]["must"][0]["match"]["msg"], "hi");
        // The nested aggregation (which itself mentions "query") survives verbatim.
        assert_eq!(doc["aggs"]["q"]["terms"]["field"], "query");
    }

    #[test]
    fn complex_sibling_subtrees_survive_verbatim() {
        let body = br#"{"size":5,"sort":[{"ts":"desc"},"_score"],"_source":["a","b"],"query":{"term":{"k":"v"}}}"#;
        let wrapped = wrap_query(body, &filter()).unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(doc["size"], 5);
        assert_eq!(doc["sort"][0]["ts"], "desc");
        assert_eq!(doc["sort"][1], "_score");
        assert_eq!(doc["_source"][1], "b");
        assert_eq!(doc["query"]["bool"]["must"][0]["term"]["k"], "v");
        assert_eq!(doc["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
    }

    #[test]
    fn escaped_and_unicode_content_in_the_client_query_is_preserved() {
        // Embedding the query verbatim must not corrupt escapes or non-ASCII.
        let body = "{\"query\":{\"match\":{\"msg\":\"a\\\"b\\\\c\\té \u{4e2d}\"}}}";
        let wrapped = wrap_query(body.as_bytes(), &filter()).unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(
            doc["query"]["bool"]["must"][0]["match"]["msg"],
            "a\"b\\c\t\u{e9} \u{4e2d}"
        );
    }

    #[test]
    fn a_non_string_filter_value_is_embedded_correctly() {
        let wrapped = wrap_query(
            br#"{"query":{"match_all":{}}}"#,
            &[
                (FieldName::from("_active"), Value::from(true)),
                (FieldName::from("_shard"), Value::from(7)),
            ],
        )
        .unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(doc["query"]["bool"]["filter"][0]["term"]["_active"], true);
        assert_eq!(doc["query"]["bool"]["filter"][1]["term"]["_shard"], 7);
    }

    #[test]
    fn a_global_aggregation_is_rejected_under_a_partition_filter() {
        // `global` ignores the query, so under a shared-index filter it would read
        // across partitions (NFR-S4). Reject it, even nested under other aggs.
        let body = br#"{"size":0,"aggs":{"outer":{"terms":{"field":"k"},"aggs":{"leak":{"global":{},"aggs":{"hits":{"top_hits":{"size":50}}}}}}}}"#;
        assert_eq!(
            wrap_query(body, &filter()).unwrap_err(),
            RewriteError::Unfilterable {
                construct: "global aggregation"
            }
        );
        // The `aggregations` spelling is caught too.
        let body = br#"{"aggregations":{"g":{"global":{}}}}"#;
        assert!(matches!(
            wrap_query(body, &filter()).unwrap_err(),
            RewriteError::Unfilterable { .. }
        ));
    }

    #[test]
    fn a_suggest_block_is_rejected_under_a_partition_filter() {
        let body = br#"{"suggest":{"s":{"text":"x","term":{"field":"msg"}}}}"#;
        assert_eq!(
            wrap_query(body, &filter()).unwrap_err(),
            RewriteError::Unfilterable {
                construct: "suggest"
            }
        );
    }

    #[test]
    fn ordinary_query_scoped_aggregations_are_allowed() {
        // A normal aggregation respects the wrapping `bool.filter`, so it stays.
        let body = br#"{"aggs":{"by_k":{"terms":{"field":"k"}}}}"#;
        let wrapped = wrap_query(body, &filter()).unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(doc["aggs"]["by_k"]["terms"]["field"], "k");
        assert_eq!(doc["query"]["bool"]["filter"][0]["term"]["_tenant"], "acme");
    }

    #[test]
    fn unfilterable_constructs_are_allowed_without_a_partition_filter() {
        // A dedicated index/cluster has no filter (the whole target is the
        // partition's), so a `global` agg or `suggest` is harmless and passes.
        let body = br#"{"aggs":{"g":{"global":{}}},"suggest":{"s":{"text":"x"}}}"#;
        let wrapped = wrap_query(body, &[]).unwrap();
        let doc: Value = serde_json::from_slice(&wrapped).unwrap();
        assert_eq!(doc["aggs"]["g"]["global"], serde_json::json!({}));
    }

    #[test]
    fn non_object_body_is_rejected() {
        assert_eq!(
            wrap_query(b"[1,2,3]", &filter()).unwrap_err(),
            RewriteError::NotAnObject
        );
        assert_eq!(
            wrap_query(b"not json", &filter()).unwrap_err(),
            RewriteError::InvalidJson
        );
    }
}
