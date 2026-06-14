//! Wrapping a client search body in the mandatory partition filter.
//!
//! The shared-index isolation guarantee (`docs/03` §5) is enforced by nesting
//! the *entire* client query inside a `bool` whose `filter` pins the partition
//! field(s). Because the client query becomes the `must` clause of a bool the
//! proxy constructs, there is no syntactic way for it to escape the sibling
//! `filter` — the filter is not a suggestion the client can override, it is a
//! structural enclosure. This is the read-path counterpart of the write-path
//! field injection.

use osproxy_core::FieldName;
use serde_json::{json, Map, Value};

use crate::error::RewriteError;

/// Wraps the `query` of a client search body so every match is additionally
/// constrained by `filter` term(s) the client cannot remove.
///
/// The client's original query (or an implicit `match_all` when absent) becomes
/// the `must` clause of a freshly constructed `bool`; the partition `filter`
/// terms become its `filter` clause. All other top-level keys (`size`, `sort`,
/// `_source`, `aggs`, …) are preserved untouched.
///
/// # Errors
///
/// Returns [`RewriteError::InvalidJson`] if `body` is non-empty but not valid
/// JSON, or [`RewriteError::NotAnObject`] if it is not a JSON object.
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
    let mut root = parse_root(body)?;

    // The client's query becomes the inner `must`; absent means match-all.
    let client_query = root.remove("query");
    let must = client_query.map_or_else(Vec::new, |q| vec![q]);
    let filter_terms: Vec<Value> = filter
        .iter()
        .map(|(name, value)| json!({ "term": { name.as_str(): value } }))
        .collect();

    root.insert(
        "query".to_owned(),
        json!({ "bool": { "must": must, "filter": filter_terms } }),
    );
    serde_json::to_vec(&Value::Object(root)).map_err(|_| RewriteError::InvalidJson)
}

/// Parses the search body into its top-level object, treating an empty body as
/// an empty object (a bare `_search` with no body is a match-all).
fn parse_root(body: &[u8]) -> Result<Map<String, Value>, RewriteError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Map::new());
    }
    match serde_json::from_slice::<Value>(body).map_err(|_| RewriteError::InvalidJson)? {
        Value::Object(map) => Ok(map),
        _ => Err(RewriteError::NotAnObject),
    }
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
