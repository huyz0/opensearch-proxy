//! Pulling scalar values out of a JSON document by path.
//!
//! Used to find the partition id in a document body and to expand
//! `{body.<path>}` placeholders in an id template. The path is a sequence of
//! object keys (the small JSONPath subset of `docs/02` §2); the leaf must be a
//! scalar so it can become a string id component.

use serde_json::Value;

use crate::error::RewriteError;

/// Follows `segments` into `doc` and returns the leaf as a string, if the leaf
/// is a scalar (string, number, or bool).
///
/// Strings are returned as-is (not re-quoted); numbers and bools use their JSON
/// rendering, so the same document always yields the same id component.
///
/// # Errors
///
/// Returns [`RewriteError::PathNotScalar`] if any segment is missing or the
/// leaf is an object, array, or null.
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// use osproxy_rewrite::extract_scalar;
///
/// let doc = json!({ "meta": { "tenant": "acme" }, "n": 7 });
/// assert_eq!(extract_scalar(&doc, ["meta", "tenant"]).unwrap(), "acme");
/// assert_eq!(extract_scalar(&doc, ["n"]).unwrap(), "7");
/// assert!(extract_scalar(&doc, ["missing"]).is_err());
/// ```
pub fn extract_scalar<'a, I>(doc: &Value, segments: I) -> Result<String, RewriteError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut cursor = doc;
    let mut walked: Vec<&str> = Vec::new();
    for segment in segments {
        walked.push(segment);
        cursor = cursor
            .as_object()
            .and_then(|obj| obj.get(segment))
            .ok_or_else(|| RewriteError::PathNotScalar {
                path: walked.join("."),
            })?;
    }
    scalar_to_string(cursor).ok_or_else(|| RewriteError::PathNotScalar {
        path: walked.join("."),
    })
}

/// Renders a scalar JSON value as a string, or `None` for object/array/null.
fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_nested_string() {
        let doc = json!({ "a": { "b": "v" } });
        assert_eq!(extract_scalar(&doc, ["a", "b"]).unwrap(), "v");
    }

    #[test]
    fn renders_number_and_bool_deterministically() {
        let doc = json!({ "n": 42, "flag": true });
        assert_eq!(extract_scalar(&doc, ["n"]).unwrap(), "42");
        assert_eq!(extract_scalar(&doc, ["flag"]).unwrap(), "true");
    }

    #[test]
    fn missing_segment_reports_walked_path() {
        let doc = json!({ "a": { "b": "v" } });
        let err = extract_scalar(&doc, ["a", "c"]).unwrap_err();
        assert_eq!(
            err,
            RewriteError::PathNotScalar {
                path: "a.c".to_owned()
            }
        );
    }

    #[test]
    fn non_scalar_leaf_is_rejected() {
        let doc = json!({ "a": { "b": [1, 2] }, "obj": {}, "nil": null });
        assert!(extract_scalar(&doc, ["a", "b"]).is_err());
        assert!(extract_scalar(&doc, ["obj"]).is_err());
        assert!(extract_scalar(&doc, ["nil"]).is_err());
    }
}
