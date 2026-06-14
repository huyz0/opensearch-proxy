//! Constructing a document `_id` from a template.
//!
//! The template grammar is two placeholders interleaved with literal text:
//! `{partition}` expands to the resolved partition id, and `{body.<path>}`
//! expands to a scalar pulled from the document at `<path>` (the JSONPath subset
//! of `docs/02` §2). Everything else is copied verbatim.
//!
//! In `SharedIndex` placement the partition id MUST appear in the id so ids
//! cannot collide across tenants (`docs/03`); that invariant is enforced one
//! level up, in the tenancy adapter, before this function is called.

use serde_json::Value;

use crate::error::RewriteError;
use crate::extract::extract_scalar;

/// Expands `template` against the resolved `partition` and the document `doc`.
///
/// # Errors
///
/// Returns [`RewriteError::PathNotScalar`] if a `{body.<path>}` placeholder does
/// not resolve to a scalar, or [`RewriteError::UnsupportedPlaceholder`] for any
/// placeholder other than `{partition}` or `{body.<path>}`.
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// use osproxy_rewrite::construct_id;
///
/// let doc = json!({ "order_id": 1001 });
/// let id = construct_id("{partition}:{body.order_id}", "acme", &doc).unwrap();
/// assert_eq!(id, "acme:1001");
/// ```
pub fn construct_id(template: &str, partition: &str, doc: &Value) -> Result<String, RewriteError> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let close = after
            .find('}')
            .ok_or_else(|| RewriteError::UnsupportedPlaceholder {
                placeholder: after.to_owned(),
            })?;
        let placeholder = &after[..close];
        out.push_str(&expand(placeholder, partition, doc)?);
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Expands a single placeholder (the text between `{` and `}`).
fn expand(placeholder: &str, partition: &str, doc: &Value) -> Result<String, RewriteError> {
    if placeholder == "partition" {
        return Ok(partition.to_owned());
    }
    if let Some(path) = placeholder.strip_prefix("body.") {
        return extract_scalar(doc, path.split('.'));
    }
    Err(RewriteError::UnsupportedPlaceholder {
        placeholder: placeholder.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn expands_partition_and_body_placeholders() {
        let doc = json!({ "k": "natural", "nested": { "v": 9 } });
        assert_eq!(
            construct_id("{partition}:{body.k}", "p1", &doc).unwrap(),
            "p1:natural"
        );
        assert_eq!(
            construct_id("{body.nested.v}-{partition}", "p1", &doc).unwrap(),
            "9-p1"
        );
    }

    #[test]
    fn literal_only_template_is_copied() {
        let doc = json!({});
        assert_eq!(construct_id("static-id", "p", &doc).unwrap(), "static-id");
    }

    #[test]
    fn unknown_placeholder_is_rejected() {
        let doc = json!({});
        assert_eq!(
            construct_id("{principal}", "p", &doc).unwrap_err(),
            RewriteError::UnsupportedPlaceholder {
                placeholder: "principal".to_owned()
            }
        );
    }

    #[test]
    fn unterminated_placeholder_is_rejected() {
        let doc = json!({});
        assert!(construct_id("{partition", "p", &doc).is_err());
    }

    #[test]
    fn missing_body_path_propagates_error() {
        let doc = json!({ "a": 1 });
        assert!(construct_id("{body.missing}", "p", &doc).is_err());
    }
}
