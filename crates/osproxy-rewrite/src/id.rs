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

use osproxy_core::json::scalar_at_path;
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
    construct_id_with(template, partition, |path| {
        extract_scalar(doc, path.split('.'))
    })
}

/// Expands `template` against the resolved `partition` and the raw document
/// `body`, reading `{body.<path>}` scalars straight from the bytes — **without
/// parsing `body` into a `Value`** (ADR-014). The byte-level twin of
/// [`construct_id`] for the streaming write path.
///
/// String leaves are decoded; number and bool leaves use their source text.
///
/// # Errors
///
/// As [`construct_id`], plus [`RewriteError::InvalidJson`] if `body` up to a
/// referenced leaf is malformed.
///
/// # Examples
///
/// ```
/// use osproxy_rewrite::construct_id_bytes;
///
/// let id = construct_id_bytes("{partition}:{body.order_id}", "acme", br#"{"order_id":1001}"#)
///     .unwrap();
/// assert_eq!(id, "acme:1001");
/// ```
pub fn construct_id_bytes(
    template: &str,
    partition: &str,
    body: &[u8],
) -> Result<String, RewriteError> {
    construct_id_with(template, partition, |path| {
        scalar_at_path(body, path.split('.')).map_err(RewriteError::from)
    })
}

/// Walks `template`, expanding `{partition}` and resolving each `{body.<path>}`
/// placeholder through `resolve_body`. Shared by [`construct_id`] (over a
/// `Value`) and [`construct_id_bytes`] (over raw bytes).
fn construct_id_with<F>(
    template: &str,
    partition: &str,
    resolve_body: F,
) -> Result<String, RewriteError>
where
    F: Fn(&str) -> Result<String, RewriteError>,
{
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
        if placeholder == "partition" {
            out.push_str(partition);
        } else if let Some(path) = placeholder.strip_prefix("body.") {
            out.push_str(&resolve_body(path)?);
        } else {
            return Err(RewriteError::UnsupportedPlaceholder {
                placeholder: placeholder.to_owned(),
            });
        }
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Maps a client-supplied **logical** id to the **physical** id stored in
/// OpenSearch, by substituting it for the template's single `{body.<path>}`
/// placeholder and expanding `{partition}` (`docs/04` §5).
///
/// This is the read-path inverse of [`construct_id`]: on write the physical id
/// is built from the document body; on read the client knows only the logical
/// (natural) id, and the proxy reconstructs the same physical id to fetch it.
///
/// # Errors
///
/// Returns [`RewriteError::IrreversibleIdTemplate`] if the template does not
/// contain exactly one `{body.<path>}` placeholder, or
/// [`RewriteError::UnsupportedPlaceholder`] for an unknown placeholder.
///
/// # Examples
///
/// ```
/// use osproxy_rewrite::map_logical_to_physical;
///
/// let physical = map_logical_to_physical("{partition}:{body.id}", "acme", "7").unwrap();
/// assert_eq!(physical, "acme:7");
/// ```
pub fn map_logical_to_physical(
    template: &str,
    partition: &str,
    logical_id: &str,
) -> Result<String, RewriteError> {
    let (prefix, suffix) = id_frame(template, partition)?;
    Ok(format!("{prefix}{logical_id}{suffix}"))
}

/// Maps a **physical** id back to the client-facing **logical** id, the inverse
/// of [`map_logical_to_physical`], by stripping the template's literal frame.
///
/// Returns `None` if `physical_id` does not fit the frame (e.g. it belongs to a
/// different partition), so a caller can fall back to the physical id rather
/// than mis-report one.
///
/// # Errors
///
/// Returns [`RewriteError::IrreversibleIdTemplate`] (or
/// [`RewriteError::UnsupportedPlaceholder`]) if the template itself is not a
/// valid reversible id template.
///
/// # Examples
///
/// ```
/// use osproxy_rewrite::map_physical_to_logical;
///
/// let logical = map_physical_to_logical("{partition}:{body.id}", "acme", "acme:7").unwrap();
/// assert_eq!(logical.as_deref(), Some("7"));
/// ```
pub fn map_physical_to_logical(
    template: &str,
    partition: &str,
    physical_id: &str,
) -> Result<Option<String>, RewriteError> {
    let (prefix, suffix) = id_frame(template, partition)?;
    Ok(physical_id
        .strip_prefix(&prefix)
        .and_then(|rest| rest.strip_suffix(&suffix))
        .map(str::to_owned))
}

/// Renders the template's literal frame around its single `{body.<path>}`
/// placeholder, with `{partition}` expanded: returns `(prefix, suffix)` such
/// that `prefix + <natural key> + suffix` is the physical id.
///
/// Exactly one body placeholder is required for the mapping to be reversible.
fn id_frame(template: &str, partition: &str) -> Result<(String, String), RewriteError> {
    let mut prefix = String::new();
    let mut suffix = String::new();
    let mut seen_body = false;
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let literal = &rest[..open];
        let after = &rest[open + 1..];
        let close = after
            .find('}')
            .ok_or_else(|| RewriteError::UnsupportedPlaceholder {
                placeholder: after.to_owned(),
            })?;
        let placeholder = &after[..close];
        let frame = if seen_body { &mut suffix } else { &mut prefix };
        frame.push_str(literal);
        if placeholder == "partition" {
            frame.push_str(partition);
        } else if placeholder.strip_prefix("body.").is_some() {
            if seen_body {
                return Err(RewriteError::IrreversibleIdTemplate);
            }
            seen_body = true;
        } else {
            return Err(RewriteError::UnsupportedPlaceholder {
                placeholder: placeholder.to_owned(),
            });
        }
        rest = &after[close + 1..];
    }
    if seen_body {
        suffix.push_str(rest);
    } else {
        return Err(RewriteError::IrreversibleIdTemplate);
    }
    Ok((prefix, suffix))
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

    #[test]
    fn logical_to_physical_substitutes_natural_key() {
        assert_eq!(
            map_logical_to_physical("{partition}:{body.id}", "acme", "7").unwrap(),
            "acme:7"
        );
        assert_eq!(
            map_logical_to_physical("doc-{body.k}@{partition}", "p1", "abc").unwrap(),
            "doc-abc@p1"
        );
    }

    #[test]
    fn physical_to_logical_strips_the_frame() {
        assert_eq!(
            map_physical_to_logical("{partition}:{body.id}", "acme", "acme:7").unwrap(),
            Some("7".to_owned())
        );
        // A physical id from a different partition does not fit the frame.
        assert_eq!(
            map_physical_to_logical("{partition}:{body.id}", "acme", "other:7").unwrap(),
            None
        );
    }

    #[test]
    fn mapping_round_trips_for_arbitrary_natural_keys() {
        let template = "{partition}:{body.natural}";
        for key in ["1001", "a-b", "", "x:y"] {
            let physical = map_logical_to_physical(template, "acme", key).unwrap();
            assert_eq!(
                map_physical_to_logical(template, "acme", &physical).unwrap(),
                Some(key.to_owned())
            );
        }
    }

    #[test]
    fn templates_without_exactly_one_body_placeholder_are_irreversible() {
        assert_eq!(
            map_logical_to_physical("{partition}:static", "p", "x").unwrap_err(),
            RewriteError::IrreversibleIdTemplate
        );
        assert_eq!(
            map_logical_to_physical("{body.a}-{body.b}", "p", "x").unwrap_err(),
            RewriteError::IrreversibleIdTemplate
        );
    }
}
