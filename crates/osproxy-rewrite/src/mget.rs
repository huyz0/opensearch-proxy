//! Parsing the `_mget` body into structured per-document fetches.
//!
//! `_mget` (multi-get) carries either a `docs` array of `{_index,_id,routing}`
//! objects or a bare `ids` array (with the index taken from the URL). Like
//! [`parse_bulk`](crate::parse_bulk) this is a pure parse with no tenancy
//! meaning: the engine resolves each item's partition and demuxes by target
//! (`docs/04` §5). Held to the same coverage bar as the other transforms.

use serde_json::Value;

use crate::error::RewriteError;

/// One parsed multi-get fetch: the optional explicit `_index` (else the URL
/// default), the document `_id`, and the optional `routing`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MgetItem {
    /// The explicit `_index` from the doc entry, if any (else the URL default).
    pub index: Option<String>,
    /// The logical document id to fetch.
    pub id: String,
    /// The explicit `routing` from the doc entry, if any.
    pub routing: Option<String>,
}

/// Parses an `_mget` body into its ordered fetches.
///
/// Accepts both shapes OpenSearch supports: `{"docs":[{"_index":…,"_id":…},…]}`
/// and `{"ids":["1","2"]}` (index defaulted from the URL).
///
/// # Errors
///
/// Returns [`RewriteError::InvalidJson`] if the body is not valid JSON,
/// [`RewriteError::NotAnObject`] if it is not an object carrying `docs` or
/// `ids`, or [`RewriteError::MalformedBulkAction`] if a `docs` entry is not an
/// object with a string `_id` (or an `ids` entry is not a string).
///
/// # Examples
///
/// ```
/// use osproxy_rewrite::parse_mget;
///
/// let body = br#"{"docs":[{"_index":"a","_id":"1"},{"_id":"2"}]}"#;
/// let items = parse_mget(body).unwrap();
/// assert_eq!(items.len(), 2);
/// assert_eq!(items[0].index.as_deref(), Some("a"));
/// assert_eq!(items[1].id, "2");
/// ```
pub fn parse_mget(body: &[u8]) -> Result<Vec<MgetItem>, RewriteError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| RewriteError::InvalidJson)?;
    let obj = value.as_object().ok_or(RewriteError::NotAnObject)?;

    if let Some(docs) = obj.get("docs").and_then(Value::as_array) {
        docs.iter().map(parse_doc_entry).collect()
    } else if let Some(ids) = obj.get("ids").and_then(Value::as_array) {
        ids.iter().map(parse_id_entry).collect()
    } else {
        Err(RewriteError::NotAnObject)
    }
}

/// Parses one `docs` entry into an [`MgetItem`].
fn parse_doc_entry(entry: &Value) -> Result<MgetItem, RewriteError> {
    let obj = entry.as_object().ok_or(RewriteError::MalformedBulkAction)?;
    let str_field = |name: &str| obj.get(name).and_then(Value::as_str).map(str::to_owned);
    let id = str_field("_id").ok_or(RewriteError::MalformedBulkAction)?;
    Ok(MgetItem {
        index: str_field("_index"),
        id,
        routing: str_field("routing"),
    })
}

/// Parses one bare `ids` entry (a string id, index from the URL).
fn parse_id_entry(entry: &Value) -> Result<MgetItem, RewriteError> {
    let id = entry
        .as_str()
        .ok_or(RewriteError::MalformedBulkAction)?
        .to_owned();
    Ok(MgetItem {
        index: None,
        id,
        routing: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_docs_form_in_order() {
        let body = br#"{"docs":[
            {"_index":"a","_id":"1","routing":"r"},
            {"_id":"2"}
        ]}"#;
        let items = parse_mget(body).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].index.as_deref(), Some("a"));
        assert_eq!(items[0].id, "1");
        assert_eq!(items[0].routing.as_deref(), Some("r"));
        assert_eq!(items[1].index, None);
        assert_eq!(items[1].id, "2");
        assert_eq!(items[1].routing, None);
    }

    #[test]
    fn parses_ids_form() {
        let items = parse_mget(br#"{"ids":["7","8"]}"#).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].id, "7");
        assert!(items[0].index.is_none());
        assert_eq!(items[1].id, "8");
    }

    #[test]
    fn doc_entry_without_id_is_rejected() {
        assert_eq!(
            parse_mget(br#"{"docs":[{"_index":"a"}]}"#).unwrap_err(),
            RewriteError::MalformedBulkAction
        );
    }

    #[test]
    fn non_string_id_entry_is_rejected() {
        assert_eq!(
            parse_mget(br#"{"ids":[1]}"#).unwrap_err(),
            RewriteError::MalformedBulkAction
        );
    }

    #[test]
    fn body_without_docs_or_ids_is_not_an_object_request() {
        assert_eq!(
            parse_mget(br#"{"other":1}"#).unwrap_err(),
            RewriteError::NotAnObject
        );
        assert_eq!(
            parse_mget(br"[1,2]").unwrap_err(),
            RewriteError::NotAnObject
        );
    }

    #[test]
    fn invalid_json_is_rejected() {
        assert_eq!(
            parse_mget(b"not json").unwrap_err(),
            RewriteError::InvalidJson
        );
    }
}
