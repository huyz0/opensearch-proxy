//! Parsing the `_bulk` NDJSON body into structured per-document actions.
//!
//! `_bulk` is newline-delimited JSON: each operation is an *action* line
//! (`{"index":{"_id":"1"}}`) optionally followed by a *source* line (the
//! document, for index/create/update; absent for delete). This module turns the
//! raw bytes into a `Vec<BulkItem>` the engine demuxes by partition (`docs/04`
//! §3). It is a pure parse — no tenancy meaning — held to the same coverage bar
//! as the other transforms.

use osproxy_core::json;
use serde_json::Value;

use crate::error::RewriteError;

/// The action of a bulk operation, mirroring OpenSearch's verbs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BulkAction {
    /// Index (create-or-replace) a document.
    Index,
    /// Create a document, failing if it already exists.
    Create,
    /// Partial-update / scripted-update a document.
    Update,
    /// Delete a document by id.
    Delete,
}

impl BulkAction {
    /// Whether this action carries a source line after its action line.
    #[must_use]
    pub fn has_source(self) -> bool {
        !matches!(self, Self::Delete)
    }

    /// The action's wire keyword (`index`/`create`/`update`/`delete`), used as
    /// the per-item key in the bulk response.
    #[must_use]
    pub fn keyword(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// One parsed bulk operation: its action, the optional explicit `_index`/`_id`/
/// `routing` from the action line, and the source document (if any).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BulkItem {
    /// The operation verb.
    pub action: BulkAction,
    /// The explicit `_index` from the action line, if any (else the URL default).
    pub index: Option<String>,
    /// The explicit `_id` from the action line, if any.
    pub id: Option<String>,
    /// The explicit `routing` from the action line, if any.
    pub routing: Option<String>,
    /// Whether the action line carries an optimistic-concurrency precondition
    /// (`if_seq_no`/`if_primary_term`/`version`/`version_type`). The async
    /// fan-out path rejects such items: the precondition is evaluated against the
    /// live version, which does not exist at enqueue time (`docs/04` §9).
    pub concurrency_control: bool,
    /// The source document as **raw bytes** (for index/create/update; `None` for
    /// delete). Kept verbatim — not parsed into a `Value` — so the per-item
    /// transform can scan and splice it without materializing a tree (ADR-014).
    pub source: Option<Vec<u8>>,
}

/// Parses an NDJSON `_bulk` body into its ordered operations.
///
/// # Errors
///
/// Returns [`RewriteError::InvalidJson`] if an action or source line is not
/// valid JSON, or [`RewriteError::MalformedBulkAction`] if an action line is not
/// a single-key `{verb: {…}}` object, names an unknown verb, or a source line is
/// missing for an action that requires one.
///
/// # Examples
///
/// ```
/// use osproxy_rewrite::{parse_bulk, BulkAction};
///
/// let body = b"{\"index\":{\"_id\":\"1\"}}\n{\"msg\":\"hi\"}\n";
/// let items = parse_bulk(body).unwrap();
/// assert_eq!(items.len(), 1);
/// assert_eq!(items[0].action, BulkAction::Index);
/// assert_eq!(items[0].id.as_deref(), Some("1"));
/// assert_eq!(items[0].source.as_deref(), Some(&b"{\"msg\":\"hi\"}"[..]));
/// ```
pub fn parse_bulk(body: &[u8]) -> Result<Vec<BulkItem>, RewriteError> {
    let mut items = Vec::new();
    let mut lines = body
        .split(|&b| b == b'\n')
        .filter(|l| !l.iter().all(u8::is_ascii_whitespace));
    while let Some(action_line) = lines.next() {
        let (action, meta) = parse_action_line(action_line)?;
        let source = if action.has_source() {
            let source_line = lines.next().ok_or(RewriteError::MalformedBulkAction)?;
            // Validate the line is well-formed JSON (no alloc), but keep the raw
            // bytes — the transform splices them later without a `Value` tree.
            json::validate(source_line).map_err(|_| RewriteError::InvalidJson)?;
            Some(source_line.to_vec())
        } else {
            None
        };
        items.push(BulkItem {
            action,
            index: meta.index,
            id: meta.id,
            routing: meta.routing,
            concurrency_control: meta.concurrency_control,
            source,
        });
    }
    Ok(items)
}

/// The `_index`/`_id`/`routing` pulled from an action line's metadata object.
struct ActionMeta {
    index: Option<String>,
    id: Option<String>,
    routing: Option<String>,
    concurrency_control: bool,
}

/// Parses one action line into its action and metadata.
fn parse_action_line(line: &[u8]) -> Result<(BulkAction, ActionMeta), RewriteError> {
    let value: Value = serde_json::from_slice(line).map_err(|_| RewriteError::InvalidJson)?;
    let obj = value.as_object().ok_or(RewriteError::MalformedBulkAction)?;
    // Exactly one key: the action verb mapping to its metadata object.
    let mut entries = obj.iter();
    let (verb, meta) = entries.next().ok_or(RewriteError::MalformedBulkAction)?;
    if entries.next().is_some() {
        return Err(RewriteError::MalformedBulkAction);
    }
    let action = match verb.as_str() {
        "index" => BulkAction::Index,
        "create" => BulkAction::Create,
        "update" => BulkAction::Update,
        "delete" => BulkAction::Delete,
        _ => return Err(RewriteError::MalformedBulkAction),
    };
    Ok((action, action_meta(meta)))
}

/// Extracts `_index`/`_id`/`routing` from an action's metadata object (lenient:
/// a missing or non-object meta yields all-`None`).
fn action_meta(meta: &Value) -> ActionMeta {
    let str_field = |name: &str| meta.get(name).and_then(Value::as_str).map(str::to_owned);
    let concurrency_control = ["if_seq_no", "if_primary_term", "version", "version_type"]
        .iter()
        .any(|k| meta.get(*k).is_some());
    ActionMeta {
        index: str_field("_index"),
        id: str_field("_id"),
        routing: str_field("routing"),
        concurrency_control,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Parses an item's raw source bytes back into a `Value` for assertions.
    fn source_json(item: &BulkItem) -> Value {
        serde_json::from_slice(item.source.as_ref().unwrap()).unwrap()
    }

    #[test]
    fn parses_index_create_delete_in_order() {
        let body = concat!(
            "{\"index\":{\"_index\":\"a\",\"_id\":\"1\"}}\n",
            "{\"msg\":\"one\"}\n",
            "{\"create\":{\"_id\":\"2\"}}\n",
            "{\"msg\":\"two\"}\n",
            "{\"delete\":{\"_id\":\"3\"}}\n",
        );
        let items = parse_bulk(body.as_bytes()).unwrap();
        assert_eq!(items.len(), 3);

        assert_eq!(items[0].action, BulkAction::Index);
        assert_eq!(items[0].index.as_deref(), Some("a"));
        assert_eq!(items[0].id.as_deref(), Some("1"));
        assert_eq!(source_json(&items[0])["msg"], json!("one"));

        assert_eq!(items[1].action, BulkAction::Create);
        assert_eq!(source_json(&items[1])["msg"], json!("two"));

        assert_eq!(items[2].action, BulkAction::Delete);
        assert_eq!(items[2].id.as_deref(), Some("3"));
        assert!(items[2].source.is_none());
    }

    #[test]
    fn optimistic_concurrency_metadata_is_flagged() {
        let body = concat!(
            "{\"index\":{\"_id\":\"1\",\"if_seq_no\":3,\"if_primary_term\":1}}\n{\"k\":1}\n",
            "{\"index\":{\"_id\":\"2\",\"version\":7}}\n{\"k\":2}\n",
            "{\"index\":{\"_id\":\"3\"}}\n{\"k\":3}\n",
        );
        let items = parse_bulk(body.as_bytes()).unwrap();
        assert!(items[0].concurrency_control, "if_seq_no/if_primary_term");
        assert!(items[1].concurrency_control, "version");
        assert!(!items[2].concurrency_control, "plain index");
    }

    #[test]
    fn routing_is_read_from_the_action_line() {
        let body = "{\"index\":{\"_id\":\"1\",\"routing\":\"r\"}}\n{\"k\":1}\n";
        let items = parse_bulk(body.as_bytes()).unwrap();
        assert_eq!(items[0].routing.as_deref(), Some("r"));
    }

    #[test]
    fn blank_lines_are_skipped() {
        let body = "\n{\"delete\":{\"_id\":\"9\"}}\n\n";
        let items = parse_bulk(body.as_bytes()).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].action, BulkAction::Delete);
    }

    #[test]
    fn missing_source_line_is_rejected() {
        let body = "{\"index\":{\"_id\":\"1\"}}\n"; // no source follows
        assert_eq!(
            parse_bulk(body.as_bytes()).unwrap_err(),
            RewriteError::MalformedBulkAction
        );
    }

    #[test]
    fn unknown_verb_and_multikey_action_are_rejected() {
        assert_eq!(
            parse_bulk(b"{\"frobnicate\":{}}\n").unwrap_err(),
            RewriteError::MalformedBulkAction
        );
        assert_eq!(
            parse_bulk(b"{\"index\":{},\"delete\":{}}\n").unwrap_err(),
            RewriteError::MalformedBulkAction
        );
    }

    #[test]
    fn invalid_json_action_is_rejected() {
        assert_eq!(
            parse_bulk(b"not json\n").unwrap_err(),
            RewriteError::InvalidJson
        );
    }

    #[test]
    fn has_source_and_keyword_match_the_action() {
        assert!(BulkAction::Index.has_source());
        assert!(!BulkAction::Delete.has_source());
        assert_eq!(BulkAction::Create.keyword(), "create");
        assert_eq!(BulkAction::Update.keyword(), "update");
    }
}
