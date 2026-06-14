//! Parsing the `_msearch` NDJSON body into structured per-search requests.
//!
//! `_msearch` (multi-search) is newline-delimited JSON in *header/body* pairs:
//! a header line (`{"index":"a"}`, possibly empty) followed by the search body
//! line (`{"query":{…}}`). This module turns the raw bytes into a
//! `Vec<MsearchItem>` the engine wraps in the partition filter and demuxes by
//! target (`docs/04` §4). Like [`parse_bulk`](crate::parse_bulk) it is a pure
//! parse with no tenancy meaning.

use serde_json::Value;

use crate::error::RewriteError;

/// One parsed multi-search request: the optional explicit `index` from the
/// header line (else the URL default), and the raw query body line.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MsearchItem {
    /// The explicit `index` from the header line, if any (else the URL default).
    pub index: Option<String>,
    /// The raw search body (the line after the header), forwarded once wrapped.
    pub query: Vec<u8>,
}

/// Parses an `_msearch` NDJSON body into its ordered searches.
///
/// Each search is a header line followed by a body line. The header's `index`
/// may be a string or the first entry of an array (OpenSearch accepts both);
/// any other shape leaves the index defaulted to the URL.
///
/// # Errors
///
/// Returns [`RewriteError::InvalidJson`] if a header or body line is not valid
/// JSON, or [`RewriteError::MalformedBulkAction`] if a header line has no
/// following body line.
///
/// # Examples
///
/// ```
/// use osproxy_rewrite::parse_msearch;
///
/// let body = b"{\"index\":\"a\"}\n{\"query\":{\"match_all\":{}}}\n";
/// let items = parse_msearch(body).unwrap();
/// assert_eq!(items.len(), 1);
/// assert_eq!(items[0].index.as_deref(), Some("a"));
/// ```
pub fn parse_msearch(body: &[u8]) -> Result<Vec<MsearchItem>, RewriteError> {
    let mut items = Vec::new();
    let mut lines = body
        .split(|&b| b == b'\n')
        .filter(|l| !l.iter().all(u8::is_ascii_whitespace));
    while let Some(header_line) = lines.next() {
        let header: Value =
            serde_json::from_slice(header_line).map_err(|_| RewriteError::InvalidJson)?;
        let body_line = lines.next().ok_or(RewriteError::MalformedBulkAction)?;
        // Validate the body is JSON, but forward the original bytes verbatim.
        serde_json::from_slice::<Value>(body_line).map_err(|_| RewriteError::InvalidJson)?;
        items.push(MsearchItem {
            index: header_index(&header),
            query: body_line.to_vec(),
        });
    }
    Ok(items)
}

/// The `index` named in a header line, accepting a string or an array's first
/// string entry (a missing or other-shaped value defaults to the URL index).
fn header_index(header: &Value) -> Option<String> {
    match header.get("index") {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Array(a)) => a.first().and_then(Value::as_str).map(str::to_owned),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn parses_header_body_pairs_in_order() {
        let body = concat!(
            "{\"index\":\"a\"}\n",
            "{\"query\":{\"match_all\":{}}}\n",
            "{}\n",
            "{\"query\":{\"term\":{\"k\":1}}}\n",
        );
        let items = parse_msearch(body.as_bytes()).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].index.as_deref(), Some("a"));
        let q0: Value = serde_json::from_slice(&items[0].query).unwrap();
        assert_eq!(q0["query"]["match_all"], serde_json::json!({}));
        assert_eq!(items[1].index, None);
        let q1: Value = serde_json::from_slice(&items[1].query).unwrap();
        assert_eq!(q1["query"]["term"]["k"], 1);
    }

    #[test]
    fn index_array_takes_the_first_entry() {
        let body = "{\"index\":[\"a\",\"b\"]}\n{\"query\":{\"match_all\":{}}}\n";
        let items = parse_msearch(body.as_bytes()).unwrap();
        assert_eq!(items[0].index.as_deref(), Some("a"));
    }

    #[test]
    fn header_without_body_is_rejected() {
        assert_eq!(
            parse_msearch(b"{\"index\":\"a\"}\n").unwrap_err(),
            RewriteError::MalformedBulkAction
        );
    }

    #[test]
    fn invalid_json_header_or_body_is_rejected() {
        assert_eq!(
            parse_msearch(b"not json\n{}\n").unwrap_err(),
            RewriteError::InvalidJson
        );
        assert_eq!(
            parse_msearch(b"{}\nnot json\n").unwrap_err(),
            RewriteError::InvalidJson
        );
    }

    #[test]
    fn blank_lines_between_pairs_are_skipped() {
        let body = "\n{}\n{\"query\":{\"match_all\":{}}}\n\n";
        let items = parse_msearch(body.as_bytes()).unwrap();
        assert_eq!(items.len(), 1);
    }
}
