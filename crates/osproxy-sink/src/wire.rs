//! Mapping a [`DocOp`] to an OpenSearch REST request, and parsing its result.
//!
//! Pure wire concerns split out of [`crate::opensearch`] so the sink itself
//! stays focused on connection handling: which verb/URI a document op targets
//! (`_doc`/`_create`/`_update`), minimal URI-segment encoding, and reading the
//! `_id`/`result` back out of a single-doc response.

use bytes::Bytes;
use hyper::{Method, Request};
use osproxy_core::IndexName;
use serde_json::Value;

use crate::ack::OpResult;
use crate::batch::DocOp;
use crate::error::SinkError;
use crate::opensearch::{buffered, ByteBody};

/// Builds the upstream request for a document op, returning it together with the
/// id to fall back to if the response omits `_id`.
pub(crate) fn build_request(
    base: &str,
    index: &IndexName,
    doc: &DocOp,
) -> Result<(Request<ByteBody>, String), SinkError> {
    let (method, uri, body, fallback_id) = request_parts(base, index, doc);

    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        // `body` is a `Bytes`: cloning it out of the borrowed `doc` above (so the
        // op survives a retry) is a refcount bump, not a copy of the document.
        .body(buffered(body))
        .map_err(|_| SinkError::Transport {
            kind: "building upstream request",
        })?;
    Ok((req, fallback_id))
}

/// Selects the `(method, uri, body, fallback_id)` for a document op. `create`
/// targets `_create` (fail-if-exists), `update` targets `_update`; `index`/
/// `delete` use `_doc`.
fn request_parts(base: &str, index: &IndexName, doc: &DocOp) -> (Method, String, Bytes, String) {
    match doc {
        DocOp::Index {
            id: Some(id),
            routing,
            body,
        } => (
            Method::PUT,
            doc_uri(base, index, Some(id), routing.as_deref()),
            body.clone(),
            id.clone(),
        ),
        DocOp::Index {
            id: None,
            routing,
            body,
        } => (
            Method::POST,
            doc_uri(base, index, None, routing.as_deref()),
            body.clone(),
            String::new(),
        ),
        DocOp::Create {
            id: Some(id),
            routing,
            body,
        } => (
            Method::PUT,
            action_uri(base, index, "_create", Some(id), routing.as_deref()),
            body.clone(),
            id.clone(),
        ),
        DocOp::Create {
            id: None,
            routing,
            body,
        } => (
            Method::POST,
            create_auto_uri(base, index, routing.as_deref()),
            body.clone(),
            String::new(),
        ),
        DocOp::Update { id, routing, body } => (
            Method::POST,
            action_uri(base, index, "_update", Some(id), routing.as_deref()),
            body.clone(),
            id.clone(),
        ),
        DocOp::Delete { id, routing } => (
            Method::DELETE,
            doc_uri(base, index, Some(id), routing.as_deref()),
            Bytes::new(),
            id.clone(),
        ),
    }
}

/// Constructs the `_doc` URI, optionally with an id path segment and a `routing`
/// query parameter.
pub(crate) fn doc_uri(
    base: &str,
    index: &IndexName,
    id: Option<&str>,
    routing: Option<&str>,
) -> String {
    action_uri(base, index, "_doc", id, routing)
}

/// Constructs an action URI `{base}/{index}/{verb}[/{id}][?routing=…]` for a
/// document-level write (`_doc`/`_create`/`_update`).
fn action_uri(
    base: &str,
    index: &IndexName,
    verb: &str,
    id: Option<&str>,
    routing: Option<&str>,
) -> String {
    let mut uri = format!("{base}/{}/{verb}", index.as_str());
    if let Some(id) = id {
        uri.push('/');
        uri.push_str(&encode(id));
    }
    if let Some(routing) = routing {
        uri.push_str("?routing=");
        uri.push_str(&encode(routing));
    }
    uri
}

/// Constructs the auto-id create URI: `_doc` with `op_type=create` so OpenSearch
/// fails on an id collision rather than replacing.
fn create_auto_uri(base: &str, index: &IndexName, routing: Option<&str>) -> String {
    let mut uri = format!("{base}/{}/_doc?op_type=create", index.as_str());
    if let Some(routing) = routing {
        uri.push_str("&routing=");
        uri.push_str(&encode(routing));
    }
    uri
}

/// Minimal percent-encoding for the characters that would break a URI path or
/// query segment. Partition ids and constructed ids are normally URL-safe; this
/// keeps a stray space or `#`/`?`/`/` from producing a malformed request.
fn encode(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b':' => {
                out.push(byte as char);
            }
            other => {
                out.push('%');
                out.push(HEX[(other >> 4) as usize] as char);
                out.push(HEX[(other & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// Parses an OpenSearch single-doc response into an [`OpResult`], falling back to
/// `fallback_id` when the response body omits `_id` (e.g. a delete or an error).
pub(crate) fn parse_result(body: &[u8], fallback_id: String, status: u16) -> OpResult {
    let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let id = parsed
        .get("_id")
        .and_then(Value::as_str)
        .map_or(fallback_id, str::to_owned);
    let created = parsed.get("result").and_then(Value::as_str) == Some("created");
    OpResult::new(id, status, created)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_uri_includes_id_and_routing() {
        let idx = IndexName::from("orders");
        assert_eq!(
            doc_uri("http://h:9200", &idx, Some("acme:1"), Some("acme")),
            "http://h:9200/orders/_doc/acme:1?routing=acme"
        );
        assert_eq!(
            doc_uri("http://h:9200", &idx, None, None),
            "http://h:9200/orders/_doc"
        );
    }

    #[test]
    fn create_and_update_uris_target_their_endpoints() {
        let idx = IndexName::from("orders");
        assert_eq!(
            action_uri(
                "http://h:9200",
                &idx,
                "_create",
                Some("acme:1"),
                Some("acme")
            ),
            "http://h:9200/orders/_create/acme:1?routing=acme"
        );
        assert_eq!(
            action_uri("http://h:9200", &idx, "_update", Some("acme:1"), None),
            "http://h:9200/orders/_update/acme:1"
        );
        assert_eq!(
            create_auto_uri("http://h:9200", &idx, Some("acme")),
            "http://h:9200/orders/_doc?op_type=create&routing=acme"
        );
    }

    #[test]
    fn encode_escapes_unsafe_bytes_only() {
        assert_eq!(encode("acme:1001"), "acme:1001");
        assert_eq!(encode("a b/c"), "a%20b%2Fc");
    }

    #[test]
    fn parse_result_reads_id_and_created() {
        let body = br#"{"_id":"acme:1","result":"created"}"#;
        let r = parse_result(body, "fallback".to_owned(), 201);
        assert_eq!(r.id, "acme:1");
        assert!(r.created);
        assert!(r.is_success());
    }

    #[test]
    fn parse_result_falls_back_when_id_absent() {
        let r = parse_result(b"{}", "del-id".to_owned(), 200);
        assert_eq!(r.id, "del-id");
        assert!(!r.created);
    }

    fn doc_body() -> Bytes {
        Bytes::from_static(br#"{"a":1}"#)
    }

    #[test]
    fn request_parts_for_index_with_id_puts_doc_with_routing() {
        let idx = IndexName::from("orders");
        let (method, uri, _body, fallback) = request_parts(
            "http://h:9200",
            &idx,
            &DocOp::Index {
                id: Some("acme:1".to_owned()),
                routing: Some("acme".to_owned()),
                body: doc_body(),
            },
        );
        assert_eq!(method, Method::PUT);
        assert_eq!(uri, "http://h:9200/orders/_doc/acme:1?routing=acme");
        assert_eq!(fallback, "acme:1");
    }

    #[test]
    fn request_parts_for_index_with_no_id_posts_doc() {
        let idx = IndexName::from("orders");
        let (method, uri, _body, fallback) = request_parts(
            "http://h:9200",
            &idx,
            &DocOp::Index {
                id: None,
                routing: None,
                body: doc_body(),
            },
        );
        assert_eq!(method, Method::POST);
        assert_eq!(uri, "http://h:9200/orders/_doc");
        assert_eq!(fallback, "");
    }

    #[test]
    fn request_parts_for_create_with_id_puts_create_fail_if_exists() {
        let idx = IndexName::from("orders");
        let (method, uri, _body, fallback) = request_parts(
            "http://h:9200",
            &idx,
            &DocOp::Create {
                id: Some("acme:2".to_owned()),
                routing: None,
                body: doc_body(),
            },
        );
        assert_eq!(method, Method::PUT);
        assert_eq!(uri, "http://h:9200/orders/_create/acme:2");
        assert_eq!(fallback, "acme:2");
    }

    #[test]
    fn request_parts_for_create_with_no_id_posts_op_type_create() {
        let idx = IndexName::from("orders");
        let (method, uri, _body, fallback) = request_parts(
            "http://h:9200",
            &idx,
            &DocOp::Create {
                id: None,
                routing: Some("acme".to_owned()),
                body: doc_body(),
            },
        );
        assert_eq!(method, Method::POST);
        assert_eq!(uri, "http://h:9200/orders/_doc?op_type=create&routing=acme");
        assert_eq!(fallback, "");
    }

    #[test]
    fn request_parts_for_update_posts_update() {
        let idx = IndexName::from("orders");
        let (method, uri, _body, fallback) = request_parts(
            "http://h:9200",
            &idx,
            &DocOp::Update {
                id: "acme:3".to_owned(),
                routing: None,
                body: doc_body(),
            },
        );
        assert_eq!(method, Method::POST);
        assert_eq!(uri, "http://h:9200/orders/_update/acme:3");
        assert_eq!(fallback, "acme:3");
    }

    #[test]
    fn request_parts_for_delete_sends_no_body() {
        let idx = IndexName::from("orders");
        let (method, uri, body, fallback) = request_parts(
            "http://h:9200",
            &idx,
            &DocOp::Delete {
                id: "acme:4".to_owned(),
                routing: Some("acme".to_owned()),
            },
        );
        assert_eq!(method, Method::DELETE);
        assert_eq!(uri, "http://h:9200/orders/_doc/acme:4?routing=acme");
        assert!(body.is_empty());
        assert_eq!(fallback, "acme:4");
    }

    #[test]
    fn build_request_carries_the_content_type_and_body() {
        let idx = IndexName::from("orders");
        let (req, fallback) = build_request(
            "http://h:9200",
            &idx,
            &DocOp::Index {
                id: Some("acme:1".to_owned()),
                routing: None,
                body: Bytes::from_static(br#"{"a":1}"#),
            },
        )
        .unwrap();
        assert_eq!(req.method(), Method::PUT);
        assert_eq!(
            req.headers().get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(fallback, "acme:1");
    }
}
