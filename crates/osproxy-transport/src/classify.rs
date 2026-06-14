//! Classifying an OpenSearch REST path into an [`EndpointKind`].
//!
//! A small, explicit matcher over the path segments — the supported matrix is
//! version-tracked in `docs/specs/opensearch-endpoints.md`. M1 fully handles
//! single-document ingest (`_doc`/`_create`); other shapes are classified so the
//! pipeline can reject them with a precise reason, not mis-handle them.

use osproxy_core::EndpointKind;
use osproxy_spi::HttpMethod;

/// The result of classifying a request path.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Classified {
    /// The endpoint class.
    pub endpoint: EndpointKind,
    /// The logical index (first path segment), empty if none.
    pub logical_index: String,
    /// The document id, if the path carries one.
    pub doc_id: Option<String>,
}

/// Classifies a `method` + `path` into an endpoint, logical index, and doc id.
///
/// The path's query string, if any, must already be stripped by the caller.
#[must_use]
pub fn classify(method: HttpMethod, path: &str) -> Classified {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match segments.as_slice() {
        // /{index}/_doc/{id} and /{index}/_create/{id}
        [index, verb @ ("_doc" | "_create"), id] => Classified {
            endpoint: by_id_endpoint(method, verb),
            logical_index: (*index).to_owned(),
            doc_id: Some((*id).to_owned()),
        },
        // /{index}/_doc (auto-id ingest)
        [index, "_doc"] => Classified {
            endpoint: doc_endpoint(method),
            logical_index: (*index).to_owned(),
            doc_id: None,
        },
        // /{index}/_search and /{index}/_count
        [index, "_search"] => classified(EndpointKind::Search, index),
        [index, "_count"] => classified(EndpointKind::Count, index),
        // /_bulk and /{index}/_bulk
        ["_bulk"] => Classified {
            endpoint: EndpointKind::IngestBulk,
            logical_index: String::new(),
            doc_id: None,
        },
        [index, "_bulk"] => classified(EndpointKind::IngestBulk, index),
        _ => Classified {
            endpoint: EndpointKind::Unknown,
            logical_index: segments
                .first()
                .map(|s| (*s).to_owned())
                .unwrap_or_default(),
            doc_id: None,
        },
    }
}

/// Endpoint for `/{index}/_doc/{id}` / `_create/{id}`, by method.
fn by_id_endpoint(method: HttpMethod, verb: &str) -> EndpointKind {
    match method {
        HttpMethod::Get | HttpMethod::Head => EndpointKind::GetById,
        HttpMethod::Delete => EndpointKind::DeleteById,
        // _create is always an ingest; _doc PUT/POST is ingest too.
        HttpMethod::Put | HttpMethod::Post if verb == "_create" || verb == "_doc" => {
            EndpointKind::IngestDoc
        }
        // PUT/POST of an unrecognized verb, or a future method: treat as
        // unsupported rather than mis-routing.
        _ => EndpointKind::Unknown,
    }
}

/// Endpoint for `/{index}/_doc` (no id), by method.
fn doc_endpoint(method: HttpMethod) -> EndpointKind {
    match method {
        HttpMethod::Post | HttpMethod::Put => EndpointKind::IngestDoc,
        _ => EndpointKind::Unknown,
    }
}

/// Helper for an endpoint that carries a logical index but no doc id.
fn classified(endpoint: EndpointKind, index: &str) -> Classified {
    Classified {
        endpoint,
        logical_index: index.to_owned(),
        doc_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_doc_with_id_is_ingest() {
        let c = classify(HttpMethod::Put, "/orders/_doc/acme:1");
        assert_eq!(c.endpoint, EndpointKind::IngestDoc);
        assert_eq!(c.logical_index, "orders");
        assert_eq!(c.doc_id.as_deref(), Some("acme:1"));
    }

    #[test]
    fn post_doc_without_id_is_ingest() {
        let c = classify(HttpMethod::Post, "/orders/_doc");
        assert_eq!(c.endpoint, EndpointKind::IngestDoc);
        assert_eq!(c.logical_index, "orders");
        assert!(c.doc_id.is_none());
    }

    #[test]
    fn get_and_delete_by_id_are_classified() {
        assert_eq!(
            classify(HttpMethod::Get, "/orders/_doc/1").endpoint,
            EndpointKind::GetById
        );
        assert_eq!(
            classify(HttpMethod::Delete, "/orders/_doc/1").endpoint,
            EndpointKind::DeleteById
        );
    }

    #[test]
    fn search_count_and_bulk() {
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_search").endpoint,
            EndpointKind::Search
        );
        assert_eq!(
            classify(HttpMethod::Get, "/orders/_count").endpoint,
            EndpointKind::Count
        );
        assert_eq!(
            classify(HttpMethod::Post, "/_bulk").endpoint,
            EndpointKind::IngestBulk
        );
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_bulk").endpoint,
            EndpointKind::IngestBulk
        );
    }

    #[test]
    fn unknown_paths_classify_as_unknown() {
        assert_eq!(
            classify(HttpMethod::Get, "/").endpoint,
            EndpointKind::Unknown
        );
        assert_eq!(
            classify(HttpMethod::Get, "/_cluster/health").endpoint,
            EndpointKind::Unknown
        );
    }

    #[test]
    fn create_verb_is_always_ingest() {
        assert_eq!(
            classify(HttpMethod::Put, "/orders/_create/1").endpoint,
            EndpointKind::IngestDoc
        );
    }
}
