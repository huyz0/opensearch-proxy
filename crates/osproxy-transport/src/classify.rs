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
    // No classified endpoint has more than three meaningful path segments
    // (`/{index}/{verb}/{id}`), and the `Admin` arm only inspects the first. So
    // collect at most four segments onto the stack — the fourth's mere presence
    // forces anything longer than a three-segment shape to the `Unknown`/`Admin`
    // arms, exactly as a full `Vec` would, but without a per-request heap
    // allocation (classify runs on every request).
    let mut buf = [""; 4];
    let mut count = 0usize;
    for seg in path.split('/').filter(|s| !s.is_empty()) {
        if count < buf.len() {
            buf[count] = seg;
        }
        count += 1;
    }
    let segments = &buf[..count.min(buf.len())];
    match segments {
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
        // Cursor lifecycle — scroll & PIT, bound to the cluster that created them
        // (`docs/03` §6). These carry a wrapped cursor the engine unwraps to route
        // to the pinned cluster; the path-form scroll id rides in `doc_id`.
        //   /_search/scroll (body-form scroll continue/clear) and /_pit (PIT
        //   delete) — both carry the wrapped cursor in the body, no logical index.
        ["_search", "scroll"] | ["_pit"] => classified(EndpointKind::Cursor, ""),
        //   /_search/scroll/{scroll_id} (path-form continue/clear)
        ["_search", "scroll", scroll_id] => Classified {
            endpoint: EndpointKind::Cursor,
            logical_index: String::new(),
            doc_id: Some((*scroll_id).to_owned()),
        },
        //   /{index}/_pit (create — resolves the index's cluster, wraps the id)
        [index, "_pit"] => classified(EndpointKind::Cursor, index),
        // /_search with no index — a PIT search (the PIT defines the index set);
        // the engine reads the `pit` in the body and routes to its pinned cluster.
        ["_search"] => classified(EndpointKind::Search, ""),
        // /{index}/_search and /{index}/_count
        [index, "_search"] => classified(EndpointKind::Search, index),
        [index, "_count"] => classified(EndpointKind::Count, index),
        // /_mget and /{index}/_mget
        ["_mget"] => classified(EndpointKind::MultiGet, ""),
        [index, "_mget"] => classified(EndpointKind::MultiGet, index),
        // /_msearch and /{index}/_msearch
        ["_msearch"] => classified(EndpointKind::MultiSearch, ""),
        [index, "_msearch"] => classified(EndpointKind::MultiSearch, index),
        // /_bulk and /{index}/_bulk
        ["_bulk"] => Classified {
            endpoint: EndpointKind::IngestBulk,
            logical_index: String::new(),
            doc_id: None,
        },
        [index, "_bulk"] => classified(EndpointKind::IngestBulk, index),
        // /{index}/_delete_by_query — only honorable in async fan-out mode, where
        // the engine expands it to a delete per match; rejected otherwise
        // (`docs/04` §9). `_update_by_query` is intentionally NOT classified — it
        // needs a scripted read-modify-write the proxy cannot do, so it falls
        // through to `Unknown` and is rejected.
        [index, "_delete_by_query"] => classified(EndpointKind::DeleteByQuery, index),
        // Administrative endpoints (`_cat/*`, `_cluster/*`, `_nodes/*`): no tenancy
        // semantics, classified `Admin` so the engine can pass them through to an
        // operator-allow-listed cluster, or reject (the default). The full path is
        // forwarded verbatim, so no segment is captured (`docs/specs/
        // opensearch-endpoints.md`). Placed last so it cannot shadow a tenancy path.
        [first, ..] if matches!(*first, "_cat" | "_cluster" | "_nodes") => {
            classified(EndpointKind::Admin, "")
        }
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
            classify(HttpMethod::Post, "/_mget").endpoint,
            EndpointKind::MultiGet
        );
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_mget").endpoint,
            EndpointKind::MultiGet
        );
        assert_eq!(
            classify(HttpMethod::Post, "/_msearch").endpoint,
            EndpointKind::MultiSearch
        );
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_msearch").endpoint,
            EndpointKind::MultiSearch
        );
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_bulk").endpoint,
            EndpointKind::IngestBulk
        );
    }

    #[test]
    fn scroll_and_pit_paths_are_cursor() {
        // Scroll continue/clear — body form and path form.
        assert_eq!(
            classify(HttpMethod::Post, "/_search/scroll").endpoint,
            EndpointKind::Cursor
        );
        let path_form = classify(HttpMethod::Get, "/_search/scroll/c2Nyb2xs");
        assert_eq!(path_form.endpoint, EndpointKind::Cursor);
        assert_eq!(path_form.doc_id.as_deref(), Some("c2Nyb2xs"));
        assert!(
            classify(HttpMethod::Delete, "/_search/scroll")
                .logical_index
                .is_empty(),
            "scroll clear carries no logical index"
        );
        // PIT create resolves the named index's cluster; PIT delete does not.
        let pit_create = classify(HttpMethod::Post, "/orders/_pit");
        assert_eq!(pit_create.endpoint, EndpointKind::Cursor);
        assert_eq!(pit_create.logical_index, "orders");
        let pit_delete = classify(HttpMethod::Delete, "/_pit");
        assert_eq!(pit_delete.endpoint, EndpointKind::Cursor);
        assert!(pit_delete.logical_index.is_empty());
    }

    #[test]
    fn a_no_index_search_classifies_as_search() {
        // `POST /_search` (no index) is a PIT search; the engine reads the body.
        let c = classify(HttpMethod::Post, "/_search");
        assert_eq!(c.endpoint, EndpointKind::Search);
        assert!(c.logical_index.is_empty());
    }

    #[test]
    fn a_real_search_is_not_mistaken_for_a_cursor() {
        // `_search` on an index is a normal search; only `_search/scroll` is a
        // cursor, so the new arms must not shadow the search arm.
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_search").endpoint,
            EndpointKind::Search
        );
    }

    #[test]
    fn admin_endpoints_classify_as_admin() {
        for path in ["/_cat/indices", "/_cluster/health", "/_nodes/stats"] {
            let c = classify(HttpMethod::Get, path);
            assert_eq!(c.endpoint, EndpointKind::Admin, "{path}");
            assert!(
                c.logical_index.is_empty(),
                "{path} carries no logical index"
            );
        }
        // An index literally named `_catalog` is not an admin path (prefix-exact).
        assert_eq!(
            classify(HttpMethod::Post, "/_catalog/_search").endpoint,
            EndpointKind::Search
        );
    }

    #[test]
    fn unknown_paths_classify_as_unknown() {
        assert_eq!(
            classify(HttpMethod::Get, "/").endpoint,
            EndpointKind::Unknown
        );
        // `_cluster/*` is now classified `Admin` (see `admin_endpoints_*`), so use
        // a genuinely unmatched proxy path here.
        assert_eq!(
            classify(HttpMethod::Get, "/_sql").endpoint,
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
