//! Scroll/PIT cursor-affinity body helpers (`docs/03` §6): recognizing a cursor
//! id in a request, substituting the real (unwrapped) id back into the upstream
//! body, and wrapping a `_scroll_id` in a response. Kept out of
//! [`crate::endpoints`] so that module stays within the file budget; the
//! cluster-resolution and dispatch live with the handler.

use osproxy_core::{ClusterId, CursorSigner};
use osproxy_spi::RequestCtx;

/// The only client query params the proxy forwards upstream — the cursor
/// lifecycle knobs. Everything else (notably query-affecting params like `q`,
/// `source`, `analyzer`) is dropped, so a client cannot bypass the mandatory
/// body partition filter via the URL (NFR-S4).
const FORWARDABLE_PARAMS: &[&str] = &["scroll", "keep_alive"];

/// Filters a raw query string down to the [`FORWARDABLE_PARAMS`] allow-list,
/// preserving each kept `key=value` pair verbatim. Returns `None` if nothing
/// survives, so a plain search appends no query at all.
pub(crate) fn forwardable_query(raw: Option<&str>) -> Option<String> {
    let kept: Vec<&str> = raw?
        .split('&')
        .filter(|pair| {
            let key = pair.split('=').next().unwrap_or(pair);
            FORWARDABLE_PARAMS.contains(&key)
        })
        .collect();
    if kept.is_empty() {
        None
    } else {
        Some(kept.join("&"))
    }
}

/// Whether a response body mentions `_scroll_id` at all — a cheap pre-filter so a
/// plain (non-scroll) search never pays for a JSON parse just to find none.
pub(crate) fn has_scroll_id(body: &[u8]) -> bool {
    const NEEDLE: &[u8] = b"_scroll_id";
    body.windows(NEEDLE.len()).any(|w| w == NEEDLE)
}

/// Replaces the response's `_scroll_id` with its signed envelope (`cluster` + id).
/// Returns the body unchanged if it is not JSON or carries no string
/// `_scroll_id`, so a malformed or scroll-less response is never corrupted.
pub(crate) fn wrap_scroll_id_in_response(
    body: Vec<u8>,
    signer: &dyn CursorSigner,
    cluster: &ClusterId,
) -> Vec<u8> {
    let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return body;
    };
    let Some(id) = v.get("_scroll_id").and_then(serde_json::Value::as_str) else {
        return body;
    };
    let wrapped = osproxy_core::cursor::wrap(signer, cluster, id);
    v["_scroll_id"] = serde_json::Value::String(wrapped);
    serde_json::to_vec(&v).unwrap_or(body)
}

/// The wrapped PIT id from a search body (`{"pit":{"id": <wrapped>}}`), if present
/// — the marker that a search is pinned to a point-in-time and must route to the
/// PIT's cluster rather than resolve a fresh target.
pub(crate) fn pit_id_in_body(body: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    v.get("pit")?
        .get("id")?
        .as_str()
        .map(std::borrow::ToOwned::to_owned)
}

/// The search body with the real (unwrapped) PIT id substituted into
/// `pit.id`, leaving the query (already partition-filtered) untouched. Returns the
/// body unchanged if it has no `pit` object.
pub(crate) fn rewrite_pit_id(body: Vec<u8>, real_id: &str) -> Vec<u8> {
    let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return body;
    };
    let Some(pit) = v.get_mut("pit").and_then(serde_json::Value::as_object_mut) else {
        return body;
    };
    pit.insert(
        "id".to_owned(),
        serde_json::Value::String(real_id.to_owned()),
    );
    serde_json::to_vec(&v).unwrap_or(body)
}

/// Replaces a response's top-level `pit_id` with its signed envelope (`cluster` +
/// id), so the client's later PIT search/close recovers the cluster. Used for both
/// a PIT create response and a PIT search response (which echoes a refreshed
/// `pit_id`). Returns the body unchanged if it has no string `pit_id`.
pub(crate) fn wrap_pit_id_in_response(
    body: Vec<u8>,
    signer: &dyn CursorSigner,
    cluster: &ClusterId,
) -> Vec<u8> {
    let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return body;
    };
    let Some(id) = v.get("pit_id").and_then(serde_json::Value::as_str) else {
        return body;
    };
    let wrapped = osproxy_core::cursor::wrap(signer, cluster, id);
    v["pit_id"] = serde_json::Value::String(wrapped);
    serde_json::to_vec(&v).unwrap_or(body)
}

/// The wrapped PIT ids from a delete body (`{"pit_id": [<wrapped>, ...]}`), the
/// OpenSearch close-PIT shape (`DELETE /_search/point_in_time`). Returns `None`
/// when the body carries no `pit_id` array, so a scroll clear (which uses
/// `scroll_id`) is not mistaken for a PIT close.
pub(crate) fn pit_ids_in_delete_body(body: &[u8]) -> Option<Vec<String>> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    let ids: Vec<String> = v
        .get("pit_id")?
        .as_array()?
        .iter()
        .filter_map(|x| x.as_str().map(std::borrow::ToOwned::to_owned))
        .collect();
    (!ids.is_empty()).then_some(ids)
}

/// A cursor continue/clear/close request, recovered from a scroll or PIT request:
/// the wrapped id plus where to substitute the real id and which upstream
/// endpoint to forward it to.
pub(crate) struct CursorRequest {
    /// The wrapped affinity envelope (to unwrap into cluster + real id).
    pub(crate) wrapped: String,
    /// The upstream endpoint this cursor op targets.
    pub(crate) upstream_path: &'static str,
    /// The body field the real (unwrapped) id is substituted into.
    pub(crate) id_field: &'static str,
}

/// Identifies a **scroll** cursor request and where its id lives: a path-form
/// scroll id (in the doc id) or a body `scroll_id` (scroll continue/clear).
/// `None` if the request carries no recognizable scroll cursor. PIT close is
/// handled separately ([`pit_ids_in_delete_body`]) because its body shape is a
/// `pit_id` array that may span clusters.
pub(crate) fn cursor_request(ctx: &RequestCtx<'_>) -> Option<CursorRequest> {
    if let Some(id) = ctx.doc_id() {
        return Some(CursorRequest {
            wrapped: id.to_owned(),
            upstream_path: "/_search/scroll",
            id_field: "scroll_id",
        });
    }
    let v: serde_json::Value = serde_json::from_slice(ctx.body()).ok()?;
    if let Some(id) = v.get("scroll_id").and_then(serde_json::Value::as_str) {
        return Some(CursorRequest {
            wrapped: id.to_owned(),
            upstream_path: "/_search/scroll",
            id_field: "scroll_id",
        });
    }
    None
}

/// The upstream cursor body with the real (unwrapped) id substituted into
/// `id_field`, preserving any other fields the client sent (e.g. a `scroll`
/// keep-alive); falls back to a minimal body for the path form (which has none).
pub(crate) fn rewrite_cursor_body(client_body: &[u8], id_field: &str, real_id: &str) -> Vec<u8> {
    let mut v = serde_json::from_slice::<serde_json::Value>(client_body)
        .ok()
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| serde_json::json!({}));
    v[id_field] = serde_json::Value::String(real_id.to_owned());
    serde_json::to_vec(&v)
        .unwrap_or_else(|_| format!(r#"{{"{id_field}":"{real_id}"}}"#).into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_cursor_params_are_forwarded_not_query_affecting_ones() {
        // The isolation guard (NFR-S4): `scroll`/`keep_alive` pass; `q`, `source`,
        // and anything else that could override the body partition filter is
        // dropped — a client cannot bypass tenancy via the URL.
        assert_eq!(
            forwardable_query(Some("scroll=1m")).as_deref(),
            Some("scroll=1m")
        );
        assert_eq!(
            forwardable_query(Some("keep_alive=5m")).as_deref(),
            Some("keep_alive=5m")
        );
        // `q` is dropped even when bundled with an allowed param.
        assert_eq!(
            forwardable_query(Some("q=*&scroll=1m")).as_deref(),
            Some("scroll=1m"),
            "a query-string search param must never reach the upstream"
        );
        assert_eq!(forwardable_query(Some("q=*")), None);
        assert_eq!(forwardable_query(Some("source={}&analyzer=x")), None);
        assert_eq!(forwardable_query(None), None);
        assert_eq!(forwardable_query(Some("")), None);
    }

    #[test]
    fn pit_delete_body_yields_the_wrapped_id_array() {
        // The OpenSearch close shape is a `pit_id` array; a scroll clear (which
        // uses `scroll_id`) must not be mistaken for a PIT close.
        assert_eq!(
            pit_ids_in_delete_body(br#"{"pit_id":["a","b"]}"#),
            Some(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(pit_ids_in_delete_body(br#"{"pit_id":[]}"#), None);
        assert_eq!(pit_ids_in_delete_body(br#"{"scroll_id":"x"}"#), None);
        assert_eq!(pit_ids_in_delete_body(b"not json"), None);
    }
}
