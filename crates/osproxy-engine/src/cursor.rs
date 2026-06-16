//! Scroll/PIT cursor-affinity body helpers (`docs/03` §6): recognizing a cursor
//! id in a request, substituting the real (unwrapped) id back into the upstream
//! body, and wrapping a `_scroll_id` in a response. Kept out of
//! [`crate::endpoints`] so that module stays within the file budget; the
//! cluster-resolution and dispatch live with the handler.

use osproxy_core::{ClusterId, CursorSigner};
use osproxy_spi::RequestCtx;

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

/// Identifies a cursor request and where its id lives: a path-form scroll id (in
/// the doc id), a body `scroll_id` (scroll continue/clear), or a body `id` (PIT
/// close, `DELETE /_pit`). `None` if the request carries no recognizable cursor.
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
    if let Some(id) = v.get("id").and_then(serde_json::Value::as_str) {
        return Some(CursorRequest {
            wrapped: id.to_owned(),
            upstream_path: "/_pit",
            id_field: "id",
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
