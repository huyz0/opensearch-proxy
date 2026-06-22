//! Injecting tenancy fields on ingest and stripping them on read.
//!
//! The two operations are inverses: a field [`inject_fields`] adds is removed by
//! [`strip_fields`]. This symmetry is the heart of the shared-index isolation
//! model (`docs/03`) and is exercised by a round-trip property test.

use osproxy_core::json::object_top_level;
use osproxy_core::FieldName;
use serde_json::{Map, Value};

use crate::error::RewriteError;

/// Inserts each `(name, value)` into the top-level object of `doc`.
///
/// A field that already exists is a [`RewriteError::ReservedFieldCollision`],
/// not an overwrite: a client must not be able to pre-seed a tenancy field and
/// defeat isolation (`docs/03`).
///
/// # Errors
///
/// Returns [`RewriteError::NotAnObject`] if `doc` is not a JSON object, or
/// [`RewriteError::ReservedFieldCollision`] if a field is already present.
///
/// # Examples
///
/// ```
/// use serde_json::{json, Value};
/// use osproxy_core::FieldName;
/// use osproxy_rewrite::inject_fields;
///
/// let mut doc = json!({ "msg": "hi" });
/// inject_fields(&mut doc, &[(FieldName::from("_tenant"), Value::from("acme"))]).unwrap();
/// assert_eq!(doc["_tenant"], json!("acme"));
/// ```
pub fn inject_fields(doc: &mut Value, fields: &[(FieldName, Value)]) -> Result<(), RewriteError> {
    let obj = doc.as_object_mut().ok_or(RewriteError::NotAnObject)?;
    // Pre-check collisions so injection is all-or-nothing: a partial inject
    // would leave the document in a half-tenanted state.
    for (name, _) in fields {
        if obj.contains_key(name.as_str()) {
            return Err(RewriteError::ReservedFieldCollision {
                field: name.clone(),
            });
        }
    }
    for (name, value) in fields {
        obj.insert(name.as_str().to_owned(), value.clone());
    }
    Ok(())
}

/// Splices `fields` into the top level of the JSON object in `body`, returning
/// the new bytes, **without parsing `body` into a `Value` or re-serializing it**
/// (ADR-014). The body is scanned once for its top-level keys (to reject a
/// spoofed reserved field) and the injected fields are written right after the
/// opening `{`; the rest of the document is copied verbatim. The byte-level twin
/// of [`inject_fields`] for the streaming write path.
///
/// A field that already exists is a [`RewriteError::ReservedFieldCollision`], as
/// in [`inject_fields`]: a client must not pre-seed a tenancy field and defeat
/// isolation (`docs/03`). Escaped key names are decoded before the check, so the
/// collision cannot be smuggled past as `"_tenant"`.
///
/// # Errors
///
/// [`RewriteError::NotAnObject`] if `body` is not a JSON object,
/// [`RewriteError::InvalidJson`] if it is malformed, or
/// [`RewriteError::ReservedFieldCollision`] if an injected field is already
/// present.
///
/// # Examples
///
/// ```
/// use serde_json::Value;
/// use osproxy_core::FieldName;
/// use osproxy_rewrite::inject_fields_bytes;
///
/// let out = inject_fields_bytes(
///     br#"{"msg":"hi"}"#,
///     &[(FieldName::from("_tenant"), Value::from("acme"))],
/// ).unwrap();
/// assert_eq!(out, br#"{"_tenant":"acme","msg":"hi"}"#);
/// ```
pub fn inject_fields_bytes(
    body: &[u8],
    fields: &[(FieldName, Value)],
) -> Result<Vec<u8>, RewriteError> {
    let top = object_top_level(body)?;
    if fields.is_empty() {
        return Ok(body.to_vec());
    }
    for (name, _) in fields {
        if top.keys.iter().any(|k| k == name.as_str()) {
            return Err(RewriteError::ReservedFieldCollision {
                field: name.clone(),
            });
        }
    }
    let mut injected: Vec<u8> = Vec::new();
    for (idx, (name, value)) in fields.iter().enumerate() {
        if idx > 0 {
            injected.push(b',');
        }
        // Serializing a `&str` key and an in-memory `Value` into a `Vec` is
        // infallible (no I/O, no non-string map keys, no NaN); the error arms are
        // unreachable but kept so the splice fails closed rather than panics.
        serde_json::to_writer(&mut injected, name.as_str())
            .map_err(|_| RewriteError::InvalidJson)?;
        injected.push(b':');
        serde_json::to_writer(&mut injected, value).map_err(|_| RewriteError::InvalidJson)?;
    }
    let mut out = Vec::with_capacity(body.len() + injected.len() + 1);
    out.extend_from_slice(&body[..top.insert_at]);
    out.extend_from_slice(&injected);
    if !top.empty {
        out.push(b',');
    }
    out.extend_from_slice(&body[top.insert_at..]);
    Ok(out)
}

/// Injects the tenancy fields into the `doc` and `upsert` sub-objects of an
/// `_update` body (`docs/04` §3).
///
/// An update never replaces a whole document, so the fields are stamped into
/// whichever sub-documents are present: a partial `doc` (re-asserting the
/// tenancy fields, harmless on an existing doc) and the `upsert` (so an upsert
/// that *creates* the document still carries its isolation fields). A sub-key
/// that is absent is skipped; a `script`-only update with no `upsert` injects
/// nothing (the targeted document already carries the fields).
///
/// # Errors
///
/// Returns [`RewriteError::NotAnObject`] if `update` itself, or a present
/// `doc`/`upsert`, is not a JSON object, or
/// [`RewriteError::ReservedFieldCollision`] if a sub-document already contains an
/// injected field (a client must not pre-seed a tenancy field, `docs/03`).
pub fn inject_update(
    update: &mut Value,
    fields: &[(FieldName, Value)],
) -> Result<(), RewriteError> {
    let obj = update.as_object_mut().ok_or(RewriteError::NotAnObject)?;
    for key in ["doc", "upsert"] {
        if let Some(sub) = obj.get_mut(key) {
            inject_fields(sub, fields)?;
        }
    }
    Ok(())
}

/// Removes each named field from the top-level object of `doc`, if present.
///
/// The inverse of [`inject_fields`]. Lenient by design: stripping a field that
/// is absent (or a non-object body) is a no-op, because the read path must
/// never fail just because a document predates a tenancy field.
///
/// Returns the number of fields actually removed (for a strip/inject symmetry
/// assertion and observability).
pub fn strip_fields(doc: &mut Value, names: &[FieldName]) -> usize {
    let Some(obj): Option<&mut Map<String, Value>> = doc.as_object_mut() else {
        return 0;
    };
    names
        .iter()
        .filter(|name| obj.remove(name.as_str()).is_some())
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inject_then_strip_restores_original() {
        let original = json!({ "msg": "hi", "n": 3 });
        let mut doc = original.clone();
        let injected = [
            (FieldName::from("_tenant"), Value::from("acme")),
            (FieldName::from("_epoch"), Value::from(5)),
        ];
        inject_fields(&mut doc, &injected).unwrap();
        assert_eq!(doc["_tenant"], json!("acme"));
        let names: Vec<_> = injected.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(strip_fields(&mut doc, &names), 2);
        assert_eq!(doc, original);
    }

    #[test]
    fn collision_is_rejected_and_leaves_doc_untouched() {
        let mut doc = json!({ "_tenant": "evil", "msg": "hi" });
        let err = inject_fields(
            &mut doc,
            &[(FieldName::from("_tenant"), Value::from("acme"))],
        )
        .unwrap_err();
        assert_eq!(
            err,
            RewriteError::ReservedFieldCollision {
                field: FieldName::from("_tenant")
            }
        );
        // Untouched: the spoofed value is still there (caller rejects the request).
        assert_eq!(doc["_tenant"], json!("evil"));
    }

    #[test]
    fn inject_update_stamps_doc_and_upsert() {
        let mut update = json!({
            "doc": { "msg": "hi" },
            "upsert": { "msg": "new" },
        });
        inject_update(
            &mut update,
            &[(FieldName::from("_tenant"), Value::from("acme"))],
        )
        .unwrap();
        assert_eq!(update["doc"]["_tenant"], json!("acme"));
        assert_eq!(update["upsert"]["_tenant"], json!("acme"));
    }

    #[test]
    fn inject_update_rejects_spoofed_tenancy_field() {
        let mut update = json!({ "upsert": { "_tenant": "evil" } });
        assert_eq!(
            inject_update(
                &mut update,
                &[(FieldName::from("_tenant"), Value::from("acme"))],
            )
            .unwrap_err(),
            RewriteError::ReservedFieldCollision {
                field: FieldName::from("_tenant")
            }
        );
    }

    #[test]
    fn inject_update_is_a_noop_without_doc_or_upsert() {
        let mut update = json!({ "script": { "source": "ctx._source.n++" } });
        inject_update(
            &mut update,
            &[(FieldName::from("_tenant"), Value::from("acme"))],
        )
        .unwrap();
        assert_eq!(update["script"]["source"], "ctx._source.n++");
    }

    #[test]
    fn inject_into_non_object_fails() {
        let mut doc = json!([1, 2, 3]);
        assert_eq!(
            inject_fields(&mut doc, &[(FieldName::from("x"), Value::from(1))]).unwrap_err(),
            RewriteError::NotAnObject
        );
    }

    #[test]
    fn strip_is_lenient_on_absent_and_non_object() {
        let mut doc = json!({ "msg": "hi" });
        assert_eq!(strip_fields(&mut doc, &[FieldName::from("_tenant")]), 0);
        let mut arr = json!([1]);
        assert_eq!(strip_fields(&mut arr, &[FieldName::from("x")]), 0);
    }
}
