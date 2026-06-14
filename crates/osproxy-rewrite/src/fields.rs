//! Injecting tenancy fields on ingest and stripping them on read.
//!
//! The two operations are inverses: a field [`inject_fields`] adds is removed by
//! [`strip_fields`]. This symmetry is the heart of the shared-index isolation
//! model (`docs/03`) and is exercised by a round-trip property test.

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
