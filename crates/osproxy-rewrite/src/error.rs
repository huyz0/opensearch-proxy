//! Failures the pure body transforms can return.

use osproxy_core::FieldName;
use thiserror::Error;

/// A failure applying a body transform.
///
/// These are document-shape failures (the body is not what the transform
/// requires) or safety failures (a client field would collide with an injected
/// tenancy field). They carry only field *names* and shapes, never values, so
/// they are safe to surface in telemetry (NFR-S2).
#[non_exhaustive]
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RewriteError {
    /// The body was expected to be a JSON object but was not.
    #[error("document body is not a JSON object")]
    NotAnObject,

    /// The body bytes were not valid JSON.
    #[error("document body is not valid JSON")]
    InvalidJson,

    /// A field the transform must inject already exists in the client document.
    /// Rejected rather than overwritten: a client could otherwise spoof a
    /// tenancy field and defeat isolation (`docs/03`).
    #[error("client document already contains reserved field")]
    ReservedFieldCollision {
        /// The colliding field name.
        field: FieldName,
    },

    /// A path referenced by a partition key or id template does not resolve to
    /// a scalar value in the document.
    #[error("path does not resolve to a scalar value in the document")]
    PathNotScalar {
        /// The dotted path that failed to resolve.
        path: String,
    },

    /// An id template referenced a placeholder the transform does not support.
    #[error("unsupported id-template placeholder")]
    UnsupportedPlaceholder {
        /// The placeholder text, without braces.
        placeholder: String,
    },

    /// A `_bulk` action line is not a single-key `{verb: {…}}` object, names an
    /// unknown verb, or an action that needs a source line has none.
    #[error("malformed _bulk action line")]
    MalformedBulkAction,

    /// An id template cannot be reversed to recover a logical id on the read
    /// path: a logical→physical mapping needs exactly one `{body.<path>}`
    /// placeholder (the natural key), so a template with none or several is not
    /// usable for `GetById`/`DeleteById` (`docs/03` §4, `docs/04` §5).
    #[error("id template is not reversible: needs exactly one body placeholder")]
    IrreversibleIdTemplate,
}
