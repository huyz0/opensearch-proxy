//! Body and query transforms.
//!
//! Pure transforms with no network or placement lookup: scalar extraction from
//! a document body, document-`_id` construction, and tenancy-field
//! inject/strip. Held to the highest coverage bar including branch coverage
//! (`docs/09`).
//!
//! This crate deliberately depends only on [`osproxy_core`] and `serde_json`:
//! it speaks in primitives (paths, names, JSON values), and the tenancy adapter
//! (`osproxy-tenancy`) translates SPI rule types into these calls. That keeps
//! the transforms a small, exhaustively testable leaf of the dependency graph.
//!
//! M2 adds query-DSL filter wrapping ([`wrap_query`], `docs/04` §4) and the
//! logical↔physical id mapping for by-id reads. Bulk NDJSON demux (`docs/04`
//! §3) lands in M3 alongside its endpoint.
#![deny(missing_docs)]

mod bulk;
mod error;
mod extract;
mod fields;
mod id;
mod mget;
mod msearch;
mod query;

pub use bulk::{parse_bulk, parse_bulk_action, parse_bulk_op, BulkAction, BulkItem};
pub use error::RewriteError;
pub use extract::extract_scalar;
pub use fields::{inject_fields, inject_fields_bytes, inject_update, strip_fields};
pub use id::{construct_id, construct_id_bytes, map_logical_to_physical, map_physical_to_logical};
pub use mget::{parse_mget, MgetItem};
pub use msearch::{parse_msearch, MsearchItem};
pub use query::wrap_query;

/// Validates that `body` is a single well-formed JSON document, allocating
/// nothing, for the verbatim write path, which forwards the body unchanged but
/// must still reject malformed input.
///
/// # Errors
///
/// [`RewriteError::InvalidJson`] if `body` is not valid JSON.
pub fn validate_json(body: &[u8]) -> Result<(), RewriteError> {
    osproxy_core::json::validate(body).map_err(RewriteError::from)
}
