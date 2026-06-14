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
//! Bulk NDJSON demux (`docs/04` §3) and query-DSL filter wrapping (`docs/04`
//! §4) land in M2/M3 alongside their endpoints.
#![deny(missing_docs)]

mod error;
mod extract;
mod fields;
mod id;

pub use error::RewriteError;
pub use extract::extract_scalar;
pub use fields::{inject_fields, strip_fields};
pub use id::construct_id;
