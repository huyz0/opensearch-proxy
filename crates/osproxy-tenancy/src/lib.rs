//! High-level tenancy layer.
//!
//! Translates declarative tenancy rules (partition key, doc-id construction,
//! injected/sensitive fields, placement lookup) into low-level routing
//! decisions, so most implementers never touch `RouteDecision` plumbing
//! (`docs/02`, `docs/03`). Logic lands in milestone M1 (`docs/11`).
#![deny(missing_docs)]
