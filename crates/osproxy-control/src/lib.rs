//! Control plane.
//!
//! Distributes the epoch-versioned placement table and diagnostics directives
//! to every proxy instance via a pluggable watched store (`docs/03` §3,
//! `docs/05` §3). Owns migration state transitions (`docs/06`). It does not
//! handle request traffic. In-memory table lands in M1; watched backends in M7.
