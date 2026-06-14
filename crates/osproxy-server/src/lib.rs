//! Library half of the `osproxy` binary.
//!
//! Holds the reference [`tenancy`] implementation and the ingress [`handler`]
//! that wires the engine pipeline to the transport. It lives in a library (with
//! `main.rs` a thin entry point) so the wiring is unit- and integration-testable
//! without spawning a process.
#![deny(missing_docs)]

pub mod handler;
pub mod tenancy;
