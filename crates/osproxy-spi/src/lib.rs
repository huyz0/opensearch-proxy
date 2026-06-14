//! Public SPI traits for osproxy.
//!
//! This is the contract implementers compile against (`docs/02`). It depends
//! only on [`osproxy_core`] so the surface stays tiny. The traits themselves
//! (`RoutingSpi`, `TenancySpi`, `Sink`, `CryptoProvider`, `Authenticator`,
//! `Authorizer`) and their supporting types are introduced in milestone M1
//! (`docs/11`); this crate currently establishes the module boundary.
#![deny(missing_docs)]

// Re-export the core vocabulary so SPI implementers get the identifier and
// error types from a single dependency.
pub use osproxy_core as core;
