//! Typed configuration.
//!
//! Loads and fully validates configuration (file → env → flags) before any
//! socket opens, producing validated value objects the other crates consume
//! (`docs/01` §6). Invalid config fails fast with a typed, actionable error. It
//! contains no business logic. Hot-reloadable state goes through
//! `osproxy-control`, not here. Lands in M1.
