//! Core data model for osproxy.
//!
//! This crate is the shared vocabulary every other crate speaks. It has **no
//! I/O dependencies** (no async runtime, no sockets, no wire serialization) so
//! the surface an SPI implementer compiles against stays tiny and fast — see
//! `docs/01-architecture.md` §2.
//!
//! It contains three things:
//!
//! - [`ids`] — strongly-typed identifier newtypes (no bare `String`/`u64`
//!   identifiers cross API boundaries — `docs/08` §7).
//! - [`endpoint`] — the [`endpoint::EndpointKind`] classification of OpenSearch
//!   requests (`docs/02` §5).
//! - [`error`] — the request-path [`error::ErrorContext`] taxonomy that makes
//!   every failure typed, contextual, and LLM-diagnosable (`docs/02` §4).
//! - [`time`] — the [`time::Clock`] seam that keeps time deterministic and
//!   testable (`docs/12`).
//!
//! The module tree is intentionally flat and small; each concept lives in its
//! own file (`docs/08` §2).
#![deny(missing_docs)]

pub mod endpoint;
pub mod error;
pub mod ids;
pub mod time;

pub use endpoint::EndpointKind;
pub use error::{ErrorCode, ErrorContext};
pub use ids::{ClusterId, Epoch, IndexName, PartitionId, PrincipalId, RequestId};
pub use time::{Clock, Instant, ManualClock, SystemClock};
