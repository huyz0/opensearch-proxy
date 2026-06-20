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
//! - [`target`] — the [`target::Target`] a routing decision resolves to
//!   (`docs/02`).
//! - [`trace`] — the [`trace::TraceContext`] W3C propagation primitive that
//!   carries distributed-trace identity to downstream calls (`docs/05` §2).
//! - [`json`] — a dependency-free byte-level JSON scanner that reads partition
//!   keys and id components straight from a body without materializing a tree
//!   (ADR-014), shared by the SPI extraction utilities and the transform layer.
//!
//! The module tree is intentionally flat and small; each concept lives in its
//! own file (`docs/08` §2).
#![deny(missing_docs)]

pub mod cursor;
pub mod endpoint;
pub mod error;
pub mod ids;
pub mod json;
pub mod target;
pub mod time;
pub mod trace;

pub use cursor::CursorSigner;
pub use endpoint::EndpointKind;
pub use error::{ErrorCode, ErrorContext};
pub use ids::{ClusterId, Epoch, FieldName, IndexName, PartitionId, PrincipalId, RequestId};
pub use json::JsonError;
pub use target::Target;
pub use time::{Clock, Instant, ManualClock, SystemClock};
pub use trace::TraceContext;
