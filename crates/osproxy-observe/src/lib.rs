//! Observability.
//!
//! Emits the per-request causal trace (shapes, ids, field names — never values,
//! `docs/05`) and assembles the `/debug/explain/{request_id}` document for LLM
//! consumption. Read-only: it never mutates routing or cluster state
//! (`docs/decisions/005`).
//!
//! The [`RequestTrace`] is **shape-only by construction** — its setters accept
//! only id newtypes, compile-time labels, and sizes — so there is no API path by
//! which a tenant value or secret can reach telemetry (`docs/05` §7). The
//! [`ExplainStore`] retains the most recent explanations for the debug endpoint.
//!
//! Directive evaluation (runtime verbosity control) lives in `osproxy-control`;
//! [`resource_spans`] encodes that same shape-only trace as an OTLP/HTTP JSON
//! payload for export (the wire emission is the I/O layer's job, M7).
#![deny(missing_docs)]

mod explain;
mod otlp;
mod trace;

pub use explain::{explain_json, ExplainStore};
pub use otlp::resource_spans;
pub use trace::{
    ClassifyInfo, DispatchInfo, EgressInfo, IngressInfo, RequestTrace, ResolveInfo, RewriteInfo,
};
