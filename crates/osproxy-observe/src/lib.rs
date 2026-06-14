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
//! OTLP export attaches in M7.
#![deny(missing_docs)]

mod explain;
mod trace;

pub use explain::{explain_json, ExplainStore};
pub use trace::{
    ClassifyInfo, DispatchInfo, EgressInfo, IngressInfo, RequestTrace, ResolveInfo, RewriteInfo,
};
