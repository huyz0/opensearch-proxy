//! Observability.
//!
//! Emits the per-request causal trace (shapes, ids, field names — never values,
//! `docs/05`), evaluates runtime diagnostics directives, and assembles the
//! `/debug/explain/{request_id}` document for LLM consumption. Read-only: it
//! never mutates routing or cluster state (`docs/decisions/005`). Lands in M1,
//! extended through M7.
