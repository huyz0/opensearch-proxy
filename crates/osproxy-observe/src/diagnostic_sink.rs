//! The diagnostic-capture sink: where directive-selected explanations go *off the
//! instance* (`docs/05` §5).
//!
//! A proxy fleet serves a request on whichever instance the load balancer picked,
//! so a captured `/debug/explain` document lands on *that* instance — its local
//! break-glass ring and `/debug/explain` store are invisible to the others. To
//! diagnose "why did request X route there" across a fleet, the capture must leave
//! the instance, keyed by `trace_id` (which the explain doc already carries), so an
//! external aggregator holds the whole fleet's record under one key.
//!
//! This seam is the **fleet-coherent counterpart of the break-glass ring**: the
//! same `ring_buffer`/`capture` directive that fills the local tape also hands the
//! shape-only explain doc to a [`DiagnosticSink`]. It is distinct from the
//! per-request request-log (all-or-none) — it pushes only the *directive-selected*
//! captures. The doc is shape-only by construction (it is the explain doc), so the
//! sink never carries a tenant value.

use serde_json::Value;

/// Receives directive-selected diagnostic documents (each a `/debug/explain` doc)
/// for delivery to a fleet-wide store/aggregator, keyed by the `trace_id` the doc
/// carries.
///
/// Implementations MUST NOT block: `emit` is called inline on the request path
/// (only for captured requests), so any network I/O belongs in a spawned task.
/// They MUST NOT panic. Delivery is best-effort — a slow or down sink must never
/// affect the request.
pub trait DiagnosticSink: Send + Sync + 'static {
    /// Whether this sink will do anything. The pipeline checks this before building
    /// a document, so a disabled sink costs only this call even when a capture
    /// directive is active.
    fn enabled(&self) -> bool {
        true
    }

    /// Hands off one shape-only diagnostic document for background delivery.
    /// Returns immediately; delivery is best-effort.
    fn emit(&self, doc: Value);
}

/// The default sink: off-instance emission is disabled, so a directive-selected
/// capture stays in the local break-glass ring only (single-instance).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDiagnosticSink;

impl DiagnosticSink for NoopDiagnosticSink {
    fn enabled(&self) -> bool {
        false
    }

    fn emit(&self, _doc: Value) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_noop_sink_is_disabled() {
        assert!(!NoopDiagnosticSink.enabled());
        NoopDiagnosticSink.emit(serde_json::json!({})); // no panic, no effect
    }
}
