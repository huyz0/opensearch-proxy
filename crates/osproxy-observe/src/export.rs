//! The span-export seam: where assembled OTLP payloads go.
//!
//! Export is **read-only and off the request's critical path** (`docs/05`): the
//! pipeline hands a finished payload to [`SpanExporter::export`], which returns
//! immediately, a concrete exporter ships it in the background and an export
//! failure never affects the request. When no exporter is configured the default
//! [`NoopExporter`] reports [`SpanExporter::enabled`] `false`, so the pipeline
//! skips even *encoding* the payload, "Off" is near-zero cost.

use serde_json::Value;

/// Receives finished OTLP span payloads for delivery to a collector.
///
/// Implementations MUST NOT block: `export` is called inline on the request path,
/// so any network I/O belongs in a spawned task. They MUST NOT panic.
pub trait SpanExporter: Send + Sync + 'static {
    /// Whether this exporter will do anything. The pipeline checks this before
    /// building a payload, so a disabled exporter costs only this call.
    fn enabled(&self) -> bool {
        true
    }

    /// Hands off a finished OTLP `ResourceSpans` payload (from
    /// [`resource_spans`](crate::resource_spans)) for background delivery.
    /// Returns immediately; delivery is best-effort.
    fn export(&self, payload: Value);
}

/// The default exporter: export is disabled, so the pipeline never encodes a
/// payload and nothing is shipped.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopExporter;

impl SpanExporter for NoopExporter {
    fn enabled(&self) -> bool {
        false
    }

    fn export(&self, _payload: Value) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_noop_exporter_is_disabled() {
        assert!(!NoopExporter.enabled());
        NoopExporter.export(serde_json::json!({})); // no panic, no effect
    }
}
