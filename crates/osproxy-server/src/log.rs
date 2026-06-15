//! Structured per-request logging.
//!
//! Each handled request can emit one **structured JSON log line** — the same
//! shape-only `/debug/explain` document, which already carries the request's
//! `trace_id` (`docs/05`). So logs correlate with the distributed trace and the
//! OTLP spans by `trace_id`, and an aggregator can join them. The document is
//! shape-only by construction, so the log line can never carry a tenant value.
//!
//! Logging is **opt-in**: the default [`NoLog`] reports [`RequestLog::enabled`]
//! `false`, so the handler skips even fetching the document — "off" is near-zero
//! cost.

use serde_json::Value;

/// Receives one structured record per handled request.
///
/// Implementations MUST NOT panic. `emit` is called inline after the response is
/// produced, so it must be cheap (a line write); heavy delivery belongs behind a
/// background sink.
pub trait RequestLog: Send + Sync {
    /// Whether this logger will emit. The handler checks this before assembling
    /// the record, so a disabled logger costs only this call.
    fn enabled(&self) -> bool {
        true
    }

    /// Emits one request record (the shape-only explain document).
    fn emit(&self, record: &Value);
}

/// The default logger: disabled, so no record is assembled or written.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLog;

impl RequestLog for NoLog {
    fn enabled(&self) -> bool {
        false
    }
    fn emit(&self, _record: &Value) {}
}

/// Writes each record as one compact JSON line to stdout — the conventional
/// structured-logging sink for a containerized service (the platform's log
/// collector scrapes stdout).
#[derive(Clone, Copy, Debug, Default)]
pub struct StdoutJsonLog;

impl RequestLog for StdoutJsonLog {
    fn emit(&self, record: &Value) {
        // `Value`'s Display is compact JSON: exactly one line per request.
        println!("{record}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_logger_is_disabled() {
        assert!(!NoLog.enabled());
        NoLog.emit(&serde_json::json!({})); // no panic, no output
    }
}
