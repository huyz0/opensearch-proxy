//! Break-glass ring buffer: a bounded, in-order tape of recent explanations
//! captured **only when a directive asks** (`ring_buffer: true`, `docs/05` §5).
//!
//! Distinct from [`ExplainStore`](crate::ExplainStore), which is the always-on,
//! lookup-by-request-id store behind `/debug/explain/{id}`. The break-glass
//! buffer is a *sequence* an operator turns on deliberately, when a class of
//! request is failing and the ids aren't known up front, flip a `ring_buffer`
//! directive and read back the last N matching requests as a forensic tape.
//!
//! Single-instance by design (the captured tape lives on the instance that
//! handled the requests); bounded so it costs nothing until used and cannot grow
//! without limit once on. Shape-only, inherited from the explain document it
//! stores, it cannot reveal a tenant value because none was ever captured.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use serde_json::Value;

/// A bounded in-memory ring of recent explanation documents, captured on demand.
#[derive(Debug)]
pub struct BreakGlassBuffer {
    capacity: usize,
    entries: Mutex<VecDeque<Value>>,
}

impl BreakGlassBuffer {
    /// Creates a buffer holding at most `capacity` recent captures.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: Mutex::new(VecDeque::new()),
        }
    }

    /// Captures one explanation document, evicting the oldest if full.
    pub fn capture(&self, doc: Value) {
        let mut entries = self.lock();
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(doc);
    }

    /// A snapshot of the captured tape, oldest first, the break-glass read.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Value> {
        self.lock().iter().cloned().collect()
    }

    /// How many captures the tape currently holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the tape is empty (nothing captured yet).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Locks the tape, recovering a poisoned lock, it is append-only forensic
    /// data with no invariant a panicking holder could tear (NFR-R1).
    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Value>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[path = "breakglass_tests.rs"]
mod tests;
