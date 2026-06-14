//! Write sink.
//!
//! Where writes go, isolated from how routing is decided (`docs/decisions/008`).
//! Ships `OpenSearchSink` (direct, single target) now; the future queue-based
//! redundancy mode is a `QueueSink` drop-in behind the same `Sink` trait. Epoch
//! stamping is enforced at this boundary (`docs/06` §2). Lands in M1.
