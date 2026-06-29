//! The asynchronous fan-out write mode (`docs/04` §9).
//!
//! In sync mode the proxy forwards a mutation to the upstream and returns its
//! real result. In **async mode** it instead durably enqueues the fully-resolved,
//! epoch-stamped op onto a [`WriteQueue`] and returns `202 Accepted` with an
//! `op_id` handle. A separate downstream component consumes the queue and applies
//! each op to one or more destinations, so the proxy's only promise is *durable
//! acceptance into the pipeline*, never application or its result.
//!
//! This is deliberately narrow:
//!
//! * The `202` is returned **only after** the queue acknowledges the enqueue, so a
//!   client that got `202` knows the op will not be silently dropped. A queue that
//!   cannot accept the op fails the request rather than lying.
//! * The op carries an **`op_id`**, client-supplied via the `X-Op-Id` header
//!   (validated) or proxy-minted from the request id otherwise, that is both the
//!   correlation handle and the idempotency key the downstream applier dedups on.
//! * The proxy hosts **no status surface**: whether and how a failed apply is
//!   reported back is the downstream's responsibility, out of scope here.
//!
//! Mode is negotiated per request (`X-Write-Mode`) over a deployment baseline; see
//! [`crate::Pipeline::with_baseline_write_mode`].

use std::future::Future;
use std::pin::Pin;

use osproxy_core::RequestId;
use osproxy_sink::{Reader, Sink, WriteBatch};
use osproxy_spi::RequestCtx;
use osproxy_tenancy::{Resolved, Router};
use serde_json::json;

use crate::pipeline::{Pipeline, PipelineResponse};

/// How a mutation is dispatched.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum WriteMode {
    /// Forward to the upstream and return its real result. The default.
    #[default]
    Sync,
    /// Durably enqueue the op for downstream fan-out and return `202` + a handle.
    Async,
}

impl WriteMode {
    /// Parses an `X-Write-Mode` header value (ASCII-case-insensitive). Unknown
    /// values yield `None` so the caller can reject rather than guess.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("sync") {
            Some(Self::Sync)
        } else if value.eq_ignore_ascii_case("async") {
            Some(Self::Async)
        } else {
            None
        }
    }
}

/// The maximum accepted length of a client-supplied `X-Op-Id`. The id is keyed
/// into the queue and logged, so it is bounded and charset-restricted to keep it
/// from injecting into a downstream keyspace.
const MAX_OP_ID_LEN: usize = 128;

/// Whether `candidate` is an acceptable op id: non-empty, within
/// `MAX_OP_ID_LEN`, and limited to a safe key charset (`A-Za-z0-9-_.:`).
#[must_use]
pub fn valid_op_id(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate.len() <= MAX_OP_ID_LEN
        && candidate
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

/// Resolves the op id for a request: the validated client-supplied `X-Op-Id`, or
/// the proxy's own request id when the header is absent or malformed. Always
/// returns a usable id so the loop can always be correlated.
#[must_use]
pub fn op_id_for(ctx: &RequestCtx<'_>, request_id: &RequestId) -> String {
    ctx.headers()
        .get("x-op-id")
        .filter(|h| valid_op_id(h))
        .map_or_else(|| request_id.as_str().to_owned(), ToOwned::to_owned)
}

/// Why a mutation cannot be honored in async mode, if it cannot, a short,
/// value-free reason for the `400`. These all need read-modify-write against the
/// document's current state, which does not exist at enqueue time, so async
/// rejects them rather than silently mis-applying or dropping the precondition.
#[must_use]
pub fn unsupported_async(ctx: &RequestCtx<'_>) -> Option<&'static str> {
    // Optimistic concurrency: the precondition is checked against the live
    // version, so dropping it (the only async option) would corrupt the contract.
    if let Some(query) = ctx.query() {
        let has_cas = query.split('&').any(|pair| {
            let key = pair.split('=').next().unwrap_or(pair);
            matches!(key, "if_seq_no" | "if_primary_term" | "version")
        });
        if has_cas {
            return Some("optimistic concurrency (if_seq_no/if_primary_term/version) is not supported in async write mode");
        }
    }
    // A scripted/partial `_update` merges into the current document; async fan-out
    // has no single authoritative document to merge against. The path is
    // `/{index}/_update/{id}`, so match the `_update` path segment, not a suffix.
    if ctx.path().split('/').any(|seg| seg == "_update") {
        return Some("scripted/partial _update is not supported in async write mode");
    }
    None
}

/// A mutation accepted for asynchronous fan-out: the fully-resolved,
/// epoch-stamped [`WriteBatch`] the sync path would have dispatched, plus the
/// correlation/idempotency id and the ordering key.
#[derive(Clone, Debug)]
pub struct QueuedWrite {
    /// Correlation handle and downstream idempotency key.
    pub op_id: String,
    /// The partition id, used as the queue partition key so all ops for one
    /// logical partition stay ordered through the fan-out.
    pub partition_key: String,
    /// The resolved op(s), identical to what the sync path would deliver.
    pub batch: WriteBatch,
}

/// A queue that durably accepts resolved write ops for downstream fan-out.
///
/// The implementor (a Kafka producer with a write-ahead log, in the shipped
/// binary) must only resolve the future `Ok` once the op is durably accepted, so
/// the `202` the pipeline returns is truthful. Implementations MUST NOT panic.
pub trait WriteQueue: Send + Sync {
    /// Whether async writes can be served. `false` (the default [`NoQueue`])
    /// means an async request is refused rather than dropped.
    fn enabled(&self) -> bool {
        false
    }

    /// Durably enqueues `write`, resolving `Ok` only once it is accepted.
    fn enqueue<'a>(
        &'a self,
        write: QueuedWrite,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + 'a>>;
}

/// A failure to enqueue an async write. Carries a value-free reason only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueError {
    /// A short, value-free description (e.g. `"broker unavailable"`).
    pub reason: &'static str,
}

/// The default queue: async writes are unavailable. An async request against a
/// pipeline with no queue is refused with `422`, never accepted-and-dropped.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoQueue;

impl WriteQueue for NoQueue {
    fn enqueue<'a>(
        &'a self,
        _write: QueuedWrite,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + 'a>> {
        Box::pin(async {
            Err(QueueError {
                reason: "async write queue is not configured",
            })
        })
    }
}

/// The `202 Accepted` envelope returned once an async write is durably enqueued.
///
/// A generic async handle, not a synthetic OpenSearch result: `result:"queued"`
/// is honest about what happened (the op was accepted, not applied), and `op_id`
/// is the handle the client correlates any downstream outcome against.
#[must_use]
pub(crate) fn accepted_response(op_id: &str, index: &str) -> PipelineResponse {
    PipelineResponse {
        status: 202,
        body: serde_json::to_vec(&json!({
            "op_id": op_id,
            "status": "accepted",
            "result": "queued",
            "_index": index,
        }))
        .unwrap_or_else(|_| b"{}".to_vec()),
        content_type: None,
    }
}

/// The `400` returned when an op cannot be honored in async mode (see
/// [`unsupported_async`]). The `reason` is value-free.
#[must_use]
pub(crate) fn unsupported_response(reason: &str, index: &str) -> PipelineResponse {
    PipelineResponse {
        status: 400,
        body: serde_json::to_vec(&json!({
            "status": "rejected",
            "error": reason,
            "_index": index,
        }))
        .unwrap_or_else(|_| b"{}".to_vec()),
        content_type: None,
    }
}

/// The `422` returned when async mode was requested but no queue is configured,
/// the op is refused, never accepted-and-dropped.
#[must_use]
pub(crate) fn unavailable_response(index: &str) -> PipelineResponse {
    PipelineResponse {
        status: 422,
        body: serde_json::to_vec(&json!({
            "status": "rejected",
            "error": "async write mode is not available on this proxy",
            "_index": index,
        }))
        .unwrap_or_else(|_| b"{}".to_vec()),
        content_type: None,
    }
}

/// The `503` returned when the queue refused the op. Retryable: the same `op_id`
/// makes the retry idempotent downstream.
#[must_use]
pub(crate) fn enqueue_failed_response(op_id: &str, index: &str) -> PipelineResponse {
    PipelineResponse {
        status: 503,
        body: serde_json::to_vec(&json!({
            "op_id": op_id,
            "status": "rejected",
            "error": "async write could not be enqueued",
            "_index": index,
        }))
        .unwrap_or_else(|_| b"{}".to_vec()),
        content_type: None,
    }
}

impl<R: Router, S: Sink + Reader> Pipeline<R, S> {
    /// Durably enqueues a resolved write for downstream fan-out and returns the
    /// `202` handle (`docs/04` §9). The `202` is produced **only after** the queue
    /// acknowledges the enqueue; a missing queue is refused (`422`) and an enqueue
    /// failure is reported (`503`), the op is never accepted-and-dropped. No live
    /// epoch gate runs here: the op carries its epoch and the downstream applier
    /// owns staleness, since there is no synchronous upstream to hold.
    pub(crate) async fn enqueue_async(
        &self,
        ctx: &RequestCtx<'_>,
        resolved: &Resolved,
        batch: WriteBatch,
    ) -> PipelineResponse {
        let index = ctx.logical_index();
        // A client error takes precedence over a missing queue: an op that cannot
        // be honored async is rejected (`400`) whether or not a queue is wired.
        if let Some(reason) = unsupported_async(ctx) {
            return unsupported_response(reason, index);
        }
        if !self.write_queue.enabled() {
            return unavailable_response(index);
        }
        let op_id = op_id_for(ctx, ctx.request_id());
        let write = QueuedWrite {
            op_id: op_id.clone(),
            partition_key: resolved.partition.as_str().to_owned(),
            batch,
        };
        match self.write_queue.enqueue(write).await {
            Ok(()) => accepted_response(&op_id, index),
            Err(_) => enqueue_failed_response(&op_id, index),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_modes_case_insensitively_and_rejects_unknown() {
        assert_eq!(WriteMode::parse("sync"), Some(WriteMode::Sync));
        assert_eq!(WriteMode::parse("ASYNC"), Some(WriteMode::Async));
        assert_eq!(WriteMode::parse("queue"), None);
        assert_eq!(WriteMode::parse(""), None);
    }

    #[test]
    fn op_id_validation_bounds_length_and_charset() {
        assert!(valid_op_id("a-b_c.d:1"));
        assert!(!valid_op_id(""));
        assert!(!valid_op_id("has space"));
        assert!(!valid_op_id("inject\nkey"));
        assert!(!valid_op_id(&"x".repeat(MAX_OP_ID_LEN + 1)));
        assert!(valid_op_id(&"x".repeat(MAX_OP_ID_LEN)));
    }

    #[tokio::test]
    async fn no_queue_is_disabled_and_refuses() {
        assert!(!NoQueue.enabled());
        let write = QueuedWrite {
            op_id: "op-1".to_owned(),
            partition_key: "acme".to_owned(),
            batch: WriteBatch::single(test_op()),
        };
        let err = NoQueue.enqueue(write).await.unwrap_err();
        assert_eq!(err.reason, "async write queue is not configured");
    }

    fn test_op() -> osproxy_sink::WriteOp {
        use osproxy_core::{ClusterId, Epoch, IndexName, Target};
        use osproxy_sink::{DocOp, WriteOp};
        WriteOp::new(
            Target::new(ClusterId::from("c"), IndexName::from("i")),
            DocOp::Index {
                id: Some("p:1".to_owned()),
                routing: None,
                body: bytes::Bytes::from_static(b"{}"),
            },
            Epoch::new(1),
        )
    }
}
