//! The streaming search response: a live upstream body piped back to the client
//! through the hit transform, never buffered (ADR-014, final stage).
//!
//! [`shape_hits_stream`] wraps the upstream [`ByteBody`] in a [`SearchHitsScanner`]
//! and produces a new [`ByteBody`] that emits transformed bytes as the upstream
//! flows. It is built with [`futures_util::stream::unfold`] + a
//! [`StreamBody`] — a spawn-free combinator (no
//! `tokio::spawn`, satisfying the spawn-discipline gate) that carries the
//! upstream body and the scanner as its state.

use bytes::Bytes;
use futures_util::{stream, StreamExt as _};
use http_body::Frame;
use http_body_util::{BodyExt as _, BodyStream, StreamBody};
use osproxy_observe::RequestTrace;
use osproxy_sink::{buffered, BodyError, ByteBody, Reader, Sink};
use osproxy_spi::RequestCtx;
use osproxy_tenancy::Router;

use crate::cursor::{forwardable_query, pit_id_in_body};
use crate::endpoints::wire_trace;
use crate::error::RequestError;
use crate::observe::{read_dispatch_info, resolve_info};
use crate::pipeline::Pipeline;
use crate::read::build_search_op;
use crate::search_scan::{HitShaper, SearchHitsScanner};

impl<R: Router, S: Sink + Reader> Pipeline<R, S> {
    /// The **streaming** search path (ADR-014, final stage): like
    /// [`search`](Self::search) but the upstream response is piped back through
    /// the hit transform without buffering — each hit shaped incrementally, every
    /// sibling (`aggregations` especially) forwarded verbatim. A PIT-pinned search
    /// falls back to the buffered path: its `_scroll_id` affinity wrap needs the
    /// whole body. The caller (transport) already excluded scroll-opening
    /// searches, which also need the buffered body. The trace lifecycle is owned
    /// by [`search_streamed`](Self::search_streamed).
    pub(crate) async fn run_search_stream(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<StreamSearch, RequestError> {
        if self.cursor_signer.is_some() {
            if let Some(wrapped) = pit_id_in_body(ctx.body()) {
                let resp = self.pit_search(ctx, trace, &wrapped).await?;
                return Ok(StreamSearch::buffered(resp.status, resp.body));
            }
        }
        let resolved = self.resolve_with_retry(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let (search_op, shape) = build_search_op(&resolved, ctx.body())?;
        let stream = self
            .sink()
            .search_stream(
                search_op
                    .with_query(forwardable_query(ctx.query()))
                    .with_trace(Some(wire_trace(ctx))),
            )
            .await?;
        trace.record_dispatch(read_dispatch_info(
            &resolved,
            stream.status,
            stream.pool_reuse,
        ));

        let shaper = HitShaper {
            logical_index: ctx.logical_index().to_owned(),
            partition: resolved.partition.as_str().to_owned(),
            shape,
        };
        Ok(StreamSearch::stream(
            stream.status,
            shape_hits_stream(stream.body, shaper),
        ))
    }
}

/// The outcome of a streaming search: the upstream status and the response body
/// as a live [`ByteBody`] — the hits transformed incrementally, all siblings
/// (including `aggregations`) passed through verbatim, none of it buffered.
pub struct StreamSearch {
    /// The upstream HTTP status.
    pub status: u16,
    /// The transformed response body, streamed back.
    pub body: ByteBody,
}

impl std::fmt::Debug for StreamSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The streamed body is not `Debug`; show the rest of the shape.
        f.debug_struct("StreamSearch")
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl StreamSearch {
    /// A streaming response whose body is transformed on the fly.
    #[must_use]
    pub fn stream(status: u16, body: ByteBody) -> Self {
        Self { status, body }
    }

    /// A buffered response (the PIT/affinity fallback, or an error), boxed into
    /// the same body type so both arms share one return type.
    #[must_use]
    pub fn buffered(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            body: buffered(Bytes::from(body)),
        }
    }
}

/// The live state of the transforming stream: the upstream frames and the
/// scanner driving them.
struct Active {
    frames: BodyStream<ByteBody>,
    scanner: SearchHitsScanner,
}

/// The per-step state of the transforming stream. The active state is boxed (it
/// dwarfs the terminal `Done`) so the `unfold` state stays small.
enum Stage {
    /// Still pulling upstream frames through the scanner.
    Active(Box<Active>),
    /// Exhausted.
    Done,
}

/// Wraps the upstream search-response body so its `hits.hits` are transformed to
/// the client's logical view incrementally — peak memory is one hit plus one
/// upstream frame, independent of the response size (INV-MEM).
#[must_use]
pub(crate) fn shape_hits_stream(upstream: ByteBody, shaper: HitShaper) -> ByteBody {
    let init = Stage::Active(Box::new(Active {
        frames: BodyStream::new(upstream),
        scanner: SearchHitsScanner::new(shaper),
    }));
    let stream = stream::unfold(init, |stage| async move { next_frame(stage).await });
    StreamBody::new(stream).boxed_unsync()
}

/// Produces the next output frame from the stream stage, or `None` at end.
/// Pulls upstream frames, feeds the scanner, and yields only non-empty output
/// (an upstream frame consumed entirely into a partial hit yields nothing, so we
/// loop to the next frame rather than emit an empty frame).
async fn next_frame(stage: Stage) -> Option<(Result<Frame<Bytes>, BodyError>, Stage)> {
    let Stage::Active(mut active) = stage else {
        return None;
    };
    loop {
        match active.frames.next().await {
            Some(Ok(frame)) => {
                let Ok(data) = frame.into_data() else {
                    continue; // a non-data frame (trailers): ignore
                };
                let out = active.scanner.feed(&data);
                if !out.is_empty() {
                    return Some((Ok(Frame::data(Bytes::from(out))), Stage::Active(active)));
                }
            }
            Some(Err(err)) => return Some((Err(err), Stage::Done)),
            None => {
                // Upstream ended: emit the scanner's final bytes (normally empty —
                // everything is emitted incrementally), then stop.
                let tail = active.scanner.finish();
                return if tail.is_empty() {
                    None
                } else {
                    Some((Ok(Frame::data(Bytes::from(tail))), Stage::Done))
                };
            }
        }
    }
}
