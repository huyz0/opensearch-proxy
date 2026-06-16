//! The [`Reader`] trait: fetching a single document by physical id.
//!
//! Reads are always direct-to-cluster — unlike writes, they cannot be served by
//! a queue — so the read seam is separate from [`Sink`](crate::Sink). The same
//! backend type may implement both (`OpenSearchSink` does, sharing its pooled
//! connection), while a write-only `QueueSink` implements only [`Sink`].
//!
//! [`Sink`]: crate::Sink

use osproxy_core::{ClusterId, Target, TraceContext};
use osproxy_spi::{HttpMethod, Protocol};

use crate::error::SinkError;

/// A read-by-id operation against a resolved [`Target`].
///
/// The id is already the **physical** id (the tenancy adapter mapped the
/// client's logical id, `docs/04` §5); the reader does no rewriting.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadOp {
    /// The physical destination to read from.
    pub target: Target,
    /// The physical document id to fetch.
    pub id: String,
    /// The `_routing` value (the partition id), if the placement routes.
    pub routing: Option<String>,
    /// The upstream wire protocol this read is dispatched over. Defaults to
    /// [`Protocol::Http1`].
    pub protocol: Protocol,
    /// The W3C trace context to forward downstream (`traceparent`), so the
    /// upstream's spans join this request's distributed trace. `None` = no
    /// propagation header is sent.
    pub trace: Option<TraceContext>,
}

impl ReadOp {
    /// Constructs a read operation (defaulting to HTTP/1.1 upstream).
    #[must_use]
    pub fn new(target: Target, id: impl Into<String>, routing: Option<String>) -> Self {
        Self {
            target,
            id: id.into(),
            routing,
            protocol: Protocol::Http1,
            trace: None,
        }
    }

    /// Sets the upstream protocol for this op (builder style).
    #[must_use]
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Sets the trace context to propagate downstream (builder style).
    #[must_use]
    pub fn with_trace(mut self, trace: Option<TraceContext>) -> Self {
        self.trace = trace;
        self
    }
}

/// The outcome of a read: whether the document was found, and its raw upstream
/// body (the document as stored, before the read-path field strip).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadOutcome {
    /// The upstream HTTP status.
    pub status: u16,
    /// Whether the document exists.
    pub found: bool,
    /// The raw upstream response body (the stored document when `found`).
    pub body: Vec<u8>,
    /// Whether this read rode a reused pooled connection (NFR-P telemetry).
    pub pool_reuse: bool,
}

impl ReadOutcome {
    /// A hit carrying the stored document body.
    #[must_use]
    pub fn found(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            found: true,
            body,
            pool_reuse: false,
        }
    }

    /// A miss (no such document).
    #[must_use]
    pub fn not_found(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            found: false,
            body,
            pool_reuse: false,
        }
    }

    /// Records whether the dispatch reused a pooled connection (builder style).
    #[must_use]
    pub fn with_pool_reuse(mut self, reused: bool) -> Self {
        self.pool_reuse = reused;
        self
    }
}

/// A search operation against a resolved [`Target`].
///
/// The body is the **already-wrapped** query (the tenancy partition filter has
/// been applied, `docs/04` §4); the reader forwards it verbatim.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SearchOp {
    /// The physical destination to search.
    pub target: Target,
    /// The query body to forward upstream (already partition-filtered).
    pub body: Vec<u8>,
    /// The upstream wire protocol this search is dispatched over. Defaults to
    /// [`Protocol::Http1`].
    pub protocol: Protocol,
    /// An already-allow-listed query string (without the `?`) to append to the
    /// upstream URL — e.g. `scroll=1m` to open a scroll. The engine filters this
    /// to cursor-safe params before it reaches here; the sink appends it verbatim.
    pub query: Option<String>,
    /// The W3C trace context to forward downstream (`traceparent`).
    pub trace: Option<TraceContext>,
}

impl SearchOp {
    /// Constructs a search operation (defaulting to HTTP/1.1 upstream).
    #[must_use]
    pub fn new(target: Target, body: Vec<u8>) -> Self {
        Self {
            target,
            body,
            protocol: Protocol::Http1,
            query: None,
            trace: None,
        }
    }

    /// Sets the upstream protocol for this op (builder style).
    #[must_use]
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Sets the (already allow-listed) upstream query string (builder style).
    #[must_use]
    pub fn with_query(mut self, query: Option<String>) -> Self {
        self.query = query;
        self
    }

    /// Sets the trace context to propagate downstream (builder style).
    #[must_use]
    pub fn with_trace(mut self, trace: Option<TraceContext>) -> Self {
        self.trace = trace;
        self
    }
}

/// The outcome of a search: the upstream status and raw response body (the
/// hits, before the read-path field strip).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SearchOutcome {
    /// The upstream HTTP status.
    pub status: u16,
    /// The raw upstream response body (the hits envelope).
    pub body: Vec<u8>,
    /// Whether this search rode a reused pooled connection (NFR-P telemetry).
    pub pool_reuse: bool,
}

impl SearchOutcome {
    /// Constructs a search outcome.
    #[must_use]
    pub fn new(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            body,
            pool_reuse: false,
        }
    }

    /// Records whether the dispatch reused a pooled connection (builder style).
    #[must_use]
    pub fn with_pool_reuse(mut self, reused: bool) -> Self {
        self.pool_reuse = reused;
        self
    }
}

/// The outcome of a count: the upstream status and the matched document count.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CountOutcome {
    /// The upstream HTTP status.
    pub status: u16,
    /// The number of matching documents.
    pub count: u64,
    /// Whether this count rode a reused pooled connection (NFR-P telemetry).
    pub pool_reuse: bool,
}

impl CountOutcome {
    /// Constructs a count outcome.
    #[must_use]
    pub fn new(status: u16, count: u64) -> Self {
        Self {
            status,
            count,
            pool_reuse: false,
        }
    }

    /// Records whether the dispatch reused a pooled connection (builder style).
    #[must_use]
    pub fn with_pool_reuse(mut self, reused: bool) -> Self {
        self.pool_reuse = reused;
        self
    }
}

/// A raw cursor passthrough op (`docs/03` §6): forward `method path` with `body`
/// to the specific `cluster` the cursor is pinned to — scroll/PIT continue,
/// clear, or close. Unlike the typed ops, the destination is *already resolved*
/// (the engine recovered it from the cursor's signed envelope), so this carries
/// the cluster directly rather than a partition.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CursorOp {
    /// The cluster the cursor is pinned to.
    pub cluster: ClusterId,
    /// The HTTP method to forward (continue is `POST`/`GET`, clear/close `DELETE`).
    pub method: HttpMethod,
    /// The upstream path (e.g. `/_search/scroll`), already with the real cursor id.
    pub path: String,
    /// The request body to forward (the real, unwrapped cursor id substituted in).
    pub body: Vec<u8>,
    /// The upstream wire protocol. Defaults to [`Protocol::Http1`].
    pub protocol: Protocol,
    /// The W3C trace context to forward downstream.
    pub trace: Option<TraceContext>,
}

impl CursorOp {
    /// Constructs a cursor passthrough op (defaulting to HTTP/1.1 upstream).
    #[must_use]
    pub fn new(
        cluster: ClusterId,
        method: HttpMethod,
        path: impl Into<String>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            cluster,
            method,
            path: path.into(),
            body,
            protocol: Protocol::Http1,
            trace: None,
        }
    }

    /// Sets the trace context to propagate downstream (builder style).
    #[must_use]
    pub fn with_trace(mut self, trace: Option<TraceContext>) -> Self {
        self.trace = trace;
        self
    }
}

/// The outcome of a cursor passthrough: the upstream status and raw body,
/// forwarded back to the client verbatim.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CursorOutcome {
    /// The upstream HTTP status.
    pub status: u16,
    /// The raw upstream response body.
    pub body: Vec<u8>,
    /// Whether this op rode a reused pooled connection (NFR-P telemetry).
    pub pool_reuse: bool,
}

impl CursorOutcome {
    /// Constructs a cursor outcome.
    #[must_use]
    pub fn new(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            body,
            pool_reuse: false,
        }
    }

    /// Records whether the dispatch reused a pooled connection (builder style).
    #[must_use]
    pub fn with_pool_reuse(mut self, reused: bool) -> Self {
        self.pool_reuse = reused;
        self
    }
}

/// Where reads come from.
///
/// The read counterpart of [`Sink`](crate::Sink). Kept separate because a read
/// is inherently direct-to-cluster: a redundancy `QueueSink` can absorb writes
/// but cannot answer a get-by-id or a search.
///
/// # Invariants
///
/// - MUST NOT panic; return [`SinkError`] for every transport/upstream failure
///   (NFR-R1). A missing document is *not* an error — it is a
///   [`ReadOutcome`] with `found == false`.
pub trait Reader: Send + Sync {
    /// Fetches a single document by physical id.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError`] if the upstream cannot be reached or returns a
    /// server error (a 404 for a missing document is a normal not-found
    /// outcome, not an error).
    fn get(
        &self,
        op: ReadOp,
    ) -> impl std::future::Future<Output = Result<ReadOutcome, SinkError>> + Send;

    /// Runs a search, returning the raw hits envelope.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError`] if the upstream cannot be reached or returns a
    /// server error.
    fn search(
        &self,
        op: SearchOp,
    ) -> impl std::future::Future<Output = Result<SearchOutcome, SinkError>> + Send;

    /// Counts the documents matching a (partition-filtered) query.
    ///
    /// Takes the same [`SearchOp`] as [`Reader::search`] — the wrapped query is
    /// identical — but hits the count endpoint, returning only the total.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError`] if the upstream cannot be reached or returns a
    /// server error.
    fn count(
        &self,
        op: SearchOp,
    ) -> impl std::future::Future<Output = Result<CountOutcome, SinkError>> + Send;

    /// Forwards a raw cursor request to its pinned cluster (scroll/PIT continue,
    /// clear, close). The default is **unsupported** — a sink that cannot
    /// passthrough (the in-memory test sink, a write-only queue) rejects it;
    /// `OpenSearchSink` overrides it with a real upstream call.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError`] if the sink does not support passthrough or the
    /// upstream cannot be reached.
    fn cursor(
        &self,
        _op: CursorOp,
    ) -> impl std::future::Future<Output = Result<CursorOutcome, SinkError>> + Send {
        async {
            Err(SinkError::Transport {
                kind: "cursor passthrough not supported by this sink",
            })
        }
    }
}
