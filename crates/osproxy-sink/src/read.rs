//! The [`Reader`] trait: fetching a single document by physical id.
//!
//! Reads are always direct-to-cluster — unlike writes, they cannot be served by
//! a queue — so the read seam is separate from [`Sink`](crate::Sink). The same
//! backend type may implement both (`OpenSearchSink` does, sharing its pooled
//! connection), while a write-only `QueueSink` implements only [`Sink`].
//!
//! [`Sink`]: crate::Sink

use osproxy_core::Target;

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
}

impl ReadOp {
    /// Constructs a read operation.
    #[must_use]
    pub fn new(target: Target, id: impl Into<String>, routing: Option<String>) -> Self {
        Self {
            target,
            id: id.into(),
            routing,
        }
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
}

impl ReadOutcome {
    /// A hit carrying the stored document body.
    #[must_use]
    pub fn found(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            found: true,
            body,
        }
    }

    /// A miss (no such document).
    #[must_use]
    pub fn not_found(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            found: false,
            body,
        }
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
}

impl SearchOp {
    /// Constructs a search operation.
    #[must_use]
    pub fn new(target: Target, body: Vec<u8>) -> Self {
        Self { target, body }
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
}

impl SearchOutcome {
    /// Constructs a search outcome.
    #[must_use]
    pub fn new(status: u16, body: Vec<u8>) -> Self {
        Self { status, body }
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
}
