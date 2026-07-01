//! The `_bulk` per-item response line, shaped straight to bytes (`docs/04` §3).
//!
//! The bulk response is `{"took":0,"errors":_,"items":[{action:{…}}, …]}`. Each
//! item line has one of three fixed shapes (a result, a queued ack, or an error),
//! so we model it as a small owned [`Line`] and let serde serialize it directly to
//! bytes — no per-item `serde_json::Value` tree is materialized (the response-side
//! twin of the byte-splice ingest path, ADR-014). The dynamic outer key is the
//! action verb (`index`/`create`/`update`/`delete`), a `&'static str`, so the only
//! escaping serde does is on the echoed `_index`/`_id`/`op_id` — which it handles.

use serde::ser::{Serialize, SerializeMap, Serializer};

/// One positional bulk response line: `{action: {_index, _id, status, …}}`. The
/// `index`/`id` are the client's logical view (never the physical id/index).
pub(crate) struct Line {
    action: &'static str,
    index: String,
    id: Option<String>,
    status: u16,
    kind: LineKind,
}

/// The variant-specific tail of a [`Line`]'s body.
enum LineKind {
    /// A applied write: `{status, result}` (`created`/`updated`).
    Result(&'static str),
    /// A durably-enqueued async op: `{op_id, status, result:"queued"}`.
    Queued { op_id: String },
    /// A per-item failure: `{status, error:{type}}` (value-free).
    Error(&'static str),
}

impl Line {
    /// An applied-write line (`created`/`updated`).
    pub(crate) fn result(
        action: &'static str,
        index: String,
        id: Option<String>,
        status: u16,
        outcome: &'static str,
    ) -> Self {
        Self {
            action,
            index,
            id,
            status,
            kind: LineKind::Result(outcome),
        }
    }

    /// A queued-async line carrying the per-item `op_id` (`status` is 202).
    pub(crate) fn queued(
        action: &'static str,
        index: String,
        id: Option<String>,
        status: u16,
        op_id: String,
    ) -> Self {
        Self {
            action,
            index,
            id,
            status,
            kind: LineKind::Queued { op_id },
        }
    }

    /// A per-item error line carrying a value-free error `type`.
    pub(crate) fn error(
        action: &'static str,
        index: String,
        id: Option<String>,
        status: u16,
        error: &'static str,
    ) -> Self {
        Self {
            action,
            index,
            id,
            status,
            kind: LineKind::Error(error),
        }
    }

    /// Whether this line reports a per-item error (drives the batch `errors` flag).
    pub(crate) fn is_error(&self) -> bool {
        matches!(self.kind, LineKind::Error(_))
    }
}

impl Serialize for Line {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // The one-entry outer object: the action verb mapping to the body.
        let mut outer = serializer.serialize_map(Some(1))?;
        outer.serialize_entry(self.action, &Body(self))?;
        outer.end()
    }
}

/// The inner body object of a [`Line`], serialized in place so it never becomes a
/// `Value` tree.
struct Body<'a>(&'a Line);

impl Serialize for Body<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let line = self.0;
        let mut m = serializer.serialize_map(None)?;
        m.serialize_entry("_index", &line.index)?;
        // `_id` is `null` when absent (e.g. a delete without an id), matching the
        // previous `Value`-built shape.
        m.serialize_entry("_id", &line.id)?;
        match &line.kind {
            LineKind::Result(outcome) => {
                m.serialize_entry("status", &line.status)?;
                m.serialize_entry("result", outcome)?;
            }
            LineKind::Queued { op_id } => {
                m.serialize_entry("op_id", op_id)?;
                m.serialize_entry("status", &line.status)?;
                m.serialize_entry("result", "queued")?;
            }
            LineKind::Error(error) => {
                m.serialize_entry("status", &line.status)?;
                m.serialize_entry("error", &ErrorType { r#type: error })?;
            }
        }
        m.end()
    }
}

/// The `{ "type": … }` error object nested under an error line.
#[derive(serde::Serialize)]
struct ErrorType {
    r#type: &'static str,
}

/// The whole bulk response body: `{"took":0,"errors":_,"items":[…]}`. Serialized
/// once from the positional line slice; an unfilled slot serializes as `null`
/// (unreachable — every ordinal is filled — but preserves the array shape).
#[derive(serde::Serialize)]
pub(crate) struct BulkBody<'a> {
    pub(crate) took: u8,
    pub(crate) errors: bool,
    pub(crate) items: &'a [Option<Line>],
}
