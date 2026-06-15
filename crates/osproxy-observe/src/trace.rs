//! The per-request causal trace — **shape-only by construction**.
//!
//! [`RequestTrace`] accumulates what happened to one request as it crosses each
//! stage. Its setters accept *only* identifier newtypes, compile-time `&'static
//! str` shape labels, and numeric sizes/counts — never a `String`/`&str` taken
//! from request data and never a JSON value. There is therefore **no API path**
//! by which a document field value, query literal, or secret can enter a trace
//! (`docs/05` §7); the guarantee is structural, not redaction after the fact.

use osproxy_core::{
    ClusterId, EndpointKind, Epoch, ErrorContext, FieldName, IndexName, PartitionId,
};

/// The `ingress` span: how the connection was framed (`docs/05` §2).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IngressInfo {
    /// Wire protocol label, e.g. `"h1"`.
    pub protocol: &'static str,
    /// Negotiated TLS suite label, if the connection was TLS.
    pub tls_suite: Option<&'static str>,
    /// Whether the TLS session was resumed.
    pub tls_reused: Option<bool>,
}

/// The `classify` span: how the request path was categorized.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ClassifyInfo {
    /// The endpoint classification.
    pub endpoint: EndpointKind,
    /// The logical index from the path (a name, never a value).
    pub logical_index: IndexName,
}

/// The `spi.resolve` span: the routing decision and its inputs.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ResolveInfo {
    /// The resolved partition (an id).
    pub partition: PartitionId,
    /// The placement mode label, e.g. `"shared_index"`.
    pub placement_kind: &'static str,
    /// The target cluster.
    pub cluster: ClusterId,
    /// The target index.
    pub index: IndexName,
    /// The placement epoch the decision was derived from.
    pub epoch: Epoch,
    /// The names of fields injected (names only, never values).
    pub inject_fields: Vec<FieldName>,
    /// Whether `_routing` was set.
    pub routing: bool,
    /// The partition's migration phase at resolve time, e.g. `"settled"` /
    /// `"draining"` / `"cutover"` — so an operator sees where a migration is
    /// without reading values (`docs/06` §5).
    pub migration: &'static str,
}

/// The `rewrite` span: what the body transform did (in shapes).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RewriteInfo {
    /// The transform kind label, e.g. `"inject+construct_id"`.
    pub transform_kind: &'static str,
    /// The transformed body size in bytes (a size, never the bytes).
    pub body_bytes: usize,
}

/// The `dispatch` span: the upstream call outcome.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DispatchInfo {
    /// The cluster the request was sent to.
    pub cluster: ClusterId,
    /// The upstream HTTP status.
    pub upstream_status: u16,
    /// Whether a pooled connection was reused.
    pub pool_reuse: bool,
}

/// The `egress` span: what was returned to the client.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EgressInfo {
    /// The status returned to the client.
    pub status: u16,
    /// The response size in bytes.
    pub response_bytes: usize,
}

/// The accumulated causal trace for one request, filled stage by stage.
///
/// Constructed with the [`RequestId`](osproxy_core::RequestId) and populated via
/// the `record_*` setters; assembled into a `/debug/explain` document by
/// [`crate::explain_json`].
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RequestTrace {
    pub(crate) ingress: Option<IngressInfo>,
    pub(crate) classify: Option<ClassifyInfo>,
    pub(crate) resolve: Option<ResolveInfo>,
    pub(crate) rewrite: Option<RewriteInfo>,
    pub(crate) dispatch: Option<DispatchInfo>,
    pub(crate) egress: Option<EgressInfo>,
    pub(crate) error: Option<ErrorContext>,
}

impl RequestTrace {
    /// A new, empty trace.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the `ingress` span.
    pub fn record_ingress(&mut self, info: IngressInfo) {
        self.ingress = Some(info);
    }

    /// Records the `classify` span.
    pub fn record_classify(&mut self, info: ClassifyInfo) {
        self.classify = Some(info);
    }

    /// Records the `spi.resolve` span.
    pub fn record_resolve(&mut self, info: ResolveInfo) {
        self.resolve = Some(info);
    }

    /// Records the `rewrite` span.
    pub fn record_rewrite(&mut self, info: RewriteInfo) {
        self.rewrite = Some(info);
    }

    /// Records the `dispatch` span.
    pub fn record_dispatch(&mut self, info: DispatchInfo) {
        self.dispatch = Some(info);
    }

    /// Records the `egress` span.
    pub fn record_egress(&mut self, info: EgressInfo) {
        self.egress = Some(info);
    }

    /// Attaches the error context to the failing span.
    pub fn record_error(&mut self, error: ErrorContext) {
        self.error = Some(error);
    }

    /// Whether the request failed (carries an error context).
    #[must_use]
    pub fn failed(&self) -> bool {
        self.error.is_some()
    }
}
