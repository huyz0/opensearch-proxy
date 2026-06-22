# 02 — SPI Reference

The SPI is the contract between the proxy core and the implementer. It is the
**most important documented surface in the project** — every item here must
carry doc comments stating intent, invariants, panics (none), and an example
(NFR-Q3).

Two layers:

- **Low-level `RoutingSpi`** — full control over the routing decision. Sees the
  authenticated principal, request metadata, and a streaming view of the body;
  returns a `RouteDecision`.
- **High-level `TenancySpi`** — what most users implement. Declares tenancy
  *rules* (partition key, doc-id construction, injected/sensitive fields) and a
  placement lookup. `osproxy-tenancy` implements `RoutingSpi` in terms of it, so
  tenancy users never touch `RouteDecision` plumbing.

> The Rust below is **illustrative of the contract**, not final code. Signatures
> may be refined during implementation, but the semantics and invariants
> documented here are binding and changes require a design-review note ([10](10-review-process.md)).

## 1. Low-level routing SPI

```rust
/// Decides where and how a single request is routed.
///
/// # Invariants
/// - MUST resolve to exactly one [`Target`] (no synchronous fan-out — ADR-002).
/// - MUST NOT block; use async for any lookup. Long lookups risk NFR-P latency.
/// - MUST NOT panic. Return [`SpiError`] for every failure.
/// - The returned [`RouteDecision::epoch`] MUST come from the placement table
///   the decision was derived from, so the sink can detect a stale-epoch write
///   during a migration (see docs/06).
pub trait RoutingSpi: Send + Sync + 'static {
    async fn route(&self, ctx: &RequestCtx<'_>) -> Result<RouteDecision, SpiError>;
}
```

### `RequestCtx` — what the SPI sees

```rust
/// Read-only view of an authenticated request, given to the SPI to decide
/// routing. Body access is a streaming view to preserve NFR-P3/P7 (no full
/// buffering); the SPI pulls only what it needs to find the partition key.
pub struct RequestCtx<'a> {
    pub principal: &'a Principal,        // authenticated identity (never the raw token)
    pub method: HttpMethod,
    pub path: &'a Path,                  // parsed OpenSearch endpoint (typed, see below)
    pub headers: &'a HeaderView<'a>,
    pub protocol: Protocol,              // H1 | H2 | Grpc
    pub body: BodyView<'a>,              // streaming, partition-key extraction only
    pub trace: &'a TraceHandle,          // attach span attributes (shape/ids only)
}
```

### `RouteDecision` — what the SPI returns

```rust
pub struct RouteDecision {
    pub target: Target,                  // cluster id + concrete index
    pub upstream_protocol: Protocol,     // may differ from ingress protocol
    pub header_ops: Vec<HeaderOp>,       // Add | Remove | Replace
    pub body_transform: BodyTransform,   // None | Inject | ConstructId | Both
    pub epoch: Epoch,                    // stamped; mismatch at sink => retryable reject
}
```

> **Read-path transforms are derived, not separate fields.** An earlier draft of
> this struct carried explicit `query_rewrite`, `response_transform`, and
> `affinity` members. They were not added: the read path derives all three from
> what the decision already carries, so they cannot drift out of sync with the
> write path. The mandatory partition **query filter** and the response **field
> strip** are both computed from `body_transform` (the injected `PartitionId`
> field is the isolation key — see `osproxy-engine`'s `read::filter_terms` /
> `read_shape`), and **cursor affinity** is handled by the engine's cursor signer
> on the scroll/PIT endpoints rather than a per-decision flag. This is the real
> shape in `osproxy-spi::decision`; the matching is what keeps write-inject and
> read-strip provably inverse (the round-trip property test, [09](09-testing-and-quality.md)).

`Target`, `BodyTransform`, etc. are defined in `osproxy-core` and documented
there. Key ones:

```rust
pub struct Target { pub cluster: ClusterId, pub index: IndexName }

pub enum BodyTransform {
    None,
    /// Inject named fields with computed values into each ingested document.
    Inject(Vec<InjectedField>),
    /// Construct the document `_id` from a rule (and set `_routing`).
    ConstructId(DocIdRule),
    Both { inject: Vec<InjectedField>, id: DocIdRule },
}
```

## 2. High-level tenancy SPI

```rust
/// The tenancy-focused contract most implementers provide. Declares the rules;
/// the proxy's `osproxy-tenancy` crate turns these into a `RoutingSpi`.
///
/// # Invariants
/// - `resolve_partition` MUST yield a partition id for every routable request,
///   or it returns a typed `SpiError::PartitionUnresolved` and the request is
///   rejected.
/// - In `SharedIndex` mode the partition id MUST be part of the constructed
///   `_id` to prevent cross-tenant id collisions (see docs/03).
/// - `injected_fields` names and `sensitive_fields` MUST be stable for a given
///   logical index version, so read-path strip/filter stays symmetric with the
///   write-path inject.
pub trait TenancySpi: Send + Sync + 'static {
    /// Resolve the partition id for a request. Most impls defer to the
    /// declarative `osproxy_tenancy::resolve_partition_spec` (naming a body
    /// field / header / principal attr); override the body to decode an encoded
    /// header, parse a token, or combine inputs — you choose the order.
    ///
    /// `body` is a [`BodyDoc`] view, NOT a parsed `serde_json::Value`: the proxy
    /// scans the raw bytes on demand for the one scalar the partition key needs,
    /// so no JSON tree is built (ADR-014, INV-MEM). Read it with `body.scalar(path)`.
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError>;

    /// Optional rule to construct the document `_id` (and `_routing`).
    fn doc_id_rule(&self) -> Option<DocIdRule>;

    /// Fields the proxy injects on ingest and strips on read. The field *names*
    /// are chosen here (per the original requirement that the SPI decides them).
    fn injected_fields(&self) -> Vec<InjectedField>;

    /// Declares which fields are sensitive. Drives redaction / value-suppression
    /// so observability never captures these values (NFR-S2).
    fn sensitive_fields(&self) -> SensitivitySpec;

    /// Resolve a partition to its current placement. Looks up the mutable,
    /// epoch-versioned placement table (NOT a pure function — migration mutates
    /// it). Returns the placement *and* the epoch it was read at.
    async fn placement_for(&self, partition: &PartitionId)
        -> Result<PlacementAt, SpiError>;

    /// Migration write gate (docs/06 §2): may a write that resolved at `epoch`
    /// for `partition` still commit? Re-checked at dispatch; `false` ⇒ a retryable
    /// stale-epoch rejection. Defaults to always-admit (no live migration).
    async fn admit_write(&self, _partition: &PartitionId, _epoch: Epoch) -> bool { true }

    /// Base URL of a cluster by id, for the paths that route to a cluster without
    /// a placement to consult (cursor affinity, admin pass-through). `None` ⇒ the
    /// request fails closed rather than route blind. Default `None`.
    fn cluster_endpoint(&self, _cluster: &ClusterId) -> Option<String> { None }
}

pub struct PlacementAt { pub placement: Placement, pub epoch: PlacementEpoch }

pub enum Placement {
    DedicatedCluster { cluster: ClusterId },
    DedicatedIndex   { cluster: ClusterId, index: IndexName },
    SharedIndex      { cluster: ClusterId, index: IndexName,
                       inject: Vec<InjectedField> },
}
```

### Supporting rule types

```rust
/// How to find the partition id in a request.
pub enum PartitionKeySpec {
    /// A JSON path into the document body (ingest) — e.g. "$.tenant_id".
    BodyField(JsonPath),
    /// A request header carries it (e.g. resolved by an upstream auth gateway).
    Header(String),
    /// Derived from a named attribute of the authenticated principal.
    PrincipalAttr(String),
    /// A composite: try in order until one resolves.
    AnyOf(Vec<PartitionKeySpec>),
}

/// Rule to construct a document `_id`. In SharedIndex mode the partition id is
/// mandatory in the template to guarantee global uniqueness.
pub struct DocIdRule {
    pub template: IdTemplate,     // e.g. "{partition}:{body.$.natural_key}"
    pub set_routing: bool,        // also set OpenSearch _routing = partition
}

pub struct InjectedField { pub name: FieldName, pub value: InjectedValue }
pub enum InjectedValue { PartitionId, Constant(JsonValue), FromPrincipal(PrincipalAttr) }
```

## 3. Other SPI-adjacent traits

```rust
/// Pluggable crypto so the FIPS module sits behind a seam (docs/07).
pub trait CryptoProvider: Send + Sync {
    fn server_config(&self) -> Arc<rustls::ServerConfig>;
    fn client_config(&self) -> Arc<rustls::ClientConfig>;
    fn fips_mode(&self) -> bool;
}

/// Where writes go. OpenSearchSink now; QueueSink (Kafka) later for the
/// redundancy mode — same RouteDecision feeds both (docs/00 §non-goals). The
/// per-op `Target` rides inside the batch (each `WriteOp` carries its own), so
/// `write` takes no separate target argument.
pub trait Sink: Send + Sync {
    async fn write(&self, batch: WriteBatch) -> Result<WriteAck, SinkError>;
}

/// The read side is a sibling trait, `Reader`, so a write-only sink need not
/// implement it. It carries the by-id, search, count, and cursor ops, each with a
/// buffered and (where memory matters) a streaming variant:
///
/// ```rust,ignore
/// pub trait Reader: Send + Sync {
///     async fn get(&self, op: ReadOp) -> Result<ReadOutcome, SinkError>;
///     async fn search(&self, op: SearchOp) -> Result<SearchOutcome, SinkError>;
///     async fn count(&self, op: SearchOp) -> Result<CountOutcome, SinkError>;
///     async fn cursor(&self, op: CursorOp) -> Result<CursorOutcome, SinkError>;
///     // Streaming variants (default: unsupported) — response piped back,
///     // never buffered (ADR-014): `search_stream`, `forward_stream`.
///     async fn search_stream(&self, op: SearchOp) -> Result<StreamingSearch, SinkError>;
/// }
/// ```

/// Authenticates a client and returns the principal. mTLS + token.
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, creds: &ClientCredentials)
        -> Result<Principal, AuthError>;
}

/// Authorizes a resolved request. Separate from authentication so policy can
/// evolve independently.
pub trait Authorizer: Send + Sync {
    async fn authorize(&self, principal: &Principal, action: &Action)
        -> Result<(), AuthError>;
}
```

## 4. Error taxonomy

Every error on the request path is a typed enum (no `anyhow`/strings, NFR-R2).
Errors are **contextual** — they carry the decision chain so the LLM can
diagnose without source (NFR-T5).

```rust
/// The top-level request-path error. Built from sub-errors of each stage so the
/// chain (principal -> partition -> placement -> epoch -> upstream) is preserved.
#[derive(thiserror::Error, Debug)]
pub enum RequestError {
    #[error("auth failed")]            Auth(#[from] AuthError),
    #[error("spi routing failed")]    Spi(#[from] SpiError),
    #[error("rewrite failed")]        Rewrite(#[from] RewriteError),
    #[error("sink failed")]           Sink(#[from] SinkError),
    #[error("upstream failed")]       Upstream(#[from] UpstreamError),
    #[error("overloaded")]            Overload(OverloadCtx),
}

/// Common shape: every variant carries a code, the decision chain, retryable
/// flag, and a remediation hint, surfaced into the trace and /debug/explain.
pub struct ErrorContext {
    pub code: ErrorCode,              // stable, documented, machine-matchable
    pub decision_chain: DecisionChain,// ids/shapes only, never values
    pub retryable: bool,
    pub remediation: &'static str,    // actionable hint for an operator/LLM
}
```

`SpiError` variants implementers will return:

```rust
#[derive(thiserror::Error, Debug)]
pub enum SpiError {
    #[error("partition could not be resolved from the request")]
    PartitionUnresolved { tried: Vec<PartitionKeySpecKind> },
    #[error("no placement exists for partition")]
    PlacementMissing { partition: PartitionId },
    #[error("placement lookup backend unavailable")]
    PlacementBackend { retryable: bool },
    #[error("request endpoint is not supported for tenancy rewrite")]
    UnsupportedEndpoint { endpoint: EndpointKind },
    #[error("custom spi rejection")]
    Custom(ErrorContext),
}
```

## 5. Endpoint coverage

The OpenSearch REST surface is large. `osproxy-core` defines a typed
`EndpointKind` enum classifying each path into how it must be handled:

| Class | Examples | Handling |
|-------|----------|----------|
| `IngestDoc` | `PUT/POST /{index}/_doc`, `_create`, `_update` | inject/construct, single target |
| `IngestBulk` | `_bulk` | NDJSON demux by partition, re-interleave |
| `Search` | `_search`, `_count`, `_msearch` | query filter + response strip, single target |
| `GetById` | `GET /{index}/_doc/{id}`, `_mget` | logical->physical id transform |
| `DeleteById` | `DELETE /{index}/_doc/{id}` | id transform |
| `Cursor` | scroll, PIT create/use | affinity pin |
| `Admin` | cat, cluster, indices mgmt | policy: pass-through allow-list or reject |
| `Unknown` | anything unmatched | configurable: reject (default) or pass-through |

The supported matrix is exhaustively listed and version-tracked in
[docs/specs/opensearch-endpoints.md](specs/opensearch-endpoints.md). Adding an
endpoint to a tenancy-aware class requires a test proving symmetry of write/read
transforms.
