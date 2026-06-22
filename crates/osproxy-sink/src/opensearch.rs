//! The direct-to-OpenSearch [`Sink`]: maps each [`WriteOp`] to a REST call and
//! delivers it over a pooled HTTP connection.
//!
//! Connection reuse (TCP, and TLS once the crypto seam is wired) comes from
//! `hyper-util`'s pooled client, so repeated writes to a cluster amortize the
//! handshake (NFR-P). M1 speaks cleartext HTTP to a configured per-cluster
//! endpoint; the TLS [`CryptoProvider`](osproxy_spi) connector attaches here in
//! the transport slice without changing this mapping.
//
// JUSTIFY(file-length): one cohesive unit, the live `OpenSearchSink` and its
// per-cluster `ClusterPool`s (construction, sharded pools, dispatch, per-request
// timeout, circuit breaker, and the pool-reuse stats accessors). These all touch
// the private `clusters` map, so splitting them would force that internal state
// public for no real separation of concerns.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_core::{Clock, ClusterId, SystemClock, TraceContext};
use osproxy_spi::{HttpMethod, Protocol};
use serde_json::Value;

use crate::ack::{OpResult, WriteAck};
use crate::batch::{WriteBatch, WriteOp};
use crate::breaker::Breaker;
use crate::conn::{CountingConnector, PoolStats};
use crate::error::SinkError;
use crate::read::{
    CountOutcome, CursorOp, CursorOutcome, ForwardOp, ReadOp, ReadOutcome, Reader, SearchOp,
    SearchOutcome, StreamingForward, StreamingSearch,
};
use crate::sink::Sink;
use crate::wire::{build_request, doc_uri, parse_result};

/// The error type the upstream body may surface. A buffered body never errors
/// (`Infallible`); a streamed verbatim-forward body surfaces the downstream read
/// error here. Boxed so both fit one client type.
pub type BodyError = Box<dyn std::error::Error + Send + Sync>;

/// A boxed byte-stream body, used both for the request sent **to** an upstream
/// cluster and as the carrier for a downstream body streamed **through** the proxy
/// (a verbatim forward, or a `_bulk` batch the engine frames). Boxed (unsync, the
/// pooled client needs only `Send`, not `Sync`) so one type covers buffered bytes,
/// a stream, or a head + stream-tail without changing the pooled client's type,
/// and so a downstream `hyper::body::Incoming` (which is `Send` but not `Sync`) can
/// be piped straight through (ADR-014).
pub type ByteBody = UnsyncBoxBody<Bytes, BodyError>;

/// Wraps fully-buffered bytes as a [`ByteBody`]. The buffered body is infallible;
/// `match never {}` discharges its `Infallible` error into [`BodyError`].
#[must_use]
pub fn buffered(bytes: Bytes) -> ByteBody {
    Full::new(bytes)
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// Adapts any streaming body into a [`ByteBody`], e.g. the downstream
/// `hyper::body::Incoming` for a verbatim forward or a streamed `_bulk`, so its
/// bytes flow through the proxy without buffering (ADR-014).
pub fn stream_body<B>(body: B) -> ByteBody
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BodyError>,
{
    body.map_err(Into::into).boxed_unsync()
}

type HttpClient = Client<CountingConnector<HttpConnector>, ByteBody>;

/// One cluster's base URL plus its own pooled HTTP/1.1 and HTTP/2 clients.
///
/// Each cluster owns its pools (not a single shared client), so connection-pool
/// state is **sharded per cluster**, a busy cluster's pool lock never contends
/// with another's (NFR-P, `docs/01` §7).
#[derive(Debug)]
struct ClusterPool {
    base: String,
    client_h1: HttpClient,
    client_h2: HttpClient,
    /// Passive health breaker: a run of failures opens it and the cluster is
    /// shed until a cooldown elapses (health-checked eviction).
    breaker: Breaker,
    /// TCP connections this pool has opened (h1 + h2), shared with the counting
    /// connectors wrapping both clients.
    opened: Arc<AtomicU64>,
    /// Requests dispatched to this cluster; `dispatched - opened` is pool reuse.
    dispatched: AtomicU64,
}

impl ClusterPool {
    /// Builds the per-cluster pools for a base URL, each wrapped in a counting
    /// connector so the pool's connection reuse is observable (NFR-P).
    fn new(base: String) -> Self {
        let opened = Arc::new(AtomicU64::new(0));
        // Disable Nagle on upstream connections too: the proxy writes a complete
        // request and waits for the response, so Nagle+delayed-ACK only adds tail
        // latency on a real network. Matches the downstream ingress setting.
        let connector = || {
            let mut http = HttpConnector::new();
            http.set_nodelay(true);
            CountingConnector::new(http, Arc::clone(&opened))
        };
        Self {
            base,
            client_h1: Client::builder(TokioExecutor::new()).build(connector()),
            client_h2: Client::builder(TokioExecutor::new())
                .http2_only(true)
                .build(connector()),
            breaker: Breaker::default(),
            opened,
            dispatched: AtomicU64::new(0),
        }
    }

    /// A snapshot of this pool's connection-reuse counters.
    fn stats(&self) -> PoolStats {
        PoolStats {
            opened: self.opened.load(Ordering::Relaxed),
            dispatched: self.dispatched.load(Ordering::Relaxed),
        }
    }

    /// The pooled client for a resolved upstream protocol: the HTTP/2
    /// (prior-knowledge) client for `Http2`/`Grpc`, the HTTP/1.1 client otherwise.
    fn client(&self, protocol: Protocol) -> &HttpClient {
        match protocol {
            Protocol::Http2 | Protocol::Grpc => &self.client_h2,
            _ => &self.client_h1,
        }
    }
}

/// The default per-request upstream deadline: a hung upstream that accepts the
/// connection but never responds must not stall the request forever (NFR-R7).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How many consecutive transport/timeout failures open a cluster's breaker.
const DEFAULT_FAILURE_THRESHOLD: u32 = 5;

/// How long a cluster is shed once its breaker opens, before a half-open trial.
const DEFAULT_COOLDOWN: Duration = Duration::from_secs(5);

/// A [`Sink`] that writes directly to OpenSearch clusters over pooled HTTP.
///
/// Holds a `ClusterPool` per cluster, its own base URL and pooled HTTP/1.1
/// and HTTP/2 (prior-knowledge) clients. Each operation selects the client
/// matching its resolved upstream [`Protocol`] (`docs/04` §7), so the proxy can
/// speak h2 to a cluster that supports it while defaulting to h1. Every dispatch
/// is bounded by a per-request timeout so a stuck upstream fails fast (NFR-R7),
/// and a per-cluster circuit breaker sheds a cluster that keeps failing.
pub struct OpenSearchSink {
    /// Per-cluster pools, built lazily the first time a placement routes to a
    /// cluster (the endpoint comes from the routing target, sourced from the
    /// tenancy's placement result). Behind a lock because that first dispatch
    /// inserts; afterwards every dispatch is a read-lock + `Arc` clone. With far
    /// fewer than ~1k clusters, creating a pool on first use is cheap.
    clusters: RwLock<HashMap<ClusterId, Arc<ClusterPool>>>,
    timeout: Duration,
    failure_threshold: u32,
    cooldown: Duration,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for OpenSearchSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected `Clock` is not `Debug`; the rest is the useful shape.
        f.debug_struct("OpenSearchSink")
            .field("clusters", &self.clusters)
            .field("timeout", &self.timeout)
            .field("failure_threshold", &self.failure_threshold)
            .field("cooldown", &self.cooldown)
            .finish_non_exhaustive()
    }
}

impl Default for OpenSearchSink {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenSearchSink {
    /// Builds an empty sink. Cluster pools are created on demand from the
    /// endpoint each routing target carries (the tenancy's placement result is
    /// the source of truth for where every cluster lives); there is no static
    /// endpoint catalog.
    #[must_use]
    pub fn new() -> Self {
        Self {
            clusters: RwLock::new(HashMap::new()),
            timeout: DEFAULT_TIMEOUT,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            cooldown: DEFAULT_COOLDOWN,
            clock: Arc::new(SystemClock),
        }
    }

    /// Sets the per-request upstream timeout (builder style).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the circuit-breaker thresholds: open after `failure_threshold`
    /// consecutive failures, shed for `cooldown` before a half-open trial.
    #[must_use]
    pub fn with_breaker(mut self, failure_threshold: u32, cooldown: Duration) -> Self {
        self.failure_threshold = failure_threshold;
        self.cooldown = cooldown;
        self
    }

    /// Swaps the clock the breaker reads (tests inject a `ManualClock`).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// A snapshot of a cluster's connection-reuse counters, or `None` if no pool
    /// has been built for it yet. Lets operators (and tests) verify the pool is
    /// amortizing handshakes, connections opened far below requests dispatched
    /// (NFR-P; the `docs/11` M4 "pool reuse rates verified" exit gate).
    #[must_use]
    pub fn pool_stats(&self, cluster: &ClusterId) -> Option<PoolStats> {
        self.read_clusters().get(cluster).map(|p| p.stats())
    }

    /// Pool-reuse counters for **every** pooled cluster, paired with its id, the
    /// fleet-/agent-facing readout behind the `/metrics` snapshot. Order is
    /// unspecified (a `HashMap` walk); callers that need stability sort by id.
    #[must_use]
    pub fn pool_stats_all(&self) -> Vec<(ClusterId, PoolStats)> {
        self.read_clusters()
            .iter()
            .map(|(id, pool)| (id.clone(), pool.stats()))
            .collect()
    }

    /// Reads the cluster map, recovering the guard if a writer panicked (a poison
    /// only means a pool insert panicked; the entries are still valid to read).
    fn read_clusters(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<ClusterId, Arc<ClusterPool>>> {
        self.clusters
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Resolves a cluster's pool, creating it from `endpoint` on first use. Errors
    /// only when the cluster has no pool yet *and* no endpoint was supplied to
    /// build one (a cursor/admin op routed to a cluster the data plane never hit).
    fn pool_for(
        &self,
        cluster: &ClusterId,
        endpoint: Option<&str>,
    ) -> Result<Arc<ClusterPool>, SinkError> {
        if let Some(pool) = self.read_clusters().get(cluster) {
            return Ok(Arc::clone(pool));
        }
        let Some(base) = endpoint else {
            return Err(SinkError::Transport {
                kind: "no endpoint for target cluster",
            });
        };
        let mut clusters = self
            .clusters
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Another writer may have inserted between the read and the write lock.
        let pool = clusters
            .entry(cluster.clone())
            .or_insert_with(|| Arc::new(ClusterPool::new(base.to_owned())));
        Ok(Arc::clone(pool))
    }

    /// Sends a request to a cluster's pool, bounded by the per-request timeout
    /// and gated by the cluster's circuit breaker.
    ///
    /// A shed request (breaker open) and a transport/timeout failure are both
    /// retryable [`SinkError`]s; failures feed the breaker so a persistently
    /// failing cluster is evicted until it recovers (health-checked eviction).
    ///
    /// On success returns the response plus whether it rode a *reused* pooled
    /// connection: the counting connector only opens (and counts) a connection
    /// when the pool has none to reuse, so an unchanged open-count across the
    /// request means it reused one (NFR-P telemetry).
    async fn send(
        &self,
        pool: &ClusterPool,
        protocol: Protocol,
        mut req: Request<ByteBody>,
        trace: Option<&TraceContext>,
        fail_kind: &'static str,
    ) -> Result<(Response<Incoming>, bool), SinkError> {
        // Propagate the W3C trace context to every upstream call (one choke point).
        crate::trace_headers::inject_trace(&mut req, trace);
        if !pool.breaker.allows(self.clock.now(), self.cooldown) {
            return Err(SinkError::Transport {
                kind: "cluster shed (circuit open)",
            });
        }
        pool.dispatched.fetch_add(1, Ordering::Relaxed);
        let opens_before = pool.opened.load(Ordering::Relaxed);
        match tokio::time::timeout(self.timeout, pool.client(protocol).request(req)).await {
            Ok(Ok(resp)) => {
                pool.breaker.record_success();
                let reused = pool.opened.load(Ordering::Relaxed) == opens_before;
                Ok((resp, reused))
            }
            Ok(Err(_)) => {
                pool.breaker
                    .record_failure(self.clock.now(), self.failure_threshold);
                Err(SinkError::Transport { kind: fail_kind })
            }
            Err(_elapsed) => {
                pool.breaker
                    .record_failure(self.clock.now(), self.failure_threshold);
                Err(SinkError::Transport {
                    kind: "upstream timeout",
                })
            }
        }
    }

    /// POSTs a (partition-filtered) query body to `{index}/{verb}` and returns
    /// the upstream status and raw response body. Shared by search and count.
    /// Sends a query op to `verb` (`_search`/`_count`) and returns the raw
    /// upstream response without reading the body, shared by the buffered
    /// [`post_query`](Self::post_query) and the streaming
    /// [`search_stream`](Reader::search_stream), which differ only in whether they
    /// collect the body or pipe it.
    async fn query_send(
        &self,
        verb: &str,
        op: &SearchOp,
    ) -> Result<(u16, Response<Incoming>, bool), SinkError> {
        let pool = self.pool_for(&op.target.cluster, op.target.endpoint.as_deref())?;
        let base = format!("{}/{}/{verb}", pool.base, op.target.index.as_str());
        // Append the engine's allow-listed query (e.g. `scroll=1m`); never the
        // client's raw query, so no param can bypass the body partition filter.
        let uri = match &op.query {
            Some(q) if !q.is_empty() => format!("{base}?{q}"),
            _ => base,
        };
        let req = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .body(buffered(Bytes::from(op.body.clone())))
            .map_err(|_| SinkError::Transport {
                kind: "building upstream query request",
            })?;

        let (resp, reused) = self
            .send(
                &pool,
                op.protocol,
                req,
                op.trace.as_ref(),
                "upstream query failed",
            )
            .await?;
        let status = resp.status().as_u16();
        reject_5xx(status)?;
        Ok((status, resp, reused))
    }

    async fn post_query(
        &self,
        verb: &str,
        op: &SearchOp,
    ) -> Result<(u16, Vec<u8>, bool), SinkError> {
        let (status, resp, reused) = self.query_send(verb, op).await?;
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|_| SinkError::Transport {
                kind: "reading upstream query response",
            })?
            .to_bytes()
            .to_vec();
        Ok((status, body, reused))
    }

    /// The one verbatim-forward path, shared by the buffered cursor op and the
    /// streaming passthrough: concatenate `op.path` (and any allow-listed query)
    /// onto the cluster base and send `body`, buffered or streamed, upstream.
    ///
    /// Defense in depth: this is the choke point where a passthrough path is
    /// concatenated verbatim into the upstream URI. Refuse a `..` segment so no op
    /// type can let a path resolve past its allow-listed prefix upstream, the
    /// engine already guards admin/cursor paths, so this should never fire.
    async fn forward_send(
        &self,
        op: &ForwardOp,
        body: ByteBody,
        fail_kind: &'static str,
    ) -> Result<(u16, Response<Incoming>, bool), SinkError> {
        reject_path_traversal(&op.path)?;
        let pool = self.pool_for(&op.cluster, op.endpoint.as_deref())?;
        let uri = match &op.query {
            Some(q) if !q.is_empty() => format!("{}{}?{q}", pool.base, op.path),
            _ => format!("{}{}", pool.base, op.path),
        };
        let req = Request::builder()
            .method(hyper_method(op.method))
            .uri(uri)
            .header("content-type", "application/json")
            .body(body)
            .map_err(|_| SinkError::Transport {
                kind: "building upstream forward request",
            })?;
        let (resp, reused) = self
            .send(&pool, op.protocol, req, op.trace.as_ref(), fail_kind)
            .await?;
        let status = resp.status().as_u16();
        reject_5xx(status)?;
        Ok((status, resp, reused))
    }

    /// Sends a single operation and parses its result, with whether the dispatch
    /// reused a pooled connection.
    async fn dispatch(&self, op: &WriteOp) -> Result<(OpResult, bool), SinkError> {
        let pool = self.pool_for(&op.target.cluster, op.target.endpoint.as_deref())?;
        let (req, fallback_id) = build_request(&pool.base, &op.target.index, &op.doc)?;

        let (resp, reused) = self
            .send(
                &pool,
                op.protocol,
                req,
                op.trace.as_ref(),
                "upstream request failed",
            )
            .await?;
        let status = resp.status().as_u16();
        reject_5xx(status)?;

        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|_| SinkError::Transport {
                kind: "reading upstream response",
            })?
            .to_bytes();
        Ok((parse_result(&body, fallback_id, status), reused))
    }
}

impl Reader for OpenSearchSink {
    async fn get(&self, op: ReadOp) -> Result<ReadOutcome, SinkError> {
        let pool = self.pool_for(&op.target.cluster, op.target.endpoint.as_deref())?;
        let uri = doc_uri(
            &pool.base,
            &op.target.index,
            Some(&op.id),
            op.routing.as_deref(),
        );
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(buffered(Bytes::new()))
            .map_err(|_| SinkError::Transport {
                kind: "building upstream read request",
            })?;

        let (resp, reused) = self
            .send(
                &pool,
                op.protocol,
                req,
                op.trace.as_ref(),
                "upstream read failed",
            )
            .await?;
        let status = resp.status().as_u16();
        // 404 is a normal "document does not exist"; only 5xx is a real failure.
        reject_5xx(status)?;
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|_| SinkError::Transport {
                kind: "reading upstream read response",
            })?
            .to_bytes()
            .to_vec();
        Ok(if status == 200 {
            ReadOutcome::found(status, body)
        } else {
            ReadOutcome::not_found(status, body)
        }
        .with_pool_reuse(reused))
    }

    async fn search(&self, op: SearchOp) -> Result<SearchOutcome, SinkError> {
        let (status, body, reused) = self.post_query("_search", &op).await?;
        Ok(SearchOutcome::new(status, body).with_pool_reuse(reused))
    }

    async fn count(&self, op: SearchOp) -> Result<CountOutcome, SinkError> {
        let (status, body, reused) = self.post_query("_count", &op).await?;
        let count = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|v| v.get("count").and_then(Value::as_u64))
            .unwrap_or(0);
        Ok(CountOutcome::new(status, count).with_pool_reuse(reused))
    }

    async fn cursor(&self, op: CursorOp) -> Result<CursorOutcome, SinkError> {
        // A cursor op's body is already buffered (the engine substituted the real
        // cursor id) and its response is small, so buffer the response too.
        let body = buffered(Bytes::from(op.body));
        let fwd = ForwardOp {
            cluster: op.cluster,
            method: op.method,
            path: op.path,
            query: op.query,
            endpoint: op.endpoint,
            protocol: op.protocol,
            trace: op.trace,
        };
        let (status, resp, reused) = self
            .forward_send(&fwd, body, "upstream cursor failed")
            .await?;
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|_| SinkError::Transport {
                kind: "reading upstream cursor response",
            })?
            .to_bytes()
            .to_vec();
        Ok(CursorOutcome::new(status, body).with_pool_reuse(reused))
    }

    async fn search_stream(&self, op: SearchOp) -> Result<StreamingSearch, SinkError> {
        // The search response streams straight back to be transformed on the fly
        // by the engine's hit scanner, never collected here (ADR-014).
        let (status, resp, reused) = self.query_send("_search", &op).await?;
        Ok(StreamingSearch {
            status,
            body: stream_body(resp.into_body()),
            pool_reuse: reused,
        })
    }

    async fn forward_stream(
        &self,
        op: ForwardOp,
        body: ByteBody,
    ) -> Result<StreamingForward, SinkError> {
        // The verbatim-passthrough path: the request body streams straight upstream
        // and the response streams straight back, neither lands in memory (ADR-014
        // stages 2 + the response-streaming follow-up).
        let (status, resp, reused) = self
            .forward_send(&op, body, "upstream forward failed")
            .await?;
        Ok(StreamingForward {
            status,
            body: stream_body(resp.into_body()),
            pool_reuse: reused,
        })
    }
}

/// Maps the SPI method to a hyper method for the cursor passthrough.
fn hyper_method(method: HttpMethod) -> Method {
    match method {
        HttpMethod::Get => Method::GET,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
        HttpMethod::Head => Method::HEAD,
        // `Post` and any future (non-exhaustive) method map to POST, the
        // scroll/PIT continue default.
        _ => Method::POST,
    }
}

impl Sink for OpenSearchSink {
    async fn write(&self, batch: WriteBatch) -> Result<WriteAck, SinkError> {
        // M1 batches are single-op; the loop is the M3 bulk seam (writes to one
        // target are issued in order to preserve item positioning).
        let mut results = Vec::with_capacity(batch.len());
        // The whole batch counts as reuse only if every op rode a pooled
        // connection (an empty batch trivially did).
        let mut all_reused = true;
        for op in batch.ops() {
            let (result, reused) = self.dispatch(op).await?;
            results.push(result);
            all_reused &= reused;
        }
        Ok(WriteAck::new(results).with_pool_reuse(all_reused))
    }
}

/// Rejects a 5xx upstream response as a retryable upstream error (502–504 are
/// retryable); below 500 passes through (e.g. a 404 read is a normal miss).
/// Refuses a forwarded passthrough path containing a `..` segment. Such a path
/// could resolve upstream (or at an intermediary) past the prefix an operator
/// allow-listed; the proxy never normalizes paths, so it fails closed here rather
/// than dispatch. Value-free, like every other [`SinkError`].
fn reject_path_traversal(path: &str) -> Result<(), SinkError> {
    if path.split('/').any(|seg| seg == "..") {
        return Err(SinkError::Transport {
            kind: "refusing a forwarded path with a `..` segment",
        });
    }
    Ok(())
}

fn reject_5xx(status: u16) -> Result<(), SinkError> {
    if status >= 500 {
        return Err(SinkError::Upstream {
            status,
            retryable: matches!(status, 502..=504),
        });
    }
    Ok(())
}
