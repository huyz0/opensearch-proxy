//! Fault-injection / chaos suite (NFR-R7, `docs/09` §5): the proxy must survive
//! the failure catalogue — slow/dropped upstreams, upstream 4xx/5xx, stale-epoch
//! conflicts, malformed bodies, partial bulk failures — **without a panic, a
//! stuck request, or an untyped error**. Every failure surfaces as a typed
//! [`RequestError`] carrying a code, a retryable classification, and a
//! remediation in the `/debug/explain` chain (NFR-R4/R5/T5).
//!
//! **Deterministic** (`docs/09` §exit): no sleeps, no wall-clock, no network. The
//! suite injects the *outcome* of each fault through a programmable tenancy/sink
//! and asserts the engine's handling. The "no stuck request" guarantee for a
//! genuinely slow or dropped upstream is enforced where the deadline lives — the
//! sink's per-request timeout (`osproxy-sink`'s
//! `a_stuck_upstream_times_out_and_is_retryable`) — not re-simulated with a real
//! sleep here. That a test simply *returns* is the no-panic proof; a panic on the
//! request path would unwind into it.
//!
//! **R7 catalogue coverage** (this file is the index; some invariants are proven
//! in the canonical home of the mechanism rather than duplicated here):
//! - slow upstream → stuck-request bound: `osproxy-sink`
//!   `a_stuck_upstream_times_out_and_is_retryable` (real timeout); classification
//!   here as `Fault::Timeout`.
//! - dropped connection / upstream 4xx-5xx / stale epoch: here
//!   (`every_upstream_fault_is_typed_classified_and_never_panics`).
//! - malformed bodies: here (`malformed_bodies_never_panic_and_stay_typed`).
//! - routing-stage faults (unresolved / missing / backend-down): here
//!   (`routing_faults_are_typed_and_classified`).
//! - partial bulk failure positioned in `items[]`: `osproxy-engine`
//!   `tests/bulk.rs` (order preserved, per-item error positioned).
//! - pool exhaustion / backpressure → graceful 413/429: `osproxy-transport`
//!   `tests/ingress.rs` (`body_over_the_per_request_cap_is_413`,
//!   `body_over_the_inflight_ceiling_is_shed_with_429`).
//! - graceful drain under shutdown: `osproxy-transport` `tests/shutdown.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use osproxy_core::{
    ClusterId, EndpointKind, Epoch, ErrorCode, FieldName, IndexName, PartitionId, RequestId,
};
use osproxy_engine::{Pipeline, RequestError};
use osproxy_sink::{
    CountOutcome, MemorySink, ReadOp, ReadOutcome, Reader, SearchOp, SearchOutcome, Sink,
    SinkError, WriteAck, WriteBatch,
};
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::TenancyRouter;

/// How routing behaves — the resolution-stage fault lever.
#[derive(Clone, Copy)]
enum Placed {
    Ok,
    Missing,
    BackendDown,
}

/// What the upstream does — the delivery-stage fault lever, covering the
/// `docs/09` §5 catalogue.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// A dropped connection (transport reset) — retryable.
    Reset,
    /// A slow upstream that timed out — retryable (the sink enforces the deadline).
    Timeout,
    /// Upstream 5xx — retryable.
    Upstream5xx,
    /// Upstream 4xx — terminal (not the proxy's to retry).
    Upstream4xx,
    /// A migrating partition rejected the stamped epoch — retryable after
    /// re-resolution.
    StaleEpoch,
}

impl Fault {
    /// The injected sink error this fault delivers.
    fn sink_error(self) -> SinkError {
        match self {
            Fault::Reset => SinkError::Transport {
                kind: "connection reset",
            },
            Fault::Timeout => SinkError::Transport {
                kind: "upstream timeout",
            },
            Fault::Upstream5xx => SinkError::Upstream {
                status: 503,
                retryable: true,
            },
            Fault::Upstream4xx => SinkError::Upstream {
                status: 400,
                retryable: false,
            },
            Fault::StaleEpoch => SinkError::StaleEpoch {
                stamped: Epoch::new(1),
                current: Epoch::new(2),
            },
        }
    }

    /// Whether the proxy should classify this fault as retryable (NFR-R4).
    fn retryable(self) -> bool {
        !matches!(self, Fault::Upstream4xx)
    }

    /// The error code the proxy should surface for this fault (NFR-T5).
    fn expected_code(self) -> ErrorCode {
        match self {
            Fault::StaleEpoch => ErrorCode::StaleEpoch,
            _ => ErrorCode::UpstreamFailed,
        }
    }

    /// The whole catalogue, for the cross-cutting matrix.
    const ALL: [Fault; 5] = [
        Fault::Reset,
        Fault::Timeout,
        Fault::Upstream5xx,
        Fault::Upstream4xx,
        Fault::StaleEpoch,
    ];
}

/// A tenancy whose placement lookup is programmable, so routing-stage faults can
/// be injected deterministically.
struct FaultTenancy {
    placed: Placed,
}

impl TenancySpi for FaultTenancy {
    fn partition_key(&self) -> PartitionKeySpec {
        PartitionKeySpec::BodyField(JsonPath::new("tenant_id"))
    }
    fn doc_id_rule(&self) -> Option<DocIdRule> {
        Some(DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true))
    }
    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )]
    }
    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::none()
    }
    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        match self.placed {
            Placed::Ok => Ok(PlacementAt::new(
                Placement::SharedIndex {
                    cluster: ClusterId::from("eu-1"),
                    index: IndexName::from("shared"),
                    inject: vec![InjectedField::new(
                        FieldName::from("_tenant"),
                        InjectedValue::PartitionId,
                    )],
                },
                Epoch::new(1),
            )),
            Placed::Missing => Err(SpiError::PlacementMissing {
                partition: partition.clone(),
            }),
            Placed::BackendDown => Err(SpiError::PlacementBackend { retryable: true }),
        }
    }
}

/// A sink that delivers the injected [`Fault`] on writes (and reads), or succeeds
/// via an inner [`MemorySink`] when no fault is set.
struct FaultSink {
    fault: Option<Fault>,
    inner: MemorySink,
}

impl FaultSink {
    fn new(fault: Option<Fault>) -> Self {
        Self {
            fault,
            inner: MemorySink::new(),
        }
    }
}

impl Sink for FaultSink {
    async fn write(&self, batch: WriteBatch) -> Result<WriteAck, SinkError> {
        match self.fault {
            Some(f) => Err(f.sink_error()),
            None => self.inner.write(batch).await,
        }
    }
}

impl Reader for FaultSink {
    async fn get(&self, op: ReadOp) -> Result<ReadOutcome, SinkError> {
        match self.fault {
            Some(f) => Err(f.sink_error()),
            None => self.inner.get(op).await,
        }
    }
    async fn search(&self, op: SearchOp) -> Result<SearchOutcome, SinkError> {
        match self.fault {
            Some(f) => Err(f.sink_error()),
            None => self.inner.search(op).await,
        }
    }
    async fn count(&self, op: SearchOp) -> Result<CountOutcome, SinkError> {
        match self.fault {
            Some(f) => Err(f.sink_error()),
            None => self.inner.count(op).await,
        }
    }
}

const GOOD_BODY: &[u8] = br#"{"tenant_id":"acme","id":1}"#;

/// Drives one ingest through a pipeline configured for `placed`/`fault`, and
/// returns the typed result plus the request id so the caller can read the
/// `/debug/explain` chain.
async fn ingest(
    placed: Placed,
    fault: Option<Fault>,
    body: &[u8],
) -> (
    Result<(), RequestError>,
    RequestId,
    Pipeline<FaultTenancy, FaultSink>,
) {
    let pipeline = Pipeline::new(
        TenancyRouter::new(FaultTenancy { placed }),
        FaultSink::new(fault),
    );
    let principal = Principal::new(osproxy_core::PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers: Vec<(String, String)> = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Put,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        body,
    );
    let result = pipeline.handle(&ctx).await.map(|_| ());
    (result, rid, pipeline)
}

/// Asserts the explain chain for a failed request carries the NFR-T5 quartet.
fn assert_explain_complete(pipeline: &Pipeline<FaultTenancy, FaultSink>, rid: &RequestId) {
    let explain = pipeline.explain(rid).expect("explain retained");
    assert_eq!(explain["outcome"], "error");
    let err = &explain["error"];
    assert!(
        err["code"].as_str().is_some_and(|c| !c.is_empty()),
        "code: {explain}"
    );
    assert!(err["retryable"].as_bool().is_some(), "retryable: {explain}");
    assert!(
        err["remediation"].as_str().is_some_and(|r| !r.is_empty()),
        "remediation: {explain}"
    );
}

#[tokio::test]
async fn every_upstream_fault_is_typed_classified_and_never_panics() {
    for fault in Fault::ALL {
        let (result, rid, pipeline) = ingest(Placed::Ok, Some(fault), GOOD_BODY).await;
        let err = result.expect_err(&format!("{fault:?} should fail the request"));
        // Typed (not a string/anyhow) error with the expected code, classified
        // retryable/terminal per R4.
        assert_eq!(err.code(), fault.expected_code(), "{fault:?} wrong code");
        assert_eq!(
            err.retryable(),
            fault.retryable(),
            "{fault:?} misclassified retryable"
        );
        // The decision chain reconstructs the failure (R5/T5).
        assert_explain_complete(&pipeline, &rid);
    }
}

#[tokio::test]
async fn routing_faults_are_typed_and_classified() {
    // Missing placement: terminal (operator must register it).
    let (missing, rid_m, p_m) = ingest(Placed::Missing, None, GOOD_BODY).await;
    let err = missing.expect_err("missing placement fails");
    assert_eq!(err.code(), ErrorCode::PlacementMissing);
    assert!(!err.retryable());
    assert_explain_complete(&p_m, &rid_m);

    // Placement backend down: retryable (a transient control-plane outage).
    let (down, rid_d, p_d) = ingest(Placed::BackendDown, None, GOOD_BODY).await;
    let err = down.expect_err("backend down fails");
    assert_eq!(err.code(), ErrorCode::PlacementBackendUnavailable);
    assert!(err.retryable());
    assert_explain_complete(&p_d, &rid_d);
}

#[tokio::test]
async fn malformed_bodies_never_panic_and_stay_typed() {
    // Each is a different way to be malformed; none may panic, all stay typed.
    let cases: [&[u8]; 4] = [
        b"",                       // empty
        b"not json at all",        // garbage
        br#"{"id":1}"#,            // valid JSON, missing the partition key
        br#"{"tenant_id":"acme""#, // truncated JSON
    ];
    for body in cases {
        let (result, rid, pipeline) = ingest(Placed::Ok, None, body).await;
        // A malformed body is the client's fault: it must fail (never silently
        // succeed) with a typed, terminal error — and never panic.
        let err = result.expect_err(&format!("malformed body should fail: {body:?}"));
        assert!(
            !err.retryable(),
            "a malformed body is not retryable: {body:?}"
        );
        assert_explain_complete(&pipeline, &rid);
    }
}

#[tokio::test]
async fn the_whole_catalogue_resolves_to_a_typed_outcome() {
    // Cross-cutting: every (routing × upstream) combination resolves to either a
    // success or a typed error with a complete decision chain — never a panic.
    for placed in [Placed::Ok, Placed::Missing, Placed::BackendDown] {
        for fault in Fault::ALL.map(Some).into_iter().chain([None]) {
            let (result, rid, pipeline) = ingest(placed, fault, GOOD_BODY).await;
            if result.is_err() {
                assert_explain_complete(&pipeline, &rid);
            }
        }
    }
}
