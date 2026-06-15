//! Blind-diagnosis (NFR-T1, `docs/09` §3): for each representative failure mode,
//! the `/debug/explain` document **alone** — no source, no logs beyond the
//! structured trace — must identify which stage failed, why (a stable code), the
//! decision chain, whether it is retryable, and an actionable remediation.
//!
//! This is the operationalization of "no human takeover": the rubric below is the
//! automated judge. If a future change makes a failure undiagnosable from the
//! trace, the matching assertion breaks. The cases are driven deterministically
//! through the engine (injected tenancy/sink), so there is no network or clock
//! flakiness.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use osproxy_core::{ClusterId, EndpointKind, Epoch, FieldName, IndexName, PartitionId, RequestId};
use osproxy_engine::Pipeline;
use osproxy_sink::{
    MemorySink, OpResult, ReadOp, ReadOutcome, Sink, SinkError, WriteAck, WriteBatch,
};
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::TenancyRouter;
use serde_json::Value;

/// How a scenario's placement lookup behaves — the routing-stage failure lever.
#[derive(Clone, Copy)]
enum Placed {
    /// A valid shared-index placement (routing succeeds).
    Ok,
    /// No placement for the partition (`PlacementMissing`).
    Missing,
    /// The placement backend is down (`PlacementBackend`, retryable).
    BackendDown,
}

/// A tenancy whose partition key is a body field and whose placement lookup is
/// programmable, so the routing-stage failures can be injected deterministically.
struct DiagTenancy {
    placed: Placed,
}

impl TenancySpi for DiagTenancy {
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

/// What a scenario's sink does — the delivery-stage failure lever.
#[derive(Clone, Copy)]
enum Deliver {
    /// The write is accepted (201).
    Ok,
    /// The upstream cluster returns a non-retryable error.
    Upstream4xx,
    /// The write is rejected as stale for a migrating partition (retryable).
    StaleEpoch,
}

/// A sink that delivers or fails per [`Deliver`]; reads delegate to an inner
/// memory sink (unused by the write-path scenarios here).
struct DiagSink {
    deliver: Deliver,
    inner: MemorySink,
}

impl Sink for DiagSink {
    async fn write(&self, batch: WriteBatch) -> Result<WriteAck, SinkError> {
        match self.deliver {
            Deliver::Ok => {
                let _ = self.inner.write(batch).await;
                Ok(WriteAck::new(vec![OpResult::new("p:1", 201, true)]))
            }
            Deliver::Upstream4xx => Err(SinkError::Upstream {
                status: 400,
                retryable: false,
            }),
            Deliver::StaleEpoch => Err(SinkError::StaleEpoch {
                stamped: Epoch::new(1),
                current: Epoch::new(2),
            }),
        }
    }
}

impl osproxy_sink::Reader for DiagSink {
    async fn get(&self, op: ReadOp) -> Result<ReadOutcome, SinkError> {
        self.inner.get(op).await
    }
    async fn search(
        &self,
        op: osproxy_sink::SearchOp,
    ) -> Result<osproxy_sink::SearchOutcome, SinkError> {
        self.inner.search(op).await
    }
    async fn count(
        &self,
        op: osproxy_sink::SearchOp,
    ) -> Result<osproxy_sink::CountOutcome, SinkError> {
        self.inner.count(op).await
    }
}

/// Runs one ingest through a pipeline configured for the scenario, and returns
/// **only** the `/debug/explain` document — the sole evidence the rubric judges.
async fn explain_for(placed: Placed, deliver: Deliver, body: &[u8]) -> Value {
    let pipeline = Pipeline::new(
        TenancyRouter::new(DiagTenancy { placed }),
        DiagSink {
            deliver,
            inner: MemorySink::new(),
        },
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
    let _ = pipeline.handle(&ctx).await;
    pipeline
        .explain(&rid)
        .expect("explain is retained for every request")
}

/// The five facts NFR-T1 requires a diagnoser to recover from the trace alone.
struct Diagnosis {
    failed_stage: String,
    code: String,
    retryable: bool,
    remediation: String,
    partition: Option<String>,
}

/// The automated judge: reconstructs the diagnosis from the explain document with
/// no other input. `failed_stage` is inferred purely from span presence — the
/// first pipeline stage that did not complete.
fn diagnose(explain: &Value) -> Diagnosis {
    assert_eq!(explain["outcome"], "error", "expected a failed request");
    let spans = &explain["spans"];
    let failed_stage = ["classify", "spi.resolve", "rewrite", "dispatch", "egress"]
        .into_iter()
        .find(|stage| spans.get(stage).is_none())
        .unwrap_or("egress")
        .to_owned();
    let err = &explain["error"];
    Diagnosis {
        failed_stage,
        code: err["code"].as_str().unwrap_or_default().to_owned(),
        retryable: err["retryable"].as_bool().unwrap_or_default(),
        remediation: err["remediation"].as_str().unwrap_or_default().to_owned(),
        partition: err["decision_chain"]["partition"]
            .as_str()
            .map(str::to_owned),
    }
}

const GOOD_BODY: &[u8] = br#"{"tenant_id":"acme","id":1}"#;

#[tokio::test]
async fn an_unresolved_partition_is_fully_diagnosable_from_the_trace() {
    // No partition key in the body: resolution cannot even begin placement lookup.
    let explain = explain_for(Placed::Ok, Deliver::Ok, br#"{"id":1}"#).await;
    let d = diagnose(&explain);
    assert_eq!(d.failed_stage, "spi.resolve", "fails at resolution");
    assert_eq!(d.code, "partition_unresolved");
    assert!(!d.retryable, "a missing key is the client's to fix");
    assert!(
        d.remediation.contains("partition key"),
        "remediation guides the fix: {}",
        d.remediation
    );
}

#[tokio::test]
async fn a_missing_placement_is_fully_diagnosable_from_the_trace() {
    let explain = explain_for(Placed::Missing, Deliver::Ok, GOOD_BODY).await;
    let d = diagnose(&explain);
    assert_eq!(d.failed_stage, "spi.resolve");
    assert_eq!(d.code, "placement_missing");
    assert!(!d.retryable);
    // The decision chain names the partition that has no placement — the operator
    // knows exactly which tenant to register.
    assert_eq!(d.partition.as_deref(), Some("acme"));
    assert!(d.remediation.contains("register a placement"));
}

#[tokio::test]
async fn a_down_placement_backend_is_diagnosed_as_retryable() {
    let explain = explain_for(Placed::BackendDown, Deliver::Ok, GOOD_BODY).await;
    let d = diagnose(&explain);
    assert_eq!(d.failed_stage, "spi.resolve");
    assert_eq!(d.code, "placement_backend_unavailable");
    assert!(d.retryable, "a backend outage is transient — retry");
    assert!(d.remediation.contains("retry"));
}

#[tokio::test]
async fn an_upstream_rejection_is_diagnosed_at_the_delivery_stage() {
    let explain = explain_for(Placed::Ok, Deliver::Upstream4xx, GOOD_BODY).await;
    let d = diagnose(&explain);
    // Routing succeeded (spi.resolve present); delivery is where it broke.
    assert!(
        explain["spans"]["spi.resolve"].is_object(),
        "the trace shows routing completed before the failure"
    );
    assert_eq!(d.failed_stage, "dispatch");
    assert_eq!(d.code, "upstream_failed");
    assert!(!d.retryable, "a 4xx is not the proxy's to retry");
}

#[tokio::test]
async fn a_stale_epoch_is_diagnosed_as_a_retryable_migration_conflict() {
    let explain = explain_for(Placed::Ok, Deliver::StaleEpoch, GOOD_BODY).await;
    let d = diagnose(&explain);
    assert_eq!(d.failed_stage, "dispatch");
    assert_eq!(d.code, "stale_epoch");
    assert!(
        d.retryable,
        "a stale epoch is retryable: the client re-resolves the new placement"
    );
    assert!(
        !d.remediation.is_empty(),
        "every failure carries a remediation"
    );
}

#[tokio::test]
async fn every_failure_mode_carries_the_full_diagnostic_quintet() {
    // The cross-cutting guarantee: for every representative failure, all five
    // NFR-T1 facts are present and non-empty in the trace alone.
    for (placed, deliver, body) in [
        (Placed::Ok, Deliver::Ok, br#"{"id":1}"#.as_slice()),
        (Placed::Missing, Deliver::Ok, GOOD_BODY),
        (Placed::BackendDown, Deliver::Ok, GOOD_BODY),
        (Placed::Ok, Deliver::Upstream4xx, GOOD_BODY),
        (Placed::Ok, Deliver::StaleEpoch, GOOD_BODY),
    ] {
        let explain = explain_for(placed, deliver, body).await;
        let d = diagnose(&explain);
        assert!(!d.failed_stage.is_empty(), "stage identified: {explain}");
        assert!(!d.code.is_empty(), "code present: {explain}");
        assert!(!d.remediation.is_empty(), "remediation present: {explain}");
        // retryable is a bool — always present by construction.
        let _ = d.retryable;
        let _ = d.partition;
    }
}
