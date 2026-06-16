//! The pipeline's OTLP export wiring, end to end through `Pipeline::handle`: a
//! handled request hands exactly one OTLP span to the configured exporter,
//! carrying the same trace id surfaced in `/debug/explain`; with no exporter
//! configured nothing is exported (and the request still succeeds).

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use osproxy_core::{
    Clock, ClusterId, EndpointKind, FieldName, IndexName, Instant, ManualClock, PartitionId,
    PrincipalId, RequestId,
};
use osproxy_engine::Pipeline;
use osproxy_observe::{
    BreakGlassBuffer, DiagLevel, DiagnosticsDirective, DirectiveMatch, DirectiveSet,
    DirectiveVerifier, InMemoryDirectiveStore, SpanExporter,
};
use osproxy_sink::MemorySink;
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter};
use serde_json::Value;

/// An exporter that records the payloads it is handed.
#[derive(Clone, Default)]
struct RecordingExporter(Arc<Mutex<Vec<Value>>>);

impl SpanExporter for RecordingExporter {
    fn export(&self, payload: Value) {
        self.0.lock().unwrap().push(payload);
    }
}

struct SharedTenancy {
    table: Arc<PlacementTable>,
}

impl TenancySpi for SharedTenancy {
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
    async fn placement_for(&self, p: &PartitionId) -> Result<PlacementAt, SpiError> {
        self.table.get(p).ok_or_else(|| SpiError::PlacementMissing {
            partition: p.clone(),
        })
    }
}

fn pipeline() -> Pipeline<TenancyRouter<SharedTenancy>, MemorySink> {
    let table = Arc::new(PlacementTable::new());
    table.set(
        PartitionId::from("acme"),
        Placement::SharedIndex {
            cluster: ClusterId::from("eu-1"),
            index: IndexName::from("shared"),
            inject: vec![InjectedField::new(
                FieldName::from("_tenant"),
                InjectedValue::PartitionId,
            )],
        },
    );
    Pipeline::new(
        TenancyRouter::new(SharedTenancy { table }),
        MemorySink::new(),
    )
}

async fn ingest(p: &Pipeline<TenancyRouter<SharedTenancy>, MemorySink>, rid: &RequestId) {
    let principal = Principal::new(PrincipalId::from("svc"));
    let headers: Vec<(String, String)> = vec![];
    let body = br#"{"tenant_id":"acme","id":7}"#;
    let ctx = RequestCtx::new(
        &principal,
        rid,
        HttpMethod::Put,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        body,
    );
    p.handle(&ctx).await.unwrap();
}

#[tokio::test]
async fn a_handled_request_exports_one_span_with_the_explain_trace_id() {
    let exporter = RecordingExporter::default();
    let p = pipeline()
        .with_exporter(Arc::new(exporter.clone()))
        .with_clock(Arc::new(ManualClock::new()))
        .with_service_name("osproxy-test");

    let rid = RequestId::from("r");
    ingest(&p, &rid).await;

    let payloads = exporter.0.lock().unwrap();
    assert_eq!(payloads.len(), 1, "exactly one span exported per request");
    let span = &payloads[0]["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
    // Same trace id the operator would see in /debug/explain — the two correlate.
    let explain_trace_id = p.explain(&rid).unwrap()["trace_id"].clone();
    assert_eq!(span["traceId"], explain_trace_id);
    assert_eq!(
        payloads[0]["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"],
        "osproxy-test"
    );
}

#[tokio::test]
async fn the_default_pipeline_exports_nothing() {
    // No exporter configured (NoopExporter is disabled): a request still succeeds
    // and nothing is shipped — verified by the absence of any export side effect.
    let exporter = RecordingExporter::default();
    let p = pipeline(); // default: no exporter
    ingest(&p, &RequestId::from("r")).await;
    assert!(
        exporter.0.lock().unwrap().is_empty(),
        "an unconfigured pipeline exports nothing"
    );
}

#[tokio::test]
async fn baseline_off_suppresses_export_until_a_directive_selects_the_request() {
    // Baseline Off makes export purely directive-driven: with no directive the
    // exporter is configured but ships nothing.
    let off = RecordingExporter::default();
    let off_pipeline = pipeline()
        .with_exporter(Arc::new(off.clone()))
        .with_clock(Arc::new(ManualClock::new()))
        .with_baseline_level(DiagLevel::Off);
    ingest(&off_pipeline, &RequestId::from("r")).await;
    assert!(
        off.0.lock().unwrap().is_empty(),
        "baseline Off + no directive exports nothing"
    );

    // A directive targeting the request's tenant re-enables export for it.
    let on = RecordingExporter::default();
    let clock = Arc::new(ManualClock::new());
    let directive = DiagnosticsDirective {
        id: "watch-acme".to_owned(),
        match_: DirectiveMatch::all().for_tenant(PartitionId::from("acme")),
        level: DiagLevel::Shape,
        sample_per_mille: 1000,
        expires_at: clock.now().saturating_add(Duration::from_secs(3600)),
        ring_buffer: false,
    };
    let on_pipeline = pipeline()
        .with_exporter(Arc::new(on.clone()))
        .with_clock(clock)
        .with_baseline_level(DiagLevel::Off)
        .with_directives(Arc::new(DirectiveSet::from_directives(vec![directive])));
    ingest(&on_pipeline, &RequestId::from("r")).await;
    assert_eq!(
        on.0.lock().unwrap().len(),
        1,
        "a directive targeting the tenant re-enables export"
    );
}

#[tokio::test]
async fn publishing_to_the_fleet_store_flips_export_without_rebuilding_the_pipeline() {
    // The fleet-wide channel: the pipeline polls a shared store fresh per request,
    // so a controller publishing a directive flips verbosity with no restart.
    let exporter = RecordingExporter::default();
    let clock = Arc::new(ManualClock::new());
    let store = Arc::new(InMemoryDirectiveStore::new());
    let p = pipeline()
        .with_exporter(Arc::new(exporter.clone()))
        .with_clock(clock.clone())
        .with_baseline_level(DiagLevel::Off)
        .with_directive_store(store.clone());

    // Empty store: nothing exports.
    ingest(&p, &RequestId::from("r")).await;
    assert!(
        exporter.0.lock().unwrap().is_empty(),
        "empty fleet store exports nothing"
    );

    // The operator publishes a fleet-wide directive — the running pipeline picks
    // it up on the next request.
    store.publish(DirectiveSet::from_directives(vec![DiagnosticsDirective {
        id: "fleet-on".to_owned(),
        match_: DirectiveMatch::all(),
        level: DiagLevel::Shape,
        sample_per_mille: 1000,
        expires_at: clock.now().saturating_add(Duration::from_secs(3600)),
        ring_buffer: false,
    }]));
    ingest(&p, &RequestId::from("r")).await;
    assert_eq!(
        exporter.0.lock().unwrap().len(),
        1,
        "a freshly published fleet directive flips export on with no restart"
    );

    // The reverse edge: publishing an empty set flips export back off, again
    // without rebuilding — the operator "turn it off" path.
    store.publish(DirectiveSet::new());
    ingest(&p, &RequestId::from("r")).await;
    assert_eq!(
        exporter.0.lock().unwrap().len(),
        1,
        "clearing the fleet store stops further export (count unchanged)"
    );
}

#[tokio::test]
async fn a_ring_buffer_directive_captures_into_the_break_glass_tape() {
    // Break-glass is off by default: no capture until a ring_buffer directive.
    let clock = Arc::new(ManualClock::new());
    let tape = Arc::new(BreakGlassBuffer::new(8));
    let store = Arc::new(InMemoryDirectiveStore::new());
    let p = pipeline()
        .with_clock(clock.clone())
        .with_baseline_level(DiagLevel::Off)
        .with_directive_store(store.clone())
        .with_break_glass(tape.clone());

    // No directive: nothing captured (and no exporter is even configured).
    ingest(&p, &RequestId::from("r")).await;
    assert!(tape.is_empty(), "no ring_buffer directive → empty tape");

    // An operator flips a ring_buffer directive on: matching requests are taped.
    store.publish(DirectiveSet::from_directives(vec![DiagnosticsDirective {
        id: "break-glass".to_owned(),
        match_: DirectiveMatch::all(),
        level: DiagLevel::Off, // capture does not require a raised export level
        sample_per_mille: 1000,
        expires_at: clock.now().saturating_add(Duration::from_secs(3600)),
        ring_buffer: true,
    }]));
    ingest(&p, &RequestId::from("r1")).await;
    ingest(&p, &RequestId::from("r2")).await;

    let captured = tape.snapshot();
    assert_eq!(
        captured.len(),
        2,
        "each matching request is captured in order"
    );
    assert_eq!(captured[0]["request_id"], "r1");
    assert_eq!(captured[1]["request_id"], "r2");
}

#[tokio::test]
async fn an_expired_directive_does_not_re_enable_export() {
    let exporter = RecordingExporter::default();
    let clock = Arc::new(ManualClock::new());
    // Expires in the past relative to the pipeline clock (which is at 0): a
    // forgotten "on" cannot keep exporting.
    let directive = DiagnosticsDirective {
        id: "stale".to_owned(),
        match_: DirectiveMatch::all(),
        level: DiagLevel::Shape,
        sample_per_mille: 1000,
        expires_at: clock.now(), // == now, so `now < expires_at` is false
        ring_buffer: false,
    };
    let p = pipeline()
        .with_exporter(Arc::new(exporter.clone()))
        .with_clock(clock)
        .with_baseline_level(DiagLevel::Off)
        .with_directives(Arc::new(DirectiveSet::from_directives(vec![directive])));
    ingest(&p, &RequestId::from("r")).await;
    assert!(
        exporter.0.lock().unwrap().is_empty(),
        "an expired directive does not export"
    );
}

/// A stand-in for the real HMAC verifier: authorizes a Shape directive only for
/// the exact token `go` (a real one would verify a signature).
struct FakeVerifier {
    expires_at: Instant,
}

impl DirectiveVerifier for FakeVerifier {
    fn verify(&self, header_value: &str) -> Option<DiagnosticsDirective> {
        (header_value == "go").then(|| DiagnosticsDirective {
            id: "header".to_owned(),
            match_: DirectiveMatch::all(),
            level: DiagLevel::Shape,
            sample_per_mille: 1000,
            expires_at: self.expires_at,
            ring_buffer: false,
        })
    }
}

async fn ingest_with_directive(
    p: &Pipeline<TenancyRouter<SharedTenancy>, MemorySink>,
    header: Option<&str>,
) {
    let principal = Principal::new(PrincipalId::from("svc"));
    let headers: Vec<(String, String)> = header
        .into_iter()
        .map(|h| ("x-debug-directive".to_owned(), h.to_owned()))
        .collect();
    let rid = RequestId::from("r");
    let body = br#"{"tenant_id":"acme","id":7}"#;
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Put,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "logical",
        HeaderView::new(&headers),
        body,
    );
    p.handle(&ctx).await.unwrap();
}

#[tokio::test]
async fn a_validly_signed_header_enables_export_for_its_request_only() {
    let clock = Arc::new(ManualClock::new());
    let verifier = FakeVerifier {
        expires_at: clock.now().saturating_add(Duration::from_secs(600)),
    };

    let exporter = RecordingExporter::default();
    let p = pipeline()
        .with_exporter(Arc::new(exporter.clone()))
        .with_clock(clock)
        .with_baseline_level(DiagLevel::Off) // export only what a directive selects
        .with_directive_verifier(Arc::new(verifier));

    // No header: baseline Off, nothing exported.
    ingest_with_directive(&p, None).await;
    assert!(
        exporter.0.lock().unwrap().is_empty(),
        "no header → no export"
    );

    // A wrongly-signed header is rejected by the verifier: still nothing.
    ingest_with_directive(&p, Some("forged")).await;
    assert!(
        exporter.0.lock().unwrap().is_empty(),
        "bad token → no export"
    );

    // The valid header authorizes export for this request.
    ingest_with_directive(&p, Some("go")).await;
    assert_eq!(
        exporter.0.lock().unwrap().len(),
        1,
        "a validly signed X-Debug-Directive enables export"
    );
}
