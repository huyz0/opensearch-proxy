//! The request pipeline: orchestrates a classified request through routing,
//! transform, and delivery, returning a response for the transport to write.
//!
//! M1 implements the single-document ingest path (`docs/04` §1): resolve the
//! routing decision, build the epoch-stamped write batch, dispatch it to the
//! sink, and shape the acknowledgement into an OpenSearch-style response. M2
//! adds the get-by-id read path (`docs/04` §5): resolve, map the logical id to
//! the physical id, fetch, and shape the stored document back into the client's
//! logical view. Search and bulk attach here in later milestones.

use std::sync::Arc;

use osproxy_core::{Clock, EndpointKind, RequestId, SystemClock};
use osproxy_observe::{
    resource_spans, ClassifyInfo, EgressInfo, ExplainStore, NoopExporter, RequestTrace,
    SpanExporter,
};
use osproxy_sink::{Reader, Sink};
use osproxy_spi::{RequestCtx, SpiError, TenancySpi};
use osproxy_tenancy::TenancyRouter;
use serde_json::Value;

use crate::error::RequestError;
use crate::observe::{error_context, logical_index};

/// How many recent request explanations `/debug/explain` retains per instance.
const EXPLAIN_CAPACITY: usize = 1024;

/// The response the pipeline produces for a handled request.
///
/// A status plus a JSON body, mirroring the relevant fields of an OpenSearch
/// response so the transport can relay it to the client unchanged.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PipelineResponse {
    /// The HTTP status to return to the client.
    pub status: u16,
    /// The JSON response body.
    pub body: Vec<u8>,
}

/// Orchestrates requests through a tenancy router and a sink.
///
/// Generic over the [`TenancySpi`] implementation and the [`Sink`], so the hot
/// path is monomorphized (no dyn dispatch) and tests can swap in an in-memory
/// sink.
pub struct Pipeline<T, S> {
    pub(crate) router: TenancyRouter<T>,
    pub(crate) sink: S,
    pub(crate) retry: crate::RetryPolicy,
    explain: Arc<ExplainStore>,
    exporter: Arc<dyn SpanExporter>,
    clock: Arc<dyn Clock>,
    service_name: String,
}

impl<T, S> std::fmt::Debug for Pipeline<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The injected exporter/clock are not `Debug`; show the rest of the shape.
        f.debug_struct("Pipeline")
            .field("retry", &self.retry)
            .field("service_name", &self.service_name)
            .field("exporting", &self.exporter.enabled())
            .finish_non_exhaustive()
    }
}

impl<T: TenancySpi, S: Sink + Reader> Pipeline<T, S> {
    /// Builds a pipeline from a router and a sink (default backend-retry policy,
    /// no span export).
    pub fn new(router: TenancyRouter<T>, sink: S) -> Self {
        Self {
            router,
            sink,
            retry: crate::RetryPolicy::default(),
            explain: Arc::new(ExplainStore::new(EXPLAIN_CAPACITY)),
            exporter: Arc::new(NoopExporter),
            clock: Arc::new(SystemClock),
            service_name: "osproxy".to_owned(),
        }
    }

    /// Sets the placement-backend retry policy (builder style).
    #[must_use]
    pub fn with_retry_policy(mut self, retry: crate::RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Sets the OTLP span exporter (builder style). Default is no export.
    #[must_use]
    pub fn with_exporter(mut self, exporter: Arc<dyn SpanExporter>) -> Self {
        self.exporter = exporter;
        self
    }

    /// Swaps the clock used to stamp span timestamps (tests inject a `ManualClock`).
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the `service.name` reported on exported spans (builder style).
    #[must_use]
    pub fn with_service_name(mut self, service_name: impl Into<String>) -> Self {
        self.service_name = service_name.into();
        self
    }

    /// The assembled `/debug/explain` document for a past request, if retained.
    #[must_use]
    pub fn explain(&self, request_id: &RequestId) -> Option<Value> {
        self.explain.get(request_id)
    }

    /// The underlying sink (e.g. to inspect what an in-memory sink recorded).
    #[must_use]
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Handles an authenticated request, dispatching on its endpoint class.
    ///
    /// Records a shape-only causal trace for every request (success or failure)
    /// into the explain store, so `/debug/explain/{id}` can reconstruct it
    /// (`docs/05`).
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`] if the endpoint is unsupported in M1, routing
    /// fails, the body transform fails, or the sink rejects the write.
    pub async fn handle(&self, ctx: &RequestCtx<'_>) -> Result<PipelineResponse, RequestError> {
        // Only pay for span timing/encoding when an exporter is actually active —
        // "Off" stays near-zero cost (`docs/05`).
        let exporting = self.exporter.enabled();
        let start_nanos = if exporting {
            self.clock.unix_nanos()
        } else {
            0
        };

        let mut trace = RequestTrace::new();
        // The same W3C context propagated to downstream calls is recorded here, so
        // `/debug/explain` and the exported OTLP span share the request's ids.
        trace.record_context(crate::endpoints::wire_trace(ctx));
        trace.record_classify(ClassifyInfo {
            endpoint: ctx.endpoint(),
            logical_index: logical_index(ctx.logical_index()),
        });

        let result = self.dispatch(ctx, &mut trace).await;
        match &result {
            Ok(resp) => trace.record_egress(EgressInfo {
                status: resp.status,
                response_bytes: resp.body.len(),
            }),
            Err(err) => trace.record_error(error_context(err)),
        }
        self.explain.record(ctx.request_id().clone(), &trace);

        // Hand the finished trace to the exporter (background, best-effort — never
        // blocks or fails the response).
        if exporting {
            let end_nanos = self.clock.unix_nanos();
            if let Some(payload) = resource_spans(
                &self.service_name,
                ctx.request_id(),
                &trace,
                start_nanos,
                end_nanos,
            ) {
                self.exporter.export(payload);
            }
        }
        result
    }

    /// Dispatches on endpoint class, recording the per-stage spans into `trace`.
    async fn dispatch(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        match ctx.endpoint() {
            EndpointKind::IngestDoc => self.ingest_doc(ctx, trace).await,
            EndpointKind::IngestBulk => self.ingest_bulk(ctx, trace).await,
            EndpointKind::GetById => self.get_by_id(ctx, trace).await,
            EndpointKind::MultiGet => self.multi_get(ctx, trace).await,
            EndpointKind::DeleteById => self.delete_by_id(ctx, trace).await,
            EndpointKind::Search => self.search(ctx, trace).await,
            EndpointKind::MultiSearch => self.multi_search(ctx, trace).await,
            EndpointKind::Count => self.count(ctx, trace).await,
            other => Err(RequestError::Spi(SpiError::UnsupportedEndpoint {
                endpoint: other,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use osproxy_core::{ClusterId, FieldName, IndexName, PartitionId, PrincipalId, RequestId};
    use osproxy_sink::MemorySink;
    use osproxy_spi::{
        DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
        PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, SensitivitySpec,
    };
    use osproxy_tenancy::PlacementTable;

    struct Tenancy {
        table: Arc<PlacementTable>,
    }

    impl TenancySpi for Tenancy {
        fn partition_key(&self) -> PartitionKeySpec {
            // Ingest resolves from the body; by-id reads (no body) from a header.
            PartitionKeySpec::AnyOf(vec![
                PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
                PartitionKeySpec::Header("x-tenant".to_owned()),
            ])
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

    fn pipeline() -> Pipeline<Tenancy, MemorySink> {
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
            TenancyRouter::new(Tenancy {
                table: Arc::clone(&table),
            }),
            MemorySink::new(),
        )
    }

    fn ctx<'a>(
        principal: &'a Principal,
        rid: &'a RequestId,
        headers: &'a [(String, String)],
        endpoint: EndpointKind,
        body: &'a [u8],
    ) -> RequestCtx<'a> {
        RequestCtx::new(
            principal,
            rid,
            HttpMethod::Put,
            endpoint,
            Protocol::Http1,
            "logical",
            HeaderView::new(headers),
            body,
        )
    }

    #[tokio::test]
    async fn ingest_doc_returns_created_response() {
        let p = pipeline();
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("r");
        let headers = vec![];
        let c = ctx(
            &principal,
            &rid,
            &headers,
            EndpointKind::IngestDoc,
            br#"{"tenant_id":"acme","id":7}"#,
        );
        let resp = p.handle(&c).await.unwrap();
        assert_eq!(resp.status, 201);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains(r#""_id":"acme:7""#), "{body}");
        assert!(body.contains(r#""result":"created""#));
    }

    #[tokio::test]
    async fn unimplemented_endpoint_is_unsupported() {
        // Cursor is tenancy-aware but not yet wired in the pipeline (M5).
        let p = pipeline();
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("r");
        let headers = vec![];
        let c = ctx(
            &principal,
            &rid,
            &headers,
            EndpointKind::Cursor,
            br#"{"q":1}"#,
        );
        let err = p.handle(&c).await.unwrap_err();
        assert!(matches!(
            err,
            RequestError::Spi(SpiError::UnsupportedEndpoint {
                endpoint: EndpointKind::Cursor
            })
        ));
    }

    #[tokio::test]
    async fn explain_records_success_spans() {
        let p = pipeline();
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("trace-ok");
        let headers = vec![];
        let c = ctx(
            &principal,
            &rid,
            &headers,
            EndpointKind::IngestDoc,
            br#"{"tenant_id":"acme","id":7}"#,
        );
        p.handle(&c).await.unwrap();

        let doc = p.explain(&rid).expect("trace recorded");
        assert_eq!(doc["outcome"], "ok");
        assert_eq!(doc["spans"]["spi.resolve"]["partition_id"], "acme");
        assert_eq!(doc["spans"]["spi.resolve"]["routing"], true);
        assert_eq!(
            doc["spans"]["rewrite"]["transform_kind"],
            "inject+construct_id"
        );
        assert_eq!(doc["spans"]["egress"]["status"], 201);
        assert!(doc["error"].is_null());
    }

    #[tokio::test]
    async fn explain_records_failure_with_remediation() {
        // A placement-missing failure: the reference table here always resolves,
        // so drive an unsupported endpoint instead — still a recorded failure.
        let p = pipeline();
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("trace-err");
        let headers = vec![];
        let c = ctx(
            &principal,
            &rid,
            &headers,
            EndpointKind::IngestBulk,
            br#"{"q":1}"#,
        );
        let _ = p.handle(&c).await;

        let doc = p.explain(&rid).expect("trace recorded");
        assert_eq!(doc["outcome"], "error");
        assert_eq!(doc["error"]["code"], "unsupported_endpoint");
        assert!(doc["error"]["remediation"].is_string());
    }
}
