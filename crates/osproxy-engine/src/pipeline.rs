//! The request pipeline: orchestrates a classified request through routing,
//! transform, and delivery, returning a response for the transport to write.
//!
//! M1 implements the single-document ingest path (`docs/04` §1): resolve the
//! routing decision, build the epoch-stamped write batch, dispatch it to the
//! sink, and shape the acknowledgement into an OpenSearch-style response. Read,
//! bulk, and by-id paths attach here in later milestones.

use std::sync::Arc;

use osproxy_core::{EndpointKind, RequestId};
use osproxy_observe::{ClassifyInfo, EgressInfo, ExplainStore, RequestTrace};
use osproxy_sink::{Sink, WriteAck};
use osproxy_spi::{RequestCtx, SpiError, TenancySpi};
use osproxy_tenancy::TenancyRouter;
use serde_json::Value;

use crate::error::RequestError;
use crate::observe::{dispatch_info, error_context, logical_index, resolve_info, rewrite_info};
use crate::plan::build_write_batch;

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
#[derive(Debug)]
pub struct Pipeline<T, S> {
    router: TenancyRouter<T>,
    sink: S,
    explain: Arc<ExplainStore>,
}

impl<T: TenancySpi, S: Sink> Pipeline<T, S> {
    /// Builds a pipeline from a router and a sink.
    pub fn new(router: TenancyRouter<T>, sink: S) -> Self {
        Self {
            router,
            sink,
            explain: Arc::new(ExplainStore::new(EXPLAIN_CAPACITY)),
        }
    }

    /// The assembled `/debug/explain` document for a past request, if retained.
    #[must_use]
    pub fn explain(&self, request_id: &RequestId) -> Option<Value> {
        self.explain.get(request_id)
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
        let mut trace = RequestTrace::new();
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
            other => Err(RequestError::Spi(SpiError::UnsupportedEndpoint {
                endpoint: other,
            })),
        }
    }

    /// The single-document ingest path.
    async fn ingest_doc(
        &self,
        ctx: &RequestCtx<'_>,
        trace: &mut RequestTrace,
    ) -> Result<PipelineResponse, RequestError> {
        let resolved = self.router.resolve(ctx).await?;
        trace.record_resolve(resolve_info(&resolved));

        let batch = build_write_batch(&resolved, ctx.body())?;
        trace.record_rewrite(rewrite_info(&resolved, &batch));

        let ack = self.sink.write(batch).await?;
        trace.record_dispatch(dispatch_info(&resolved, &ack));
        Ok(response_for(&ack))
    }
}

/// Shapes a write acknowledgement into an OpenSearch-style ingest response.
///
/// For a single-document write the ack carries one result; its status and the
/// created/updated outcome are surfaced as the client would expect.
fn response_for(ack: &WriteAck) -> PipelineResponse {
    let Some(result) = ack.results().first() else {
        // No operations is not reachable from the single-doc path, but never
        // panic: report an empty 200 rather than unwrapping (NFR-R1).
        return PipelineResponse {
            status: 200,
            body: b"{}".to_vec(),
        };
    };
    let outcome = if result.created { "created" } else { "updated" };
    let body = format!(r#"{{"_id":"{}","result":"{outcome}"}}"#, result.id).into_bytes();
    PipelineResponse {
        status: result.status,
        body,
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
    async fn non_ingest_endpoint_is_unsupported_in_m1() {
        let p = pipeline();
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("r");
        let headers = vec![];
        let c = ctx(
            &principal,
            &rid,
            &headers,
            EndpointKind::Search,
            br#"{"q":1}"#,
        );
        let err = p.handle(&c).await.unwrap_err();
        assert!(matches!(
            err,
            RequestError::Spi(SpiError::UnsupportedEndpoint {
                endpoint: EndpointKind::Search
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
            EndpointKind::Search,
            br#"{"q":1}"#,
        );
        let _ = p.handle(&c).await;

        let doc = p.explain(&rid).expect("trace recorded");
        assert_eq!(doc["outcome"], "error");
        assert_eq!(doc["error"]["code"], "unsupported_endpoint");
        assert!(doc["error"]["remediation"].is_string());
    }
}
