//! Admin (`_cat`/`_cluster`/`_nodes`) pass-through (`docs/03` §6): with an
//! operator policy, an allow-listed admin request is forwarded verbatim to the
//! configured cluster; without one — or for a path off the allow-list — it is
//! rejected (fail-closed, `docs/decisions/006`). Admin output is not
//! tenancy-filtered, so the full path and query reach the upstream unchanged.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use osproxy_core::Epoch;
use osproxy_core::{ClusterId, EndpointKind, ErrorCode, IndexName, PartitionId, RequestId};
use osproxy_engine::{AdminPolicy, Pipeline, RequestError};
use osproxy_sink::{
    CountOutcome, CursorOp, CursorOutcome, MemorySink, ReadOp, ReadOutcome, Reader, SearchOp,
    SearchOutcome, Sink, SinkError, WriteAck, WriteBatch,
};
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, InjectedField, JsonPath, PartitionKeySpec, Placement,
    PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec, SpiError, TenancySpi,
};
use osproxy_tenancy::TenancyRouter;

/// The concrete pipeline these tests drive (the stub tenancy over a recording
/// sink), aliased so the nested router type stays readable in signatures.
type StubPipeline = Pipeline<TenancyRouter<StubTenancy>, RecordingSink>;

/// Records the passthrough op and returns a fixed admin-looking response.
struct RecordingSink {
    seen: Arc<Mutex<Option<CursorOp>>>,
    inner: MemorySink,
}

impl RecordingSink {
    fn new() -> (Self, Arc<Mutex<Option<CursorOp>>>) {
        let seen = Arc::new(Mutex::new(None));
        (
            Self {
                seen: seen.clone(),
                inner: MemorySink::new(),
            },
            seen,
        )
    }
}

impl Sink for RecordingSink {
    async fn write(&self, batch: WriteBatch) -> Result<WriteAck, SinkError> {
        self.inner.write(batch).await
    }
}

impl Reader for RecordingSink {
    async fn get(&self, op: ReadOp) -> Result<ReadOutcome, SinkError> {
        self.inner.get(op).await
    }
    async fn search(&self, op: SearchOp) -> Result<SearchOutcome, SinkError> {
        self.inner.search(op).await
    }
    async fn count(&self, op: SearchOp) -> Result<CountOutcome, SinkError> {
        self.inner.count(op).await
    }
    async fn cursor(&self, op: CursorOp) -> Result<CursorOutcome, SinkError> {
        *self.seen.lock().unwrap() = Some(op);
        Ok(CursorOutcome::new(200, br#"[{"index":"a"}]"#.to_vec()))
    }
}

/// A tenancy the admin path never consults (admin bypasses resolution).
struct StubTenancy;
impl TenancySpi for StubTenancy {
    fn resolve_partition(
        &self,
        ctx: &osproxy_spi::RequestCtx<'_>,
        doc: Option<&serde_json::Value>,
    ) -> Result<osproxy_core::PartitionId, osproxy_spi::SpiError> {
        osproxy_tenancy::resolve_partition_spec(
            &PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            ctx,
            doc,
        )
    }
    fn doc_id_rule(&self) -> Option<DocIdRule> {
        None
    }
    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![]
    }
    fn sensitive_fields(&self) -> SensitivitySpec {
        SensitivitySpec::none()
    }
    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(
            Placement::SharedIndex {
                cluster: ClusterId::from("eu-1"),
                index: IndexName::from("shared"),
                inject: vec![],
            },
            Epoch::new(1),
        ))
    }
}

fn pipeline(policy: Option<AdminPolicy>) -> (StubPipeline, Arc<Mutex<Option<CursorOp>>>) {
    let (sink, seen) = RecordingSink::new();
    let mut p = Pipeline::new(TenancyRouter::new(StubTenancy), sink);
    if let Some(policy) = policy {
        p = p.with_admin_passthrough(policy);
    }
    (p, seen)
}

async fn run(
    p: &StubPipeline,
    path: &str,
    query: Option<&str>,
) -> Result<(u16, Vec<u8>), RequestError> {
    let principal = Principal::new(osproxy_core::PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers: Vec<(String, String)> = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Get,
        EndpointKind::Admin,
        Protocol::Http1,
        "",
        HeaderView::new(&headers),
        b"",
    )
    .with_path(path)
    .with_query(query);
    p.handle(&ctx).await.map(|r| (r.status, r.body))
}

#[tokio::test]
async fn an_allow_listed_admin_request_forwards_verbatim_to_the_admin_cluster() {
    let policy = AdminPolicy::new(ClusterId::from("admin-1"), vec!["/_cat/".to_owned()]);
    let (p, seen) = pipeline(Some(policy));

    let (status, body) = run(&p, "/_cat/indices", Some("v&format=json"))
        .await
        .expect("allow-listed admin request passes through");
    assert_eq!(status, 200);
    assert_eq!(body, br#"[{"index":"a"}]"#);

    let op = seen.lock().unwrap().clone().expect("forwarded to the sink");
    assert_eq!(
        op.cluster,
        ClusterId::from("admin-1"),
        "routed to admin cluster"
    );
    assert_eq!(op.path, "/_cat/indices", "full path forwarded verbatim");
    // Admin is not tenancy-filtered, so the whole query is forwarded (unlike the
    // cursor allow-list) — there is no body partition filter to bypass.
    assert_eq!(op.query.as_deref(), Some("v&format=json"));
}

#[tokio::test]
async fn an_admin_request_off_the_allow_list_is_rejected() {
    // Allowing `_cat` does not open `_cluster/settings`.
    let policy = AdminPolicy::new(ClusterId::from("admin-1"), vec!["/_cat/".to_owned()]);
    let (p, seen) = pipeline(Some(policy));

    let err = run(&p, "/_cluster/settings", None)
        .await
        .expect_err("a non-allow-listed admin path is rejected");
    assert_eq!(err.code(), ErrorCode::UnsupportedEndpoint);
    assert!(seen.lock().unwrap().is_none(), "no dispatch on rejection");
}

#[tokio::test]
async fn a_traversal_path_cannot_escape_the_allow_listed_prefix() {
    // `/_cat/../_cluster/settings` matches the `/_cat/` prefix textually but would
    // resolve upstream to a non-allow-listed endpoint — the allow-list is an
    // authorization boundary, so it must be rejected with no dispatch.
    let policy = AdminPolicy::new(ClusterId::from("admin-1"), vec!["/_cat/".to_owned()]);
    let (p, seen) = pipeline(Some(policy));

    let err = run(&p, "/_cat/../_cluster/settings", None)
        .await
        .expect_err("a traversal path must be rejected");
    assert_eq!(err.code(), ErrorCode::UnsupportedEndpoint);
    assert!(
        seen.lock().unwrap().is_none(),
        "no dispatch on a traversal path"
    );
}

#[tokio::test]
async fn admin_is_rejected_when_no_policy_is_configured() {
    let (p, seen) = pipeline(None); // pass-through off (the default)
    let err = run(&p, "/_cat/indices", None)
        .await
        .expect_err("admin is rejected by default");
    assert_eq!(err.code(), ErrorCode::UnsupportedEndpoint);
    assert!(seen.lock().unwrap().is_none(), "no dispatch when disabled");
}
