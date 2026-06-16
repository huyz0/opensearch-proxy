//! Cursor (scroll) affinity (`docs/03` §6): a continued scroll routes to the
//! cluster pinned in its signed envelope — recovered from the token alone, so any
//! fleet instance resolves it — and fails closed (never a blind dispatch) when
//! affinity is off or the envelope does not verify.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use osproxy_core::cursor::{self, CursorSigner};
use osproxy_core::{ClusterId, EndpointKind, Epoch, ErrorCode, IndexName, PartitionId, RequestId};
use osproxy_engine::{Pipeline, RequestError};
use osproxy_sink::{
    CountOutcome, CursorOp, CursorOutcome, MemorySink, ReadOp, ReadOutcome, Reader, SearchOp,
    SearchOutcome, Sink, SinkError, WriteAck, WriteBatch,
};
use osproxy_spi::{
    DocIdRule, HeaderView, HttpMethod, InjectedField, JsonPath, PartitionKeySpec, Placement,
    PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec, SpiError, TenancySpi,
};
use osproxy_tenancy::TenancyRouter;

/// A deterministic stand-in for the HMAC signer (keyed FNV-1a fold) — same key on
/// every instance, so a token wrapped on one verifies on another.
struct FnvSigner(u64);
impl CursorSigner for FnvSigner {
    fn tag(&self, msg: &[u8]) -> Vec<u8> {
        let mut h = 0xcbf2_9ce4_8422_2325 ^ self.0;
        for &b in msg {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h.to_be_bytes().to_vec()
    }
}

/// A sink that records the cursor passthrough op it is handed and returns a fixed
/// response; the typed ops delegate to an inner `MemorySink` (unused here).
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
    async fn search(&self, _op: SearchOp) -> Result<SearchOutcome, SinkError> {
        // A scroll-opening search: the upstream returns a `_scroll_id` the proxy
        // must wrap before handing it to the client.
        Ok(SearchOutcome::new(
            200,
            br#"{"_scroll_id":"UPSTREAMSCROLL","hits":{"total":{"value":0},"hits":[]}}"#.to_vec(),
        ))
    }
    async fn count(&self, op: SearchOp) -> Result<CountOutcome, SinkError> {
        self.inner.count(op).await
    }
    async fn cursor(&self, op: CursorOp) -> Result<CursorOutcome, SinkError> {
        *self.seen.lock().unwrap() = Some(op);
        Ok(CursorOutcome::new(200, br#"{"hits":{"hits":[]}}"#.to_vec()))
    }
}

/// A tenancy the cursor path never consults (it bypasses resolution); present
/// only to satisfy the pipeline's type.
struct StubTenancy;
impl TenancySpi for StubTenancy {
    fn partition_key(&self) -> PartitionKeySpec {
        PartitionKeySpec::BodyField(JsonPath::new("tenant_id"))
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
        // Resolve every partition to one shared cluster — the search test needs a
        // successful resolution; the cursor tests bypass this entirely.
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

fn pipeline(
    signer: Option<Arc<dyn CursorSigner>>,
) -> (
    Pipeline<StubTenancy, RecordingSink>,
    Arc<Mutex<Option<CursorOp>>>,
) {
    let (sink, seen) = RecordingSink::new();
    let mut p = Pipeline::new(TenancyRouter::new(StubTenancy), sink);
    if let Some(s) = signer {
        p = p.with_cursor_signer(s);
    }
    (p, seen)
}

/// Drives one cursor request (method, body, optional path-form id) through the
/// pipeline and returns the result.
async fn run(
    p: &Pipeline<StubTenancy, RecordingSink>,
    method: HttpMethod,
    body: &[u8],
    path_form_id: Option<&str>,
) -> Result<(), RequestError> {
    let principal = Principal::new(osproxy_core::PrincipalId::from("svc"));
    let rid = RequestId::from("r");
    let headers: Vec<(String, String)> = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        method,
        EndpointKind::Cursor,
        Protocol::Http1,
        "",
        HeaderView::new(&headers),
        body,
    )
    .with_doc_id(path_form_id);
    p.handle(&ctx).await.map(|_| ())
}

const REAL_ID: &str = "DXF1ZXJ5QW5kRmV0Y2grealScrollId==";

#[tokio::test]
async fn a_continued_scroll_routes_to_its_pinned_cluster_with_the_real_id() {
    let signer = Arc::new(FnvSigner(9));
    let token = cursor::wrap(signer.as_ref(), &ClusterId::from("eu-1"), REAL_ID);
    let (p, seen) = pipeline(Some(signer));

    let body = format!(r#"{{"scroll":"1m","scroll_id":"{token}"}}"#);
    run(&p, HttpMethod::Post, body.as_bytes(), None)
        .await
        .expect("a valid cursor routes");

    let op = seen
        .lock()
        .unwrap()
        .clone()
        .expect("sink received the cursor op");
    assert_eq!(
        op.cluster,
        ClusterId::from("eu-1"),
        "routed to the pinned cluster"
    );
    let forwarded = String::from_utf8(op.body).unwrap();
    assert!(
        forwarded.contains(REAL_ID),
        "real id substituted: {forwarded}"
    );
    assert!(
        !forwarded.contains(&token),
        "the wrapper must be stripped before upstream"
    );
    assert!(
        forwarded.contains(r#""scroll":"1m""#),
        "keep-alive preserved: {forwarded}"
    );
}

#[tokio::test]
async fn the_path_form_scroll_id_is_unwrapped_from_the_doc_id() {
    let signer = Arc::new(FnvSigner(9));
    let token = cursor::wrap(signer.as_ref(), &ClusterId::from("us-2"), REAL_ID);
    let (p, seen) = pipeline(Some(signer));

    run(&p, HttpMethod::Get, b"", Some(&token))
        .await
        .expect("path-form cursor routes");
    let op = seen.lock().unwrap().clone().unwrap();
    assert_eq!(op.cluster, ClusterId::from("us-2"));
    assert!(String::from_utf8(op.body).unwrap().contains(REAL_ID));
}

#[tokio::test]
async fn a_scroll_opening_search_wraps_the_scroll_id_for_the_client() {
    // The create → continue loop closes: a search that opens a scroll returns a
    // *wrapped* `_scroll_id` that unwraps back to the cluster it was served from.
    let signer = Arc::new(FnvSigner(5));
    let (p, _seen) = pipeline(Some(signer.clone()));
    let principal = Principal::new(osproxy_core::PrincipalId::from("svc"));
    let rid = RequestId::from("s");
    let headers: Vec<(String, String)> = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::Search,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        br#"{"query":{"match_all":{}},"tenant_id":"acme"}"#,
    );
    let resp = p.handle(&ctx).await.expect("search succeeds");
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let wrapped = v["_scroll_id"]
        .as_str()
        .expect("response carries a scroll id");
    assert_ne!(
        wrapped, "UPSTREAMSCROLL",
        "the raw upstream id must not leak"
    );
    let (cluster, real) = cursor::unwrap(signer.as_ref(), wrapped).expect("the token verifies");
    assert_eq!(
        cluster,
        ClusterId::from("eu-1"),
        "pinned to the serving cluster"
    );
    assert_eq!(real, "UPSTREAMSCROLL", "unwraps to the real upstream id");
}

#[tokio::test]
async fn a_pit_close_routes_to_its_pinned_cluster_via_the_pit_endpoint() {
    // `DELETE /_pit {"id": <wrapped>}` recovers the cluster from the body `id` and
    // forwards the close to the pinned cluster at `/_pit` with the real id.
    let signer = Arc::new(FnvSigner(3));
    let token = cursor::wrap(signer.as_ref(), &ClusterId::from("eu-1"), REAL_ID);
    let (p, seen) = pipeline(Some(signer));

    let body = format!(r#"{{"id":"{token}"}}"#);
    run(&p, HttpMethod::Delete, body.as_bytes(), None)
        .await
        .expect("pit close routes");
    let op = seen.lock().unwrap().clone().unwrap();
    assert_eq!(op.cluster, ClusterId::from("eu-1"));
    assert_eq!(op.path, "/_pit", "pit close targets the _pit endpoint");
    let forwarded = String::from_utf8(op.body).unwrap();
    assert!(
        forwarded.contains(REAL_ID),
        "real id substituted: {forwarded}"
    );
    assert!(
        !forwarded.contains(&token),
        "wrapper stripped before upstream"
    );
}

#[tokio::test]
async fn affinity_disabled_fails_closed() {
    let (p, seen) = pipeline(None); // no signer ⇒ affinity off
    let err = run(&p, HttpMethod::Post, br#"{"scroll_id":"anything"}"#, None)
        .await
        .expect_err("cursor must fail when affinity is off");
    assert_eq!(err.code(), ErrorCode::CursorUnresolvable);
    assert!(!err.retryable(), "re-issue the search, not a blind retry");
    assert!(seen.lock().unwrap().is_none(), "no dispatch on failure");
}

#[tokio::test]
async fn a_forged_cursor_fails_closed_without_dispatch() {
    // A token signed with a different key must not verify, and must not route.
    let real = Arc::new(FnvSigner(1));
    let foreign = FnvSigner(2);
    let token = cursor::wrap(&foreign, &ClusterId::from("eu-1"), REAL_ID);
    let (p, seen) = pipeline(Some(real));

    let body = format!(r#"{{"scroll_id":"{token}"}}"#);
    let err = run(&p, HttpMethod::Post, body.as_bytes(), None)
        .await
        .expect_err("a forged cursor must be rejected");
    assert_eq!(err.code(), ErrorCode::CursorUnresolvable);
    assert!(
        seen.lock().unwrap().is_none(),
        "a forged cursor is never dispatched"
    );
}
