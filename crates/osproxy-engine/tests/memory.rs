//! Deterministic allocation budget for the `_bulk` demux (the highest-throughput
//! ingest path, `docs/04` §3, NFR-P3/P7). A single-partition bulk resolves its
//! placement once and shares it across every item, so the **marginal** allocation
//! cost of each additional document must not include re-cloning the placement or
//! re-deriving its inject pairs. This guards the per-request resolution cache:
//! before it, every item cloned the resolved placement and re-collected the
//! inject vector, so the marginal per-doc cost was markedly higher.
//!
//! Like `osproxy-rewrite/tests/memory.rs`, this is an **upper bound**, not an
//! exact count: owned buffers realloc a profile-/allocator-dependent number of
//! times. The bound still decisively catches a regression that reintroduces
//! per-item placement work. The dhat allocator is installed for this binary only.

// Test scaffolding (a tenancy impl + helpers, not `#[test]` fns).
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use dhat::{HeapStats, Profiler};
use osproxy_core::{
    ClusterId, EndpointKind, FieldName, IndexName, PartitionId, PrincipalId, RequestId,
};
use osproxy_engine::Pipeline;
use osproxy_sink::MemorySink;
use osproxy_spi::{
    BodyDoc, DocIdRule, HeaderView, HttpMethod, IdTemplate, InjectedField, InjectedValue, JsonPath,
    PartitionKeySpec, Placement, PlacementAt, Principal, Protocol, RequestCtx, SensitivitySpec,
    SpiError, TenancySpi,
};
use osproxy_tenancy::{PlacementTable, TenancyRouter};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// The number of heap allocations `f` makes (dhat's profiler must be live).
fn allocs(f: impl FnOnce()) -> u64 {
    let before = HeapStats::get().total_blocks;
    f();
    HeapStats::get().total_blocks - before
}

/// A shared-index isolation tenancy: a body-field partition key, a
/// `{partition}:{body.id}` id rule, and an injected `_tenant` field, so the
/// resolved body transform is the heaviest (`Both { inject, id }`) — exactly the
/// per-item work the resolution cache must do once, not once per document.
struct SharedTenancy {
    table: Arc<PlacementTable>,
}

impl TenancySpi for SharedTenancy {
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        osproxy_tenancy::resolve_partition_spec(
            &PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            ctx,
            body,
        )
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

/// An `_bulk` body of `n` index ops, all for partition `acme` (one placement), so
/// the demux resolves it once and reuses it for every item.
fn bulk_body(n: usize) -> Vec<u8> {
    let mut body = Vec::new();
    for i in 0..n {
        body.extend_from_slice(b"{\"index\":{}}\n");
        body.extend_from_slice(
            format!("{{\"tenant_id\":\"acme\",\"id\":\"k{i}\",\"msg\":\"hello\"}}\n").as_bytes(),
        );
    }
    body
}

/// The allocations made while handling an `n`-document single-partition bulk
/// (the pipeline and body are built outside the measured region).
fn bulk_handle_allocs(rt: &tokio::runtime::Runtime, n: usize) -> u64 {
    let p = pipeline();
    let body = bulk_body(n);
    let principal = Principal::new(PrincipalId::from("svc"));
    let rid = RequestId::from("bulk");
    let headers = vec![];
    let ctx = RequestCtx::new(
        &principal,
        &rid,
        HttpMethod::Post,
        EndpointKind::IngestBulk,
        Protocol::Http1,
        "orders",
        HeaderView::new(&headers),
        &body,
    );
    allocs(|| {
        let resp = rt.block_on(p.handle(&ctx)).unwrap();
        assert_eq!(resp.status, 200, "bulk handled");
    })
}

#[test]
fn bulk_demux_marginal_allocation_budget() {
    // Skip under coverage instrumentation, which perturbs heap-allocation counts
    // (see the note in `osproxy-rewrite/tests/memory.rs`).
    if std::env::var_os("LLVM_PROFILE_FILE").is_some() {
        return;
    }
    let _profiler = Profiler::builder().testing().build();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // The marginal cost of one more document in the same partition: the difference
    // between a 1-doc and a 41-doc bulk, divided by the 40 extra documents. With
    // the resolution cached once, this covers only the genuine per-item work (id
    // map, body splice, response line, the write op), not a placement re-clone.
    let a1 = bulk_handle_allocs(&rt, 1);
    let a41 = bulk_handle_allocs(&rt, 41);
    let marginal = a41.saturating_sub(a1) / 40;
    eprintln!("BULK_ALLOC a1={a1} a41={a41} marginal_per_doc={marginal}");

    // Measured ~68 allocations per added document with the resolution cached once.
    // Before the cache it was ~77: each extra document also re-cloned the resolved
    // placement (partition + decision + body-transform strings) and re-collected
    // its inject vector. The bound sits between the two so that regression fails
    // here while the genuine per-item work (id map, body splice, response line,
    // write op) stays within budget.
    assert!(
        marginal <= 72,
        "bulk per-document allocation budget: {marginal} > 72 \
         (a per-item placement re-clone would push it back to ~77)"
    );
}
