//! Deterministic allocation-count budgets for the per-document rewrite hot path
//! (docs/12, NFR-P3). Allocation counts are exact for a fixed input, so a change
//! that adds a heap allocation to a transform that runs on every document fails
//! CI. The dhat allocator is installed for this test binary only.
//!
//! The budgets are baselines, not targets: a change that moves one is a
//! deliberate decision to review (update the number with the reason), not a
//! silent regression. The load-bearing one is `strip_fields == 0` — the
//! read-path field strip on every hit must not allocate.

#![allow(clippy::unwrap_used)]

use dhat::{HeapStats, Profiler};
use osproxy_core::FieldName;
use osproxy_rewrite::{construct_id, inject_fields, strip_fields, wrap_query};
use serde_json::{json, Value};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// The number of heap allocations `f` makes (dhat's profiler must be live).
fn allocs(f: impl FnOnce()) -> u64 {
    let before = HeapStats::get().total_blocks;
    f();
    HeapStats::get().total_blocks - before
}

/// All rewrite-path budgets in one test: dhat's profiler is process-global, so
/// only one may exist at a time (parallel `#[test]`s would collide).
#[test]
fn rewrite_hot_path_allocation_budgets() {
    let _profiler = Profiler::builder().testing().build();

    // construct_id: one expansion of `{partition}:{body.id}` into an owned id.
    let doc = json!({ "id": 7, "msg": "hi" });
    assert_eq!(
        allocs(|| {
            let _ =
                std::hint::black_box(construct_id("{partition}:{body.id}", "acme", &doc).unwrap());
        }),
        4,
        "construct_id allocation budget"
    );

    // inject_fields: stamp one tenancy field into a document object (the inserted
    // key string + the cloned value).
    let mut target = json!({ "msg": "hi" });
    let fields = vec![(FieldName::from("_tenant"), Value::from("acme"))];
    assert_eq!(
        allocs(|| {
            inject_fields(&mut target, &fields).unwrap();
        }),
        2,
        "inject_fields allocation budget"
    );

    // strip_fields: the read-path inverse — removing a key must NOT allocate.
    let mut hit = json!({ "_tenant": "acme", "msg": "hi" });
    let names = vec![FieldName::from("_tenant")];
    assert_eq!(
        allocs(|| {
            let _ = std::hint::black_box(strip_fields(&mut hit, &names));
        }),
        0,
        "strip_fields must not allocate (NFR-P3)"
    );

    // wrap_query: parse the client query, nest it under the partition filter, and
    // re-serialize — the heaviest per-search transform; a baseline to watch.
    let body = br#"{"query":{"match":{"msg":"hi"}}}"#;
    let filter = vec![(FieldName::from("_tenant"), Value::from("acme"))];
    assert_eq!(
        allocs(|| {
            let _ = std::hint::black_box(wrap_query(body, &filter).unwrap());
        }),
        33,
        "wrap_query allocation budget"
    );
}
