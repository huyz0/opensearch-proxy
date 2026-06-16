//! Deterministic allocation-count budgets for the rewrite hot paths (docs/12,
//! NFR-P3): the per-document transforms (id construction, field inject/strip,
//! query wrap), the per-document id frame mapping, and the per-request demux
//! parses (`_bulk`/`_mget`/`_msearch`). Allocation counts are exact for a fixed
//! input, so a change that adds a heap allocation to a path that runs on every
//! document or request fails CI. The dhat allocator is installed for this test
//! binary only.
//!
//! The budgets are baselines, not targets: a change that moves one is a
//! deliberate decision to review (update the number with the reason), not a
//! silent regression. The load-bearing one is `strip_fields == 0` — the
//! read-path field strip on every hit must not allocate.

#![allow(clippy::unwrap_used)]

use dhat::{HeapStats, Profiler};
use osproxy_core::FieldName;
use osproxy_rewrite::{
    construct_id, inject_fields, map_logical_to_physical, map_physical_to_logical, parse_bulk,
    parse_mget, parse_msearch, strip_fields, wrap_query,
};
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
/// only one may exist at a time (parallel `#[test]`s would collide). The groups
/// are helpers called within the one live profiler, not separate tests.
#[test]
fn rewrite_hot_path_allocation_budgets() {
    let _profiler = Profiler::builder().testing().build();
    document_transform_budgets();
    bulk_path_budgets();
    demux_parse_budgets();
}

/// Per-document write/read transforms.
fn document_transform_budgets() {
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

    // wrap_query: parse only the top level (sibling subtrees and the client query
    // stay as raw byte spans via RawValue), nest the query under the partition
    // filter, and re-serialize — the heaviest per-search transform. Down from 33
    // when the whole body was materialized into a Value tree; the remaining cost
    // is the top-level map, the constructed bool subtree, and the output buffer.
    let body = br#"{"query":{"match":{"msg":"hi"}}}"#;
    let filter = vec![(FieldName::from("_tenant"), Value::from("acme"))];
    assert_eq!(
        allocs(|| {
            let _ = std::hint::black_box(wrap_query(body, &filter).unwrap());
        }),
        12,
        "wrap_query allocation budget"
    );
}

/// Bulk-path budgets (the highest-throughput ingest path; one id mapping per
/// document, one parse per request).
fn bulk_path_budgets() {
    // map_logical_to_physical: frame the natural key into the physical id — runs
    // per document on bulk ingest. (prefix + suffix from id_frame, plus the
    // formatted physical id.)
    let l2p = allocs(|| {
        let _ = std::hint::black_box(
            map_logical_to_physical("{partition}:{body.id}", "acme", "7").unwrap(),
        );
    });

    // map_physical_to_logical: strip the frame back off — runs per hit on the
    // read/response path; only the recovered logical id is owned.
    let p2l = allocs(|| {
        let _ = std::hint::black_box(
            map_physical_to_logical("{partition}:{body.id}", "acme", "acme:7").unwrap(),
        );
    });

    // parse_bulk: parse a fixed two-operation NDJSON body (an index with a source
    // line and a delete) — the per-request bulk parse.
    let bulk_body =
        b"{\"index\":{\"_id\":\"1\"}}\n{\"msg\":\"hi\"}\n{\"delete\":{\"_id\":\"2\"}}\n";
    let bulk = allocs(|| {
        let _ = std::hint::black_box(parse_bulk(bulk_body).unwrap());
    });

    assert_eq!(
        (l2p, p2l, bulk),
        (2, 2, 16),
        "bulk-path allocation budgets (map_logical_to_physical, map_physical_to_logical, parse_bulk)"
    );
}

/// The other two demux parses, completing the `_bulk`/`_mget`/`_msearch` family.
fn demux_parse_budgets() {
    // One `_mget` doc and one `_msearch` header/body pair.
    let mget_body = b"{\"docs\":[{\"_index\":\"a\",\"_id\":\"1\"}]}";
    let mget = allocs(|| {
        let _ = std::hint::black_box(parse_mget(mget_body).unwrap());
    });
    let msearch_body = b"{\"index\":\"a\"}\n{\"query\":{\"match_all\":{}}}\n";
    let msearch = allocs(|| {
        let _ = std::hint::black_box(parse_msearch(msearch_body).unwrap());
    });
    assert_eq!(
        (mget, msearch),
        (11, 10),
        "demux-parse allocation budgets (parse_mget, parse_msearch)"
    );
}
