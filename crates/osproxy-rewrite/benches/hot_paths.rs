//! Wall-clock micro-benchmarks of the rewrite hot paths (`cargo xtask bench`).
//!
//! These measure the per-request CPU cost of the body/query transforms and the
//! demux parses — the proxy's *added compute* over a direct-to-cluster call
//! (a component of NFR-P1/P7). Wall-clock numbers are host-specific and noisy,
//! so this is a **local calibration tool, not a CI gate**: the deterministic
//! gates are the dhat allocation budgets (`tests/memory.rs`) and, in CI where
//! valgrind exists, iai-callgrind instruction counts. divan owns its timing
//! loop, so no banned `Instant::now` appears here.

#![allow(clippy::unwrap_used)]

use osproxy_core::FieldName;
use osproxy_rewrite::{
    construct_id, inject_fields, map_logical_to_physical, map_physical_to_logical, parse_bulk,
    parse_mget, parse_msearch, strip_fields, wrap_query,
};
use serde_json::{json, Value};

fn main() {
    divan::main();
}

/// `construct_id`: expand `{partition}:{body.id}` into an owned physical id.
#[divan::bench]
fn bench_construct_id(bencher: divan::Bencher) {
    let doc = json!({ "id": 7, "msg": "hi" });
    bencher.bench_local(|| construct_id(divan::black_box("{partition}:{body.id}"), "acme", &doc));
}

/// `inject_fields`: stamp one tenancy field into a document object.
#[divan::bench]
fn bench_inject_fields(bencher: divan::Bencher) {
    let fields = vec![(FieldName::from("_tenant"), Value::from("acme"))];
    bencher
        .with_inputs(|| json!({ "msg": "hi" }))
        .bench_local_values(|mut doc| {
            let _ = inject_fields(&mut doc, divan::black_box(&fields));
            doc
        });
}

/// `strip_fields`: the read-path inverse — remove the tenancy key from a hit.
#[divan::bench]
fn bench_strip_fields(bencher: divan::Bencher) {
    let names = vec![FieldName::from("_tenant")];
    bencher
        .with_inputs(|| json!({ "_tenant": "acme", "msg": "hi" }))
        .bench_local_values(|mut hit| {
            strip_fields(&mut hit, divan::black_box(&names));
            hit
        });
}

/// `wrap_query`: nest a client query under the mandatory partition filter and
/// re-serialize — the heaviest per-search transform.
#[divan::bench]
fn bench_wrap_query(bencher: divan::Bencher) {
    let body = br#"{"query":{"match":{"msg":"hi"}}}"#;
    let filter = vec![(FieldName::from("_tenant"), Value::from("acme"))];
    bencher.bench_local(|| wrap_query(divan::black_box(body), &filter));
}

/// `map_logical_to_physical`: frame the natural key into the physical id.
#[divan::bench]
fn bench_map_logical_to_physical(bencher: divan::Bencher) {
    bencher.bench_local(|| {
        map_logical_to_physical(divan::black_box("{partition}:{body.id}"), "acme", "7")
    });
}

/// `map_physical_to_logical`: strip the frame back off on the response path.
#[divan::bench]
fn bench_map_physical_to_logical(bencher: divan::Bencher) {
    bencher.bench_local(|| {
        map_physical_to_logical(divan::black_box("{partition}:{body.id}"), "acme", "acme:7")
    });
}

/// `parse_bulk`: parse a two-operation NDJSON body (index + delete).
#[divan::bench]
fn bench_parse_bulk(bencher: divan::Bencher) {
    let body = b"{\"index\":{\"_id\":\"1\"}}\n{\"msg\":\"hi\"}\n{\"delete\":{\"_id\":\"2\"}}\n";
    bencher.bench_local(|| parse_bulk(divan::black_box(body)));
}

/// `parse_mget`: parse one `_mget` `docs` entry.
#[divan::bench]
fn bench_parse_mget(bencher: divan::Bencher) {
    let body = b"{\"docs\":[{\"_index\":\"a\",\"_id\":\"1\"}]}";
    bencher.bench_local(|| parse_mget(divan::black_box(body)));
}

/// `parse_msearch`: parse one `_msearch` header/body pair.
#[divan::bench]
fn bench_parse_msearch(bencher: divan::Bencher) {
    let body = b"{\"index\":\"a\"}\n{\"query\":{\"match_all\":{}}}\n";
    bencher.bench_local(|| parse_msearch(divan::black_box(body)));
}
