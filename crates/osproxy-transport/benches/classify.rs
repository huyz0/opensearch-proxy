//! Wall-clock micro-benchmark of endpoint classification (`cargo xtask bench-local`).
//!
//! `classify` runs on **every** ingress request — it splits the path and matches
//! the segment shape into an [`EndpointKind`]. A regression here taxes 100% of
//! traffic, so it is worth a calibration point even though each call is cheap.
//! Wall-clock numbers are host-specific; the deterministic gate is the dhat
//! allocation budget in `tests/memory.rs`. divan owns its timing loop, so no
//! banned `Instant::now` appears here.

use osproxy_spi::HttpMethod;
use osproxy_transport::classify;

fn main() {
    divan::main();
}

/// The dominant data path: single-doc ingest with an index, verb, and id.
#[divan::bench]
fn classify_ingest_by_id() -> osproxy_transport::Classified {
    classify(
        divan::black_box(HttpMethod::Put),
        divan::black_box("/orders/_doc/acme:1"),
    )
}

/// A search path (index + `_search`), the read-path counterpart.
#[divan::bench]
fn classify_search() -> osproxy_transport::Classified {
    classify(
        divan::black_box(HttpMethod::Post),
        divan::black_box("/orders/_search"),
    )
}

/// An admin pass-through path (`_cat`/`_cluster`/`_nodes`).
#[divan::bench]
fn classify_admin() -> osproxy_transport::Classified {
    classify(
        divan::black_box(HttpMethod::Get),
        divan::black_box("/_cat/indices"),
    )
}
