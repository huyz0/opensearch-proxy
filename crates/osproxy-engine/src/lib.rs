//! Pipeline orchestration.
//!
//! Drives a request through the stages, authenticate, authorize, classify,
//! resolve, transform, dispatch, reverse-transform, egress (`docs/04` §1),
//! wiring the other crates together through `osproxy-core` types and
//! `osproxy-spi` traits. It owns no low-level wire or parsing detail.
//!
//! M1 lands the write-path core: [`build_write_batch`] turns a resolved routing
//! decision plus the request body into the epoch-stamped
//! [`WriteBatch`](osproxy_sink::WriteBatch) the sink delivers. M2 adds the
//! get-by-id read path: the [`Pipeline`] maps a client's logical id to the
//! physical id, fetches it through the [`Reader`](osproxy_sink::Reader) seam,
//! and strips the injected tenancy fields so the client sees its logical
//! document, the write→read round-trip symmetry the model rests on.
#![deny(missing_docs)]

mod admin;
mod asyncwrite;
mod bulk;
mod bulkline;
mod bulkprep;
mod cursor;
mod dbq;
mod endpoints;
mod error;
mod mget;
mod msearch;
mod observe;
mod passthrough;
mod pipeline;
mod pit;
mod plan;
mod read;
mod retry;
mod search_scan;
mod search_stream;

pub use admin::AdminPolicy;
pub use asyncwrite::{
    op_id_for, unsupported_async, valid_op_id, NoQueue, QueueError, QueuedWrite, WriteMode,
    WriteQueue,
};
pub use error::RequestError;
pub use passthrough::PassthroughPolicy;
pub use pipeline::{Pipeline, PipelineResponse};
pub use plan::build_write_batch;
pub use retry::RetryPolicy;
pub use search_stream::StreamSearch;

/// Internal entry points exposed **only** for benchmarks (`benches/`), which are
/// separate crates and so cannot reach `pub(crate)` items. Hidden from docs and
/// not part of the public API, do not depend on it.
#[doc(hidden)]
pub mod bench_support {
    use osproxy_core::FieldName;
    use osproxy_spi::{DocIdRule, IdTemplate};

    use crate::read::{shape_hits, ReadShape};
    use crate::search_scan::{HitShaper, SearchHitsScanner};

    /// The shared-index read shape the reference tenancy produces: strip
    /// `_tenant`, invert the `{partition}:{body.id}` id rule, drop `_routing`.
    fn shape() -> ReadShape {
        ReadShape {
            inject_names: vec![FieldName::from("_tenant")],
            id_rule: Some(
                DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true),
            ),
        }
    }

    /// Builds a realistic `_search` response: `n_hits` shared-index hits (each
    /// carrying the injected `_tenant`) plus an `aggregations` blob of `agg_bytes`,
    /// the sibling the proxy forwards verbatim past the hits.
    #[must_use]
    pub fn response(n_hits: usize, agg_bytes: usize) -> Vec<u8> {
        let mut s = String::from(r#"{"took":5,"hits":{"total":{"value":"#);
        s.push_str(&n_hits.to_string());
        s.push_str(r#"},"hits":["#);
        for i in 0..n_hits {
            if i > 0 {
                s.push(',');
            }
            s.push_str(r#"{"_index":"shared","_id":"acme:"#);
            s.push_str(&i.to_string());
            s.push_str(r#"","_routing":"acme","_source":{"_tenant":"acme","msg":"record number "#);
            s.push_str(&i.to_string());
            s.push_str(r#""}}"#);
        }
        s.push_str(r#"]},"aggregations":{"blob":""#);
        s.push_str(&"x".repeat(agg_bytes));
        s.push_str(r#""}}"#);
        s.into_bytes()
    }

    /// The buffered transform (`shape_hits`): parse the top level, materialize
    /// and strip the `hits` subtree, reserialize.
    #[must_use]
    pub fn buffered(body: &[u8]) -> Vec<u8> {
        shape_hits(body, "orders", "acme", &shape()).unwrap_or_default()
    }

    /// The streaming transform: feed the whole body through the resumable scanner
    /// in one chunk (the per-byte work the live pipeline does incrementally as
    /// upstream frames arrive).
    #[must_use]
    pub fn streaming(body: &[u8]) -> Vec<u8> {
        let shaper = HitShaper {
            logical_index: "orders".to_owned(),
            partition: "acme".to_owned(),
            shape: shape(),
        };
        let mut scanner = SearchHitsScanner::new(shaper);
        let mut out = scanner.feed(body);
        out.extend(scanner.finish());
        out
    }
}
