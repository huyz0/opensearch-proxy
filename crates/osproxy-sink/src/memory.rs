//! An in-memory [`Sink`] (and [`Reader`]) for tests and dry-run routing.
//!
//! Records every batch it receives and acknowledges each operation as a
//! success, without any network. It also keeps the indexed documents by
//! `(index, id)` so it can serve get-by-id [`Reader`] requests — which lets the
//! full write→read round-trip be exercised in memory (the real `OpenSearchSink`
//! is covered by a testcontainer round-trip). Not for production: it persists
//! nothing.
//
// JUSTIFY(file-length): one cohesive in-memory double implementing the full
// `Sink` + `Reader` surface (write/get/search/search_stream/count) over a single
// shared store, plus its focused unit tests; splitting the trait impls from the
// store they share would scatter one small test fixture across files.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use bytes::Bytes;

use crate::ack::{OpResult, WriteAck};
use crate::batch::{DocOp, WriteBatch};
use crate::error::SinkError;
use crate::opensearch::buffered;
use crate::read::{
    CountOutcome, ReadOp, ReadOutcome, Reader, SearchOp, SearchOutcome, StreamingSearch,
};
use crate::sink::Sink;

/// A non-persistent [`Sink`]/[`Reader`] that records batches, stores indexed
/// documents, and acknowledges success.
#[derive(Debug, Default)]
pub struct MemorySink {
    recorded: Mutex<Vec<WriteBatch>>,
    /// Indexed documents keyed by `(physical index, physical id)`.
    docs: Mutex<HashMap<(String, String), Vec<u8>>>,
    /// Search operations received, in arrival order (for test assertions on the
    /// wrapped query the engine dispatched).
    searches: Mutex<Vec<SearchOp>>,
    auto_id: AtomicU64,
}

impl MemorySink {
    /// Creates an empty recording sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The batches recorded so far, in arrival order.
    ///
    /// Recovers a poisoned lock: the recording is inert data with no invariant
    /// a panicking writer could tear, and a test asserting on it must not itself
    /// panic on poisoning.
    #[must_use]
    pub fn recorded(&self) -> Vec<WriteBatch> {
        self.recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The search operations received so far, in arrival order. Recovers a
    /// poisoned lock for the same reason as [`MemorySink::recorded`].
    #[must_use]
    pub fn recorded_searches(&self) -> Vec<SearchOp> {
        self.searches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Builds the success ack for a batch, assigning ids to auto-id operations.
    fn ack_for(&self, batch: &WriteBatch) -> WriteAck {
        let results = batch
            .ops()
            .iter()
            .map(|op| match &op.doc {
                DocOp::Index { id, .. } | DocOp::Create { id, .. } => {
                    let id = id.clone().unwrap_or_else(|| self.next_auto_id());
                    OpResult::new(id, 201, true)
                }
                DocOp::Update { id, .. } | DocOp::Delete { id, .. } => {
                    OpResult::new(id.clone(), 200, false)
                }
            })
            .collect();
        WriteAck::new(results)
    }

    /// A deterministic id for an auto-id index op (`auto-1`, `auto-2`, …).
    fn next_auto_id(&self) -> String {
        let n = self.auto_id.fetch_add(1, Ordering::SeqCst) + 1;
        format!("auto-{n}")
    }

    /// Applies a batch to the document store: index/create store, update merges,
    /// delete removes. The ack supplies any auto-assigned id.
    fn store(&self, batch: &WriteBatch, ack: &WriteAck) {
        let mut docs = self
            .docs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (op, result) in batch.ops().iter().zip(ack.results()) {
            let index = op.target.index.as_str().to_owned();
            match &op.doc {
                DocOp::Index { body, .. } | DocOp::Create { body, .. } => {
                    docs.insert((index, result.id.clone()), body.clone());
                }
                DocOp::Update { id, body, .. } => {
                    let key = (index, id.clone());
                    let existing = docs
                        .get(&key)
                        .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok());
                    if let Some(bytes) =
                        apply_update(existing, body).and_then(|m| serde_json::to_vec(&m).ok())
                    {
                        docs.insert(key, bytes);
                    }
                }
                DocOp::Delete { id, .. } => {
                    docs.remove(&(index, id.clone()));
                }
            }
        }
    }
}

impl Sink for MemorySink {
    async fn write(&self, batch: WriteBatch) -> Result<WriteAck, SinkError> {
        let ack = self.ack_for(&batch);
        self.store(&batch, &ack);
        self.recorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(batch);
        Ok(ack)
    }
}

impl Reader for MemorySink {
    async fn get(&self, op: ReadOp) -> Result<ReadOutcome, SinkError> {
        let index = op.target.index.as_str().to_owned();
        let doc = self
            .docs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(index.clone(), op.id.clone()))
            .cloned();
        // Emulate the OpenSearch get-by-id envelope so the engine's response
        // shaping is identical against the memory sink and a real cluster.
        Ok(match doc {
            Some(body) => ReadOutcome::found(200, envelope(&index, &op.id, &body, true)),
            None => ReadOutcome::not_found(404, envelope(&index, &op.id, b"null", false)),
        })
    }

    async fn search(&self, op: SearchOp) -> Result<SearchOutcome, SinkError> {
        // A degenerate match-all: return every stored doc in the target index as
        // a hit. It does NOT evaluate the DSL (real filtering/isolation is proven
        // against a live cluster); it exists so the engine's query wrapping and
        // hit-stripping can be exercised, and it records the wrapped query.
        let index = op.target.index.as_str().to_owned();
        let hits: Vec<serde_json::Value> = self
            .docs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|((idx, _), _)| idx == &index)
            .map(|((idx, id), body)| {
                let source: serde_json::Value =
                    serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);
                serde_json::json!({ "_index": idx, "_id": id, "_source": source })
            })
            .collect();
        self.searches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(op);
        let body = serde_json::json!({
            "hits": { "total": { "value": hits.len() }, "hits": hits },
        });
        Ok(SearchOutcome::new(
            200,
            serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec()),
        ))
    }

    async fn search_stream(&self, op: SearchOp) -> Result<StreamingSearch, SinkError> {
        // Reuse the buffered match-all, then hand the bytes back as a (single-frame)
        // stream so the engine's streaming hit-transform wiring can be exercised in
        // memory (multi-frame resumability is covered by the scanner's fuzz test and
        // the live RSS test).
        let out = self.search(op).await?;
        Ok(StreamingSearch {
            status: out.status,
            body: buffered(Bytes::from(out.body)),
            pool_reuse: false,
        })
    }

    async fn count(&self, op: SearchOp) -> Result<CountOutcome, SinkError> {
        // Degenerate match-all count: every stored doc in the target index (the
        // DSL is not evaluated; real filtering is proven against a live cluster).
        let index = op.target.index.as_str().to_owned();
        let count = self
            .docs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .filter(|(idx, _)| idx == &index)
            .count();
        self.searches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(op);
        Ok(CountOutcome::new(200, count as u64))
    }
}

/// Applies an `_update` body: a partial `doc` is shallow-merged into the
/// existing source; when absent the `upsert` (or, with `doc_as_upsert`, the
/// `doc`) becomes the new source. `None` if nothing to write; scripts no-op.
fn apply_update(existing: Option<serde_json::Value>, body: &[u8]) -> Option<serde_json::Value> {
    let patch: serde_json::Value = serde_json::from_slice(body).unwrap_or(serde_json::Value::Null);
    let Some(mut source) = existing else {
        let doc_as_upsert = patch
            .get("doc_as_upsert")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        return patch
            .get("upsert")
            .or_else(|| doc_as_upsert.then(|| patch.get("doc")).flatten())
            .cloned();
    };
    if let (Some(target), Some(doc)) = (
        source.as_object_mut(),
        patch.get("doc").and_then(serde_json::Value::as_object),
    ) {
        for (k, v) in doc {
            target.insert(k.clone(), v.clone());
        }
    }
    Some(source)
}

/// Builds the OpenSearch get-by-id response envelope around a stored document.
fn envelope(index: &str, id: &str, source: &[u8], found: bool) -> Vec<u8> {
    let source: serde_json::Value =
        serde_json::from_slice(source).unwrap_or(serde_json::Value::Null);
    let doc = serde_json::json!({
        "_index": index,
        "_id": id,
        "found": found,
        "_source": source,
    });
    serde_json::to_vec(&doc).unwrap_or_else(|_| b"{\"found\":false}".to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::WriteOp;
    use osproxy_core::{ClusterId, Epoch, IndexName, Target};

    fn index_op(id: Option<&str>) -> WriteOp {
        WriteOp::new(
            Target::new(ClusterId::from("c"), IndexName::from("i")),
            DocOp::Index {
                id: id.map(str::to_owned),
                routing: None,
                body: b"{}".to_vec(),
            },
            Epoch::new(1),
        )
    }

    #[tokio::test]
    async fn auto_ids_are_deterministic_and_increment() {
        let sink = MemorySink::new();
        let ack = sink
            .write(WriteBatch::new().with(index_op(None)).with(index_op(None)))
            .await
            .unwrap();
        assert_eq!(ack.results()[0].id, "auto-1");
        assert_eq!(ack.results()[1].id, "auto-2");
    }

    #[tokio::test]
    async fn explicit_id_is_preserved() {
        let sink = MemorySink::new();
        let ack = sink
            .write(WriteBatch::single(index_op(Some("p:7"))))
            .await
            .unwrap();
        assert_eq!(ack.results()[0].id, "p:7");
    }

    fn target() -> Target {
        Target::new(ClusterId::from("c"), IndexName::from("i"))
    }

    #[tokio::test]
    async fn written_document_is_readable_by_id() {
        let sink = MemorySink::new();
        let op = WriteOp::new(
            target(),
            DocOp::Index {
                id: Some("acme:7".to_owned()),
                routing: Some("acme".to_owned()),
                body: br#"{"msg":"hi"}"#.to_vec(),
            },
            Epoch::new(1),
        );
        sink.write(WriteBatch::single(op)).await.unwrap();

        let hit = sink
            .get(ReadOp::new(target(), "acme:7", Some("acme".to_owned())))
            .await
            .unwrap();
        assert!(hit.found);
        // The body is the OpenSearch get-by-id envelope around the stored doc.
        let doc: serde_json::Value = serde_json::from_slice(&hit.body).unwrap();
        assert_eq!(doc["found"], true);
        assert_eq!(doc["_id"], "acme:7");
        assert_eq!(doc["_source"]["msg"], "hi");
    }

    #[tokio::test]
    async fn missing_document_is_a_not_found_outcome() {
        let sink = MemorySink::new();
        let miss = sink
            .get(ReadOp::new(target(), "absent", None))
            .await
            .unwrap();
        assert!(!miss.found);
        assert_eq!(miss.status, 404);
    }

    #[tokio::test]
    async fn search_returns_stored_docs_and_records_the_query() {
        let sink = MemorySink::new();
        sink.write(WriteBatch::single(WriteOp::new(
            target(),
            DocOp::Index {
                id: Some("acme:7".to_owned()),
                routing: None,
                body: br#"{"_tenant":"acme","msg":"hi"}"#.to_vec(),
            },
            Epoch::new(1),
        )))
        .await
        .unwrap();

        let wrapped = br#"{"query":{"bool":{"filter":[{"term":{"_tenant":"acme"}}]}}}"#.to_vec();
        let out = sink
            .search(SearchOp::new(target(), wrapped.clone()))
            .await
            .unwrap();
        assert_eq!(out.status, 200);
        let doc: serde_json::Value = serde_json::from_slice(&out.body).unwrap();
        assert_eq!(doc["hits"]["total"]["value"], 1);
        assert_eq!(doc["hits"]["hits"][0]["_source"]["msg"], "hi");
        // The wrapped query the engine dispatched was recorded for assertions.
        assert_eq!(sink.recorded_searches().len(), 1);
        assert_eq!(sink.recorded_searches()[0].body, wrapped);
    }

    #[tokio::test]
    async fn count_returns_the_number_of_stored_docs() {
        let sink = MemorySink::new();
        for id in ["acme:1", "acme:2"] {
            sink.write(WriteBatch::single(WriteOp::new(
                target(),
                DocOp::Index {
                    id: Some(id.to_owned()),
                    routing: None,
                    body: b"{}".to_vec(),
                },
                Epoch::new(1),
            )))
            .await
            .unwrap();
        }
        let out = sink
            .count(SearchOp::new(target(), b"{}".to_vec()))
            .await
            .unwrap();
        assert_eq!(out.status, 200);
        assert_eq!(out.count, 2);
    }

    #[tokio::test]
    async fn delete_removes_a_stored_document() {
        let sink = MemorySink::new();
        sink.write(WriteBatch::single(WriteOp::new(
            target(),
            DocOp::Index {
                id: Some("acme:7".to_owned()),
                routing: None,
                body: b"{}".to_vec(),
            },
            Epoch::new(1),
        )))
        .await
        .unwrap();
        sink.write(WriteBatch::single(WriteOp::new(
            target(),
            DocOp::Delete {
                id: "acme:7".to_owned(),
                routing: None,
            },
            Epoch::new(1),
        )))
        .await
        .unwrap();
        let miss = sink
            .get(ReadOp::new(target(), "acme:7", None))
            .await
            .unwrap();
        assert!(!miss.found);
    }
}
