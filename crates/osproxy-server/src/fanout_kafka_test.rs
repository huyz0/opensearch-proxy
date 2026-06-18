//! Live Kafka round-trip test for the async fan-out op envelope (`docs/04` §9).
//! Split from `fanout.rs` to stay within the file-length budget; shares that
//! module's `KafkaWriteQueue`/`envelope`/`OpEnvelope` via `use super::*`.
//!
//! `KafkaWriteQueue` produces an op envelope to a containerized Apache Kafka, and
//! a krafka consumer reads it back and decodes it — proving the
//! produce→broker→consume→decode contract the in-process tests cannot. Needs
//! Docker + the `capture-kafka` feature, so it is `#[ignore]`'d. Run with:
//!   cargo test -p osproxy-server --features capture-kafka --bin osproxy -- --ignored

// Test scaffolding: `expect`/`panic` are how the live-container tests fail fast
// on setup errors, as in `tests/testcontainer.rs`.
#![allow(clippy::expect_used, clippy::panic)]

use super::*;
use std::sync::Arc;
use std::time::Duration;

use krafka::consumer::Consumer;
use osproxy_core::{ClusterId, Epoch, IndexName, Target};
use osproxy_engine::WriteQueue;
use osproxy_kafka_krafka::KrafkaProducer;
use osproxy_sink::{DocOp, WriteBatch, WriteOp};
use prost::Message;
// The Apache image's start script rewrites the advertised listener to the
// dynamic host port; the default (Confluent) image advertises its internal
// port, which an external client cannot reach.
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a Docker daemon"]
async fn op_envelope_round_trips_through_a_real_broker() {
    let node = Kafka::default().start().await.expect("start kafka");
    let port = node
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("mapped kafka port");
    let brokers = vec![format!("127.0.0.1:{port}")];
    let topic = "osproxy.fanout.test";
    ensure_topic(&brokers, topic).await;

    // Produce one resolved op through the real queue (broker-ack durable).
    let producer = KrafkaProducer::connect(brokers.clone(), "osproxy-fanout-test", None)
        .await
        .expect("connect producer");
    let queue = KafkaWriteQueue::new(Arc::new(producer), topic.to_owned(), BodyEncoding::Cbor);
    let write = QueuedWrite {
        op_id: "op-1".to_owned(),
        partition_key: "acme".to_owned(),
        batch: WriteBatch::single(WriteOp::new(
            Target::new(ClusterId::from("eu-1"), IndexName::from("shared")),
            DocOp::Index {
                id: Some("acme:7".to_owned()),
                routing: Some("acme".to_owned()),
                body: br#"{"_tenant":"acme","id":7,"msg":"hi"}"#.to_vec(),
            },
            Epoch::new(4),
        )),
    };
    queue.enqueue(write).await.expect("enqueue acked");

    // Consume it back and decode the envelope + CBOR body.
    let record = read_first(&brokers, topic).await;
    assert_eq!(record.key.as_deref(), Some(b"acme".as_ref())); // keyed by partition
    let env = OpEnvelope::decode(record.value.expect("payload").as_ref()).expect("decode");
    assert_eq!(env.op_id, "op-1");
    assert_eq!(env.partition, "acme");
    assert_eq!(env.cluster, "eu-1");
    assert_eq!(env.index, "shared");
    assert_eq!(env.epoch, 4);
    assert_eq!(env.op_type, OpType::Index as i32);
    assert_eq!(env.id, "acme:7");
    assert_eq!(env.content_type, "application/cbor");
    let body: serde_json::Value =
        ciborium::from_reader(env.body.as_slice()).expect("decode cbor body");
    assert_eq!(
        body,
        serde_json::json!({"_tenant":"acme","id":7,"msg":"hi"})
    );
}

/// A bulk request and an expanded `_delete_by_query` both enqueue *multiple*
/// envelopes for one logical partition: bulk with a per-item `{batch}:{n}` op id,
/// delete-by-query with a `Delete` op (no body) per match. This proves that shape
/// survives a real broker — same-key ordering preserved, the bodyless delete
/// envelope decodes — which the single-op test above does not exercise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a Docker daemon"]
async fn multi_op_partition_round_trips_in_order() {
    let node = Kafka::default().start().await.expect("start kafka");
    let port = node
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("mapped kafka port");
    let brokers = vec![format!("127.0.0.1:{port}")];
    let topic = "osproxy.fanout.multi";
    ensure_topic(&brokers, topic).await;

    let producer = KrafkaProducer::connect(brokers.clone(), "osproxy-fanout-multi", None)
        .await
        .expect("connect producer");
    let queue = KafkaWriteQueue::new(Arc::new(producer), topic.to_owned(), BodyEncoding::Cbor);

    // An index item (op_id `batch:0`) then a delete item (op_id `batch:1`), as
    // the bulk demux / DBQ expansion enqueue them — each its own single-op write,
    // all keyed by the same partition so they stay ordered within a Kafka partition.
    let index = single(
        "batch:0",
        DocOp::Index {
            id: Some("acme:7".to_owned()),
            routing: Some("acme".to_owned()),
            body: br#"{"_tenant":"acme","id":7}"#.to_vec(),
        },
    );
    let delete = single(
        "batch:1",
        DocOp::Delete {
            id: "acme:8".to_owned(),
            routing: Some("acme".to_owned()),
        },
    );
    queue.enqueue(index).await.expect("enqueue index acked");
    queue.enqueue(delete).await.expect("enqueue delete acked");

    let records = read_n(&brokers, topic, 2).await;
    for r in &records {
        assert_eq!(r.key.as_deref(), Some(b"acme".as_ref())); // same partition key
    }
    let first = OpEnvelope::decode(records[0].value.as_deref().expect("payload")).expect("decode");
    assert_eq!(first.op_id, "batch:0");
    assert_eq!(first.op_type, OpType::Index as i32);
    assert_eq!(first.id, "acme:7");
    assert_eq!(first.content_type, "application/cbor");

    let second = OpEnvelope::decode(records[1].value.as_deref().expect("payload")).expect("decode");
    assert_eq!(second.op_id, "batch:1");
    assert_eq!(second.op_type, OpType::Delete as i32);
    assert_eq!(second.id, "acme:8");
    assert!(second.body.is_empty(), "a delete carries no body");
    assert_eq!(second.content_type, "");
}

/// One single-op write keyed by `acme`, as the bulk/DBQ paths enqueue each item.
fn single(op_id: &str, doc: DocOp) -> QueuedWrite {
    QueuedWrite {
        op_id: op_id.to_owned(),
        partition_key: "acme".to_owned(),
        batch: WriteBatch::single(WriteOp::new(
            Target::new(ClusterId::from("eu-1"), IndexName::from("shared")),
            doc,
            Epoch::new(4),
        )),
    }
}

/// Creates `topic` (1 partition, RF 1), retrying through the broker's
/// post-start warmup. krafka does not auto-create topics on produce.
async fn ensure_topic(brokers: &[String], topic: &str) {
    use krafka::admin::{AdminClient, NewTopic};
    let admin = AdminClient::builder()
        .bootstrap_servers(brokers.join(","))
        .build()
        .await
        .expect("admin client");
    for _ in 0..20 {
        let spec = NewTopic::new(topic, 1, 1).expect("topic spec");
        if admin
            .create_topics(vec![spec], Duration::from_secs(10), false)
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    panic!("topic could not be created within the warmup window");
}

/// Reads the first record on partition 0 of `topic`. Uses a manual partition
/// assignment + seek rather than a subscription, so the read does not depend on
/// the consumer-group coordinator (which can lag a freshly-started broker).
async fn read_first(brokers: &[String], topic: &str) -> krafka::consumer::ConsumerRecord {
    let consumer = Consumer::builder()
        .bootstrap_servers(brokers.join(","))
        .build()
        .await
        .expect("build consumer");
    consumer.assign(topic, vec![0]).await.expect("assign");
    consumer
        .seek_to_beginning(topic, 0)
        .await
        .expect("seek to beginning");
    read_n(brokers, topic, 1).await.pop().expect("a record")
}

/// Reads the first `n` records on partition 0 of `topic`, in offset order
/// (manual assign + seek, no consumer group — see [`read_first`]).
async fn read_n(
    brokers: &[String],
    topic: &str,
    n: usize,
) -> Vec<krafka::consumer::ConsumerRecord> {
    let consumer = Consumer::builder()
        .bootstrap_servers(brokers.join(","))
        .build()
        .await
        .expect("build consumer");
    consumer.assign(topic, vec![0]).await.expect("assign");
    consumer
        .seek_to_beginning(topic, 0)
        .await
        .expect("seek to beginning");
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let record = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("recv did not time out")
            .expect("a record");
        out.push(record);
    }
    out
}
