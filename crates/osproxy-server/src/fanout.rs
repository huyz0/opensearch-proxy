//! Async fan-out write queue (`docs/04` §9, ADR-010).
//!
//! Serializes a resolved `QueuedWrite` into the protobuf `OpEnvelope` and
//! produces it to Kafka with **broker-ack durability**, so the `202` the pipeline
//! returns is truthful: `KafkaWriteQueue::enqueue` resolves `Ok` only once every
//! op in the write is acknowledged.
//!
//! The wrapper is typed protobuf; the document body is opaque bytes in
//! `content_type` — **CBOR by default** (compact, OpenSearch-native), JSON when
//! configured for debuggability. The downstream applier forwards the body verbatim
//! with that Content-Type and never parses the document.

#[cfg(any(feature = "capture-kafka", test))]
use osproxy_engine::{QueueError, QueuedWrite};
#[cfg(any(feature = "capture-kafka", test))]
use osproxy_sink::{DocOp, WriteOp};

/// The generated protobuf messages (`osproxy.fanout.v1`).
#[cfg(any(feature = "capture-kafka", test))]
mod pb {
    #![allow(
        clippy::doc_markdown,
        clippy::large_enum_variant,
        clippy::trivially_copy_pass_by_ref,
        missing_docs,
        unreachable_pub
    )]
    include!(concat!(env!("OUT_DIR"), "/osproxy.fanout.v1.rs"));
}

#[cfg(any(feature = "capture-kafka", test))]
pub(crate) use pb::{OpEnvelope, OpType};

/// How the document body is encoded inside the envelope.
#[cfg(any(feature = "capture-kafka", test))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum BodyEncoding {
    /// CBOR (RFC 8949): compact binary, OpenSearch-native. The default.
    #[default]
    Cbor,
    /// Verbatim JSON: human-readable for debugging the queue.
    Json,
}

/// Transcodes a JSON document body to the configured encoding, returning the
/// bytes and the media type to stamp on the envelope.
#[cfg(any(feature = "capture-kafka", test))]
fn encode_body(json: &[u8], encoding: BodyEncoding) -> Result<(Vec<u8>, &'static str), QueueError> {
    match encoding {
        BodyEncoding::Json => Ok((json.to_vec(), "application/json")),
        BodyEncoding::Cbor => {
            // The body is already-transformed JSON; re-parse and emit CBOR. (The
            // engine has already validated/normalized it, so this cannot lose
            // fidelity beyond that parse.)
            let value: serde_json::Value =
                serde_json::from_slice(json).map_err(|_| QueueError {
                    reason: "fan-out body is not valid JSON",
                })?;
            let mut out = Vec::new();
            ciborium::into_writer(&value, &mut out).map_err(|_| QueueError {
                reason: "fan-out body CBOR encoding failed",
            })?;
            Ok((out, "application/cbor"))
        }
    }
}

/// Builds the protobuf envelope for one resolved op.
#[cfg(any(feature = "capture-kafka", test))]
pub(crate) fn envelope(
    write: &QueuedWrite,
    op: &WriteOp,
    encoding: BodyEncoding,
) -> Result<OpEnvelope, QueueError> {
    // Pull out the per-kind fields, then encode the body once (a delete has none).
    let (op_type, id, routing, body) = match &op.doc {
        DocOp::Index { id, routing, body } => {
            (OpType::Index, id.clone(), routing.clone(), Some(body))
        }
        DocOp::Create { id, routing, body } => {
            (OpType::Create, id.clone(), routing.clone(), Some(body))
        }
        DocOp::Update { id, routing, body } => (
            OpType::Update,
            Some(id.clone()),
            routing.clone(),
            Some(body),
        ),
        DocOp::Delete { id, routing } => (OpType::Delete, Some(id.clone()), routing.clone(), None),
    };
    let (body, content_type) = match body {
        Some(json) => {
            let (bytes, ct) = encode_body(json, encoding)?;
            (bytes, ct.to_owned())
        }
        None => (Vec::new(), String::new()),
    };
    Ok(OpEnvelope {
        op_id: write.op_id.clone(),
        partition: write.partition_key.clone(),
        cluster: op.target.cluster.as_str().to_owned(),
        index: op.target.index.as_str().to_owned(),
        epoch: op.epoch.get(),
        op_type: op_type as i32,
        id: id.unwrap_or_default(),
        routing: routing.unwrap_or_default(),
        content_type,
        body,
    })
}

/// A [`WriteQueue`](osproxy_engine::WriteQueue) that produces each resolved op as
/// an [`OpEnvelope`] to a Kafka topic, acknowledged before returning.
#[cfg(feature = "capture-kafka")]
pub(crate) struct KafkaWriteQueue<P> {
    producer: std::sync::Arc<P>,
    topic: String,
    encoding: BodyEncoding,
}

#[cfg(feature = "capture-kafka")]
impl<P> KafkaWriteQueue<P> {
    /// Builds a queue producing to `topic` with the given body `encoding`.
    pub(crate) fn new(producer: std::sync::Arc<P>, topic: String, encoding: BodyEncoding) -> Self {
        Self {
            producer,
            topic,
            encoding,
        }
    }
}

#[cfg(feature = "capture-kafka")]
impl<P: osproxy_kafka::AckProducer> osproxy_engine::WriteQueue for KafkaWriteQueue<P> {
    fn enabled(&self) -> bool {
        true
    }

    fn enqueue<'a>(
        &'a self,
        write: QueuedWrite,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), QueueError>> + Send + 'a>>
    {
        Box::pin(async move {
            use prost::Message;
            // Partition by the logical partition so all ops for one partition stay
            // ordered within a Kafka partition through the fan-out.
            let key = write.partition_key.clone().into_bytes();
            for op in write.batch.ops() {
                let payload = envelope(&write, op, self.encoding)?.encode_to_vec();
                self.producer
                    .send_acked(&self.topic, &key, &payload)
                    .await
                    .map_err(|_| QueueError {
                        reason: "fan-out enqueue was not acknowledged",
                    })?;
            }
            Ok(())
        })
    }
}

/// Binds the async fan-out write queue into `pipeline` when `cfg.fanout` is set:
/// connects an ack-producing krafka producer (over TLS/mTLS when configured),
/// wraps it in [`KafkaWriteQueue`], and sets the deployment-default write mode.
/// A fan-out without TLS is a plaintext broker connection.
#[cfg(feature = "capture-kafka")]
pub(crate) async fn attach<R, S>(
    pipeline: osproxy_engine::Pipeline<R, S>,
    cfg: &osproxy_config::Config,
) -> Result<osproxy_engine::Pipeline<R, S>, String>
where
    R: osproxy_tenancy::Router,
    S: osproxy_sink::Sink + osproxy_sink::Reader,
{
    use osproxy_engine::WriteMode;
    use osproxy_kafka_krafka::{AuthConfig, KrafkaProducer, TlsConfig as KafkaTlsConfig};

    let Some(fc) = &cfg.fanout else {
        return Ok(pipeline);
    };
    let auth = fc.tls.as_ref().map(|tls| {
        let mut t = KafkaTlsConfig::new()
            .with_ca_cert(&tls.ca_path)
            .with_kafka_alpn();
        if let (Some(cert), Some(key)) = (&tls.client_cert_path, &tls.client_key_path) {
            t = t.with_client_cert(cert, key);
        }
        AuthConfig::ssl(t)
    });
    let producer = KrafkaProducer::connect(fc.brokers.clone(), "osproxy-fanout", auth)
        .await
        .map_err(|e| format!("connecting fan-out producer: {}", e.reason))?;

    let encoding = match fc.body_encoding {
        osproxy_config::FanoutBodyEncoding::Cbor => BodyEncoding::Cbor,
        osproxy_config::FanoutBodyEncoding::Json => BodyEncoding::Json,
    };
    let mode = if fc.async_default {
        WriteMode::Async
    } else {
        WriteMode::Sync
    };
    println!(
        "osproxy fanout: on (topic={}, brokers={}, tls={}, body={:?}, default={:?})",
        fc.topic,
        fc.brokers.len(),
        fc.tls.is_some(),
        fc.body_encoding,
        mode,
    );
    let queue = KafkaWriteQueue::new(std::sync::Arc::new(producer), fc.topic.clone(), encoding);
    Ok(pipeline
        .with_write_queue(std::sync::Arc::new(queue))
        .with_baseline_write_mode(mode))
}

/// Without the `capture-kafka` feature no broker client is linked, so a
/// configured fan-out is a loud startup error rather than a silent no-op.
#[cfg(not(feature = "capture-kafka"))]
#[allow(clippy::unused_async)]
pub(crate) async fn attach<R, S>(
    pipeline: osproxy_engine::Pipeline<R, S>,
    cfg: &osproxy_config::Config,
) -> Result<osproxy_engine::Pipeline<R, S>, String>
where
    R: osproxy_tenancy::Router,
    S: osproxy_sink::Sink + osproxy_sink::Reader,
{
    if cfg.fanout.is_some() {
        return Err(
            "fan-out is configured (fanout_kafka_brokers/fanout_topic) but this binary \
                    was built without the `capture-kafka` feature; rebuild with \
                    `--features capture-kafka`"
                .to_owned(),
        );
    }
    Ok(pipeline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{ClusterId, Epoch, IndexName, Target};
    use osproxy_sink::WriteBatch;
    use prost::Message;

    fn write(doc: DocOp) -> QueuedWrite {
        let op = WriteOp::new(
            Target::new(ClusterId::from("eu-1"), IndexName::from("shared")),
            doc,
            Epoch::new(4),
        );
        QueuedWrite {
            op_id: "op-1".to_owned(),
            partition_key: "acme".to_owned(),
            batch: WriteBatch::single(op),
        }
    }

    #[test]
    fn cbor_envelope_round_trips_metadata_and_body() {
        let json = br#"{"_tenant":"acme","id":7,"msg":"hi"}"#;
        let w = write(DocOp::Index {
            id: Some("acme:7".to_owned()),
            routing: Some("acme".to_owned()),
            body: json.to_vec(),
        });

        let env = envelope(&w, &w.batch.ops()[0], BodyEncoding::Cbor).unwrap();
        let decoded = OpEnvelope::decode(env.encode_to_vec().as_slice()).unwrap();

        assert_eq!(decoded.op_id, "op-1");
        assert_eq!(decoded.partition, "acme");
        assert_eq!(decoded.cluster, "eu-1");
        assert_eq!(decoded.index, "shared");
        assert_eq!(decoded.epoch, 4);
        assert_eq!(decoded.op_type, OpType::Index as i32);
        assert_eq!(decoded.id, "acme:7");
        assert_eq!(decoded.routing, "acme");
        assert_eq!(decoded.content_type, "application/cbor");

        // The CBOR body decodes back to the original document.
        let value: serde_json::Value = ciborium::from_reader(decoded.body.as_slice()).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"_tenant":"acme","id":7,"msg":"hi"})
        );
    }

    #[test]
    fn json_encoding_keeps_the_body_verbatim() {
        let json = br#"{"id":1}"#;
        let w = write(DocOp::Index {
            id: None,
            routing: None,
            body: json.to_vec(),
        });
        let env = envelope(&w, &w.batch.ops()[0], BodyEncoding::Json).unwrap();
        assert_eq!(env.content_type, "application/json");
        assert_eq!(env.body, json.to_vec());
        assert_eq!(env.id, ""); // auto-assign
    }

    #[test]
    fn delete_envelope_carries_no_body() {
        let w = write(DocOp::Delete {
            id: "acme:7".to_owned(),
            routing: Some("acme".to_owned()),
        });
        let env = envelope(&w, &w.batch.ops()[0], BodyEncoding::Cbor).unwrap();
        assert_eq!(env.op_type, OpType::Delete as i32);
        assert_eq!(env.id, "acme:7");
        assert!(env.body.is_empty());
        assert_eq!(env.content_type, "");
    }
}
