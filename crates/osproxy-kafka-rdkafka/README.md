# osproxy-kafka-rdkafka

A librdkafka-backed `Producer` for [`osproxy-kafka`](../osproxy-kafka), turning the
captured traffic stream into Kafka records.

This crate is **not part of the workspace** (it is in the root `Cargo.toml`
`exclude` list). It links librdkafka, a heavy native library, so the proxy's
default build and CI never compile it. You opt in by building this crate directly.

## Build

```bash
cd crates/osproxy-kafka-rdkafka
cargo build
```

### System requirements

The `cmake-build` feature compiles librdkafka from source. You need:

- `cmake` and a C compiler (`cc`/`gcc`/`clang`)
- curl and OpenSSL development headers

On Debian/Ubuntu:

```bash
sudo apt-get install -y cmake build-essential libcurl4-openssl-dev libssl-dev
```

## Use

Compose it into the proxy's capture seam:

```rust
use osproxy_kafka::KafkaCapture;
use osproxy_kafka_rdkafka::RdKafkaProducer;
use osproxy_capture::RedactingCapture;

let producer = RdKafkaProducer::new("broker-1:9092,broker-2:9092")?;
let capture = RedactingCapture::new(KafkaCapture::new(producer, "osproxy.capture"));
let handler = app_handler.with_capture(Box::new(capture));
```

`RedactingCapture` drops the `Authorization` header before the record is produced.
The bodies stay full-fidelity for replay, so treat the topic as a privileged,
access-controlled, encrypted stream.
