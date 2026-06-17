//! Fan-out write-mode config parsing (`docs/04` §9). Split from `load.rs` to keep
//! each test file within the length budget.

use osproxy_config::{Config, ConfigError, FanoutBodyEncoding};

/// Resolve from canonical pairs (the env-free path).
fn resolve(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
    Config::resolve_for_test(pairs)
}

#[test]
fn fanout_is_off_by_default() {
    assert!(resolve(&[]).unwrap().fanout.is_none());
}

#[test]
fn fanout_needs_both_brokers_and_topic() {
    let err = resolve(&[("fanout_kafka_brokers", "broker:9092")]).unwrap_err();
    assert_eq!(err.field(), "fanout_kafka_brokers");
}

#[test]
fn fanout_parses_brokers_and_defaults_to_cbor_sync_plaintext() {
    let fc = resolve(&[
        ("fanout_kafka_brokers", "b1:9092, b2:9092"),
        ("fanout_topic", "osproxy.fanout"),
    ])
    .unwrap()
    .fanout
    .expect("fan-out configured");
    assert_eq!(fc.brokers, vec!["b1:9092", "b2:9092"]);
    assert_eq!(fc.topic, "osproxy.fanout");
    assert_eq!(
        fc.body_encoding,
        FanoutBodyEncoding::Cbor,
        "CBOR is the default body encoding"
    );
    assert!(!fc.async_default, "sync is the default write mode");
    assert!(
        !fc.expand_delete_by_query,
        "delete-by-query expansion is opt-in"
    );
    assert!(
        fc.tls.is_none(),
        "no CA configured means a plaintext broker link"
    );
}

#[test]
fn fanout_json_encoding_and_async_default_opt_in() {
    let fc = resolve(&[
        ("fanout_kafka_brokers", "b:9092"),
        ("fanout_topic", "t"),
        ("fanout_body_encoding", "json"),
        ("fanout_async_default", "true"),
        ("fanout_expand_delete_by_query", "true"),
    ])
    .unwrap()
    .fanout
    .unwrap();
    assert_eq!(fc.body_encoding, FanoutBodyEncoding::Json);
    assert!(fc.async_default);
    assert!(fc.expand_delete_by_query);
}

#[test]
fn fanout_rejects_unknown_body_encoding() {
    let err = resolve(&[
        ("fanout_kafka_brokers", "b:9092"),
        ("fanout_topic", "t"),
        ("fanout_body_encoding", "msgpack"),
    ])
    .unwrap_err();
    assert_eq!(err.field(), "fanout_body_encoding");
}

#[test]
fn fanout_ca_enables_tls_and_client_cert_needs_key_and_ca() {
    let fc = resolve(&[
        ("fanout_kafka_brokers", "b:9092"),
        ("fanout_topic", "t"),
        ("fanout_kafka_ca", "/ca.pem"),
    ])
    .unwrap()
    .fanout
    .unwrap();
    assert_eq!(fc.tls.expect("CA means TLS").ca_path, "/ca.pem");

    // client cert without key is rejected.
    let err = resolve(&[
        ("fanout_kafka_brokers", "b:9092"),
        ("fanout_topic", "t"),
        ("fanout_kafka_ca", "/ca.pem"),
        ("fanout_kafka_client_cert", "/c.pem"),
    ])
    .unwrap_err();
    assert_eq!(err.field(), "fanout_kafka_client_cert");

    // client cert/key without a CA is rejected.
    let err = resolve(&[
        ("fanout_kafka_brokers", "b:9092"),
        ("fanout_topic", "t"),
        ("fanout_kafka_client_cert", "/c.pem"),
        ("fanout_kafka_client_key", "/k.pem"),
    ])
    .unwrap_err();
    assert_eq!(err.field(), "fanout_kafka_ca");
}

#[test]
fn fanout_tls_keys_without_brokers_are_rejected() {
    let err = resolve(&[("fanout_kafka_ca", "/ca.pem")]).unwrap_err();
    assert_eq!(err.field(), "fanout_kafka_ca");
}
