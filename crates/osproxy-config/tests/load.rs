//! Validation + layering tests for the config loader. These exercise the typed
//! resolution and the file→env→flags precedence without touching the process
//! environment (env layering is covered by `Config::load` in the binary; here we
//! drive the deterministic file/flag/pair paths).

use osproxy_config::{Config, ConfigError, DiagBaseline};

/// Resolve from canonical pairs (the env-free path).
fn resolve(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
    Config::resolve_for_test(pairs)
}

#[test]
fn defaults_apply_when_nothing_is_set() {
    let cfg = resolve(&[]).unwrap();
    assert_eq!(cfg.bind.to_string(), "127.0.0.1:8080");
    assert_eq!(cfg.upstream, "http://127.0.0.1:9200");
    assert_eq!(cfg.index, "osproxy-shared");
    assert!(cfg.grpc_bind.is_none());
    assert!(cfg.tokens.is_empty(), "empty token map = dev mode");
    assert!(cfg.tls.is_none(), "cleartext by default");
    assert!(cfg.admin_passthrough.is_none(), "admin rejected by default");
    assert!(cfg.cursor_affinity_key.is_none());
    assert_eq!(cfg.observability.diag_baseline, DiagBaseline::Shape);
    assert_eq!(cfg.observability.service_name, "osproxy");
    assert!(
        cfg.require_tls_for_mutation,
        "NFR-S1 enforced unless opted out"
    );
}

#[test]
fn a_bad_bind_address_names_the_field() {
    let err = resolve(&[("bind", "nope")]).unwrap_err();
    assert_eq!(err.field(), "bind");
    assert!(err.to_string().contains("bind"), "{err}");
}

#[test]
fn cleartext_opt_out_flips_the_mutation_guard() {
    let cfg = resolve(&[("allow_cleartext_mutation", "true")]).unwrap();
    assert!(!cfg.require_tls_for_mutation);
    assert!(resolve(&[("allow_cleartext_mutation", "maybe")]).is_err());
}

#[test]
fn tls_requires_both_cert_and_key() {
    assert_eq!(
        resolve(&[("tls_cert", "c.pem")]).unwrap_err().field(),
        "tls_cert"
    );
    let cfg = resolve(&[("tls_cert", "c.pem"), ("tls_key", "k.pem")]).unwrap();
    let tls = cfg.tls.expect("tls configured");
    assert_eq!(tls.cert_path, "c.pem");
    assert!(tls.client_ca_path.is_none());
}

#[test]
fn a_client_ca_without_cert_and_key_is_rejected() {
    assert_eq!(
        resolve(&[("tls_client_ca", "ca.pem")]).unwrap_err().field(),
        "tls_client_ca"
    );
    let cfg = resolve(&[
        ("tls_cert", "c.pem"),
        ("tls_key", "k.pem"),
        ("tls_client_ca", "ca.pem"),
    ])
    .unwrap();
    assert_eq!(cfg.tls.unwrap().client_ca_path.as_deref(), Some("ca.pem"));
}

#[test]
fn tokens_parse_and_reject_malformed_entries() {
    let cfg = resolve(&[("tokens", "s3cr3t=svc, t2 = other ")]).unwrap();
    assert_eq!(
        cfg.tokens,
        vec![
            ("s3cr3t".to_owned(), "svc".to_owned()),
            ("t2".to_owned(), "other".to_owned()),
        ]
    );
    assert_eq!(
        resolve(&[("tokens", "garbage")]).unwrap_err().field(),
        "tokens"
    );
}

#[test]
fn diag_baseline_parses_or_errors() {
    assert_eq!(
        resolve(&[("diag_baseline", "off")])
            .unwrap()
            .observability
            .diag_baseline,
        DiagBaseline::Off
    );
    assert_eq!(
        resolve(&[("diag_baseline", "loud")]).unwrap_err().field(),
        "diag_baseline"
    );
}

#[test]
fn admin_passthrough_defaults_its_prefixes() {
    let cfg = resolve(&[("admin_passthrough_cluster", "admin-1")]).unwrap();
    let admin = cfg.admin_passthrough.expect("policy built");
    assert_eq!(admin.cluster, "admin-1");
    assert_eq!(admin.prefixes, vec!["/_cat/", "/_cluster/", "/_nodes/"]);

    let custom = resolve(&[
        ("admin_passthrough_cluster", "admin-1"),
        (
            "admin_passthrough_prefixes",
            "/_cat/health, /_cluster/health",
        ),
    ])
    .unwrap()
    .admin_passthrough
    .unwrap();
    assert_eq!(custom.prefixes, vec!["/_cat/health", "/_cluster/health"]);
}

#[test]
fn an_unknown_key_fails_closed() {
    let err = resolve(&[("bnid", "x")]).unwrap_err();
    assert_eq!(err.field(), "bnid");
    assert!(err.to_string().contains("unknown"), "{err}");
}

#[test]
fn load_layers_a_file_under_env_under_flags() {
    // Write a config file setting bind + index, then override bind with a flag.
    let dir = std::env::temp_dir();
    let path = dir.join(format!("osproxy-cfg-{}.conf", std::process::id()));
    std::fs::write(
        &path,
        "# sample\nbind = \"127.0.0.1:7000\"\nindex = 'from-file'\n",
    )
    .unwrap();

    let args = vec![
        "--config".to_owned(),
        path.to_string_lossy().into_owned(),
        "--bind".to_owned(),
        "127.0.0.1:7777".to_owned(),
    ];
    let cfg = Config::load(args).unwrap();
    // Flag wins over the file for bind; the file still provides index.
    assert_eq!(cfg.bind.to_string(), "127.0.0.1:7777");
    assert_eq!(cfg.index, "from-file");

    std::fs::remove_file(&path).ok();
}

#[test]
fn an_unknown_flag_is_rejected() {
    let err = Config::load(vec!["--nope".to_owned(), "x".to_owned()]).unwrap_err();
    assert_eq!(err.field(), "nope");
}

#[test]
fn capture_is_off_by_default() {
    assert!(resolve(&[]).unwrap().capture.is_none());
}

#[test]
fn capture_needs_both_brokers_and_topic() {
    let err = resolve(&[("capture_kafka_brokers", "broker:9092")]).unwrap_err();
    assert_eq!(err.field(), "capture_kafka_brokers");
}

#[test]
fn capture_parses_brokers_and_defaults_to_redacting_plaintext() {
    let cap = resolve(&[
        ("capture_kafka_brokers", "b1:9092, b2:9092"),
        ("capture_topic", "osproxy.capture"),
    ])
    .unwrap()
    .capture
    .expect("capture configured");
    assert_eq!(cap.brokers, vec!["b1:9092", "b2:9092"]);
    assert_eq!(cap.topic, "osproxy.capture");
    assert!(
        cap.redact,
        "redaction defaults on for the privileged stream"
    );
    assert!(
        cap.tls.is_none(),
        "no CA configured means a plaintext broker link"
    );
}

#[test]
fn capture_ca_enables_tls_and_redact_opts_out() {
    let cap = resolve(&[
        ("capture_kafka_brokers", "b1:9092"),
        ("capture_topic", "t"),
        ("capture_kafka_ca", "/etc/osproxy/kafka-ca.pem"),
        ("capture_redact", "false"),
    ])
    .unwrap()
    .capture
    .unwrap();
    assert!(!cap.redact);
    let tls = cap.tls.expect("CA configured means TLS");
    assert_eq!(tls.ca_path, "/etc/osproxy/kafka-ca.pem");
    assert!(tls.client_cert_path.is_none());
}

#[test]
fn capture_client_cert_requires_its_key_and_a_ca() {
    // cert without key is rejected.
    let err = resolve(&[
        ("capture_kafka_brokers", "b:9092"),
        ("capture_topic", "t"),
        ("capture_kafka_ca", "/ca.pem"),
        ("capture_kafka_client_cert", "/c.pem"),
    ])
    .unwrap_err();
    assert_eq!(err.field(), "capture_kafka_client_cert");

    // client cert/key without a CA is rejected (mTLS needs server trust pinned).
    let err = resolve(&[
        ("capture_kafka_brokers", "b:9092"),
        ("capture_topic", "t"),
        ("capture_kafka_client_cert", "/c.pem"),
        ("capture_kafka_client_key", "/k.pem"),
    ])
    .unwrap_err();
    assert_eq!(err.field(), "capture_kafka_ca");
}

#[test]
fn capture_tls_keys_without_brokers_are_rejected() {
    let err = resolve(&[("capture_kafka_ca", "/ca.pem")]).unwrap_err();
    assert_eq!(err.field(), "capture_kafka_ca");
}

#[test]
fn capture_default_is_off_and_opts_in() {
    assert!(!resolve(&[]).unwrap().capture_default);
    assert!(
        resolve(&[("capture_default", "true")])
            .unwrap()
            .capture_default
    );
}

#[test]
fn capture_delivery_knobs_default_and_parse() {
    let cap = resolve(&[("capture_kafka_brokers", "b:9092"), ("capture_topic", "t")])
        .unwrap()
        .capture
        .unwrap();
    assert_eq!(
        (cap.max_inflight, cap.max_attempts, cap.backoff_ms),
        (1024, 4, 50)
    );

    let tuned = resolve(&[
        ("capture_kafka_brokers", "b:9092"),
        ("capture_topic", "t"),
        ("capture_max_inflight", "8192"),
        ("capture_max_attempts", "10"),
        ("capture_backoff_ms", "100"),
    ])
    .unwrap()
    .capture
    .unwrap();
    assert_eq!(
        (tuned.max_inflight, tuned.max_attempts, tuned.backoff_ms),
        (8192, 10, 100)
    );

    // Zero / non-numeric is rejected, naming the field.
    let err = resolve(&[
        ("capture_kafka_brokers", "b:9092"),
        ("capture_topic", "t"),
        ("capture_max_attempts", "0"),
    ])
    .unwrap_err();
    assert_eq!(err.field(), "capture_max_attempts");
}

#[test]
fn capture_wal_is_off_by_default_and_opts_into_durable() {
    let plain = resolve(&[("capture_kafka_brokers", "b:9092"), ("capture_topic", "t")])
        .unwrap()
        .capture
        .unwrap();
    assert!(plain.wal_dir.is_none(), "in-memory best-effort by default");
    assert_eq!(plain.wal_max_bytes, 256 * 1024 * 1024);

    let durable = resolve(&[
        ("capture_kafka_brokers", "b:9092"),
        ("capture_topic", "t"),
        ("capture_wal_dir", "/var/lib/osproxy/capture"),
        ("capture_wal_max_bytes", "1048576"),
    ])
    .unwrap()
    .capture
    .unwrap();
    assert_eq!(durable.wal_dir.as_deref(), Some("/var/lib/osproxy/capture"));
    assert_eq!(durable.wal_max_bytes, 1_048_576);
}
