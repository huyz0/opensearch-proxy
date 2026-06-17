//! Validating the merged [`Raw`] layers into a typed [`Config`].
//!
//! Every fallible parse names its field in the returned [`ConfigError`], so a bad
//! value points an operator (or an LLM) straight at the setting to fix. Defaults
//! are applied here, once, so downstream code never re-derives them.

use std::net::SocketAddr;

use crate::raw::Raw;
use crate::{
    AdminPassthroughConfig, CaptureConfig, CaptureTlsConfig, Config, ConfigError, DiagBaseline,
    ObservabilityConfig, TlsConfig,
};

/// The default admin pass-through allow-list when a cluster is configured but no
/// explicit prefixes are given.
const DEFAULT_ADMIN_PREFIXES: &str = "/_cat/,/_cluster/,/_nodes/";

/// Validates the merged raw layers into a [`Config`], or the first error found.
pub(crate) fn resolve(raw: &Raw) -> Result<Config, ConfigError> {
    Ok(Config {
        bind: socket_addr(raw, "bind", "127.0.0.1:8080")?,
        grpc_bind: optional_socket_addr(raw, "grpc_bind")?,
        upstream: string_or(raw, "upstream", "http://127.0.0.1:9200"),
        index: string_or(raw, "index", "osproxy-shared"),
        tokens: tokens(raw)?,
        require_tls_for_mutation: !bool_or(raw, "allow_cleartext_mutation", false)?,
        tls: tls(raw)?,
        observability: observability(raw)?,
        admin_passthrough: admin_passthrough(raw),
        cursor_affinity_key: opt(raw, "cursor_affinity_key"),
        passthrough: passthrough(raw)?,
        capture: capture(raw)?,
    })
}

/// Full-fidelity capture: requires both the brokers and the topic, or neither.
/// Set, the binary builds a Kafka producer and tees every exchange to it.
fn capture(raw: &Raw) -> Result<Option<CaptureConfig>, ConfigError> {
    match (opt(raw, "capture_kafka_brokers"), opt(raw, "capture_topic")) {
        (None, None) => {
            // Guard: TLS-only capture keys without brokers/topic is a misconfig.
            if raw.get("capture_kafka_ca").is_some() {
                return Err(ConfigError::invalid(
                    "capture_kafka_ca",
                    "set capture_kafka_brokers and capture_topic to enable capture",
                ));
            }
            Ok(None)
        }
        (Some(brokers), Some(topic)) => {
            let brokers: Vec<String> = brokers
                .split(',')
                .map(str::trim)
                .filter(|b| !b.is_empty())
                .map(str::to_owned)
                .collect();
            if brokers.is_empty() {
                return Err(ConfigError::invalid(
                    "capture_kafka_brokers",
                    "expected at least one `host:port` broker",
                ));
            }
            Ok(Some(CaptureConfig {
                brokers,
                topic,
                redact: bool_or(raw, "capture_redact", true)?,
                tls: capture_tls(raw)?,
            }))
        }
        _ => Err(ConfigError::invalid(
            "capture_kafka_brokers",
            "set both capture_kafka_brokers and capture_topic, or neither",
        )),
    }
}

/// Capture broker TLS: enabled by the presence of a pinned CA. A client cert/key
/// pair (both-or-neither) adds mTLS and requires the CA.
fn capture_tls(raw: &Raw) -> Result<Option<CaptureTlsConfig>, ConfigError> {
    let client = match (
        opt(raw, "capture_kafka_client_cert"),
        opt(raw, "capture_kafka_client_key"),
    ) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some((cert, key)),
        _ => {
            return Err(ConfigError::invalid(
                "capture_kafka_client_cert",
                "set both capture_kafka_client_cert and capture_kafka_client_key, or neither",
            ))
        }
    };
    let Some(ca_path) = opt(raw, "capture_kafka_ca") else {
        if client.is_some() {
            return Err(ConfigError::invalid(
                "capture_kafka_ca",
                "client-cert mTLS to the brokers requires capture_kafka_ca",
            ));
        }
        return Ok(None);
    };
    let (client_cert_path, client_key_path) = match client {
        Some((cert, key)) => (Some(cert), Some(key)),
        None => (None, None),
    };
    Ok(Some(CaptureTlsConfig {
        ca_path,
        client_cert_path,
        client_key_path,
    }))
}

/// Transparent passthrough: requires both the cluster and its endpoint, or
/// neither. Set, both, the proxy forwards every request verbatim there.
fn passthrough(raw: &Raw) -> Result<Option<(String, String)>, ConfigError> {
    match (
        opt(raw, "passthrough_cluster"),
        opt(raw, "passthrough_endpoint"),
    ) {
        (None, None) => Ok(None),
        (Some(cluster), Some(endpoint)) => Ok(Some((cluster, endpoint))),
        _ => Err(ConfigError::invalid(
            "passthrough_cluster",
            "set both passthrough_cluster and passthrough_endpoint, or neither",
        )),
    }
}

/// An optional string value (`None` when unset/empty).
fn opt(raw: &Raw, key: &'static str) -> Option<String> {
    raw.get(key).map(str::to_owned)
}

/// A string value, falling back to `default` when unset.
fn string_or(raw: &Raw, key: &'static str, default: &str) -> String {
    raw.get(key).unwrap_or(default).to_owned()
}

/// Parses a required socket address, defaulting when unset.
fn socket_addr(raw: &Raw, key: &'static str, default: &str) -> Result<SocketAddr, ConfigError> {
    raw.get(key)
        .unwrap_or(default)
        .parse()
        .map_err(|_| ConfigError::invalid(key, "expected a `host:port` socket address"))
}

/// Parses an optional socket address (`None` when unset).
fn optional_socket_addr(raw: &Raw, key: &'static str) -> Result<Option<SocketAddr>, ConfigError> {
    match raw.get(key) {
        Some(value) => value
            .parse()
            .map(Some)
            .map_err(|_| ConfigError::invalid(key, "expected a `host:port` socket address")),
        None => Ok(None),
    }
}

/// Parses a boolean (`1`/`true`/`yes` or `0`/`false`/`no`, case-insensitive),
/// falling back to `default` when unset.
fn bool_or(raw: &Raw, key: &'static str, default: bool) -> Result<bool, ConfigError> {
    match raw.get(key) {
        None => Ok(default),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => Err(ConfigError::invalid(
                key,
                "expected a boolean (1/true/yes or 0/false/no)",
            )),
        },
    }
}

/// Parses the `token=principal,...` auth map. A non-empty entry without `=`, or
/// with an empty side, is a typed error rather than silently dropped.
fn tokens(raw: &Raw) -> Result<Vec<(String, String)>, ConfigError> {
    let Some(spec) = raw.get("tokens") else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (token, principal) = entry.split_once('=').ok_or_else(|| {
            ConfigError::invalid(
                "tokens",
                format!("entry {entry:?} is not `token=principal`"),
            )
        })?;
        let (token, principal) = (token.trim(), principal.trim());
        if token.is_empty() || principal.is_empty() {
            return Err(ConfigError::invalid(
                "tokens",
                format!("entry {entry:?} has an empty token or principal"),
            ));
        }
        out.push((token.to_owned(), principal.to_owned()));
    }
    Ok(out)
}

/// Validates the TLS settings: cert and key are both-or-neither, and a client CA
/// (mTLS) is only meaningful alongside them.
fn tls(raw: &Raw) -> Result<Option<TlsConfig>, ConfigError> {
    match (opt(raw, "tls_cert"), opt(raw, "tls_key")) {
        (None, None) => {
            if raw.get("tls_client_ca").is_some() {
                return Err(ConfigError::invalid(
                    "tls_client_ca",
                    "set tls_cert and tls_key before configuring a client CA (mTLS)",
                ));
            }
            Ok(None)
        }
        (Some(cert_path), Some(key_path)) => Ok(Some(TlsConfig {
            cert_path,
            key_path,
            client_ca_path: opt(raw, "tls_client_ca"),
        })),
        _ => Err(ConfigError::invalid(
            "tls_cert",
            "set both tls_cert and tls_key, or neither",
        )),
    }
}

/// Resolves the observability + control-plane settings.
fn observability(raw: &Raw) -> Result<ObservabilityConfig, ConfigError> {
    Ok(ObservabilityConfig {
        log_requests: bool_or(raw, "log_requests", false)?,
        otlp_endpoint: opt(raw, "otlp_endpoint"),
        service_name: string_or(raw, "service_name", "osproxy"),
        diag_baseline: diag_baseline(raw)?,
        debug_directive_key: opt(raw, "debug_directive_key"),
        directive_admin_token: opt(raw, "directive_admin_token"),
        debug_endpoints: bool_or(raw, "debug_endpoints", true)?,
    })
}

/// Parses the diagnostics baseline level, defaulting to `Shape`.
fn diag_baseline(raw: &Raw) -> Result<DiagBaseline, ConfigError> {
    match raw.get("diag_baseline") {
        None => Ok(DiagBaseline::Shape),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "off" => Ok(DiagBaseline::Off),
            "shape" => Ok(DiagBaseline::Shape),
            "shape-timing" => Ok(DiagBaseline::ShapeTiming),
            "shape-rewrite-diff" => Ok(DiagBaseline::ShapeRewriteDiff),
            _ => Err(ConfigError::invalid(
                "diag_baseline",
                "expected off|shape|shape-timing|shape-rewrite-diff",
            )),
        },
    }
}

/// Builds the admin pass-through policy when a target cluster is configured.
fn admin_passthrough(raw: &Raw) -> Option<AdminPassthroughConfig> {
    let cluster = raw.get("admin_passthrough_cluster")?.to_owned();
    let prefixes = raw
        .get("admin_passthrough_prefixes")
        .unwrap_or(DEFAULT_ADMIN_PREFIXES)
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect();
    Some(AdminPassthroughConfig {
        cluster,
        prefixes,
        endpoint: opt(raw, "admin_passthrough_endpoint"),
    })
}
