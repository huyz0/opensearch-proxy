//! Validating the merged [`Raw`] layers into a typed [`Config`].
//!
//! Every fallible parse names its field in the returned [`ConfigError`], so a bad
//! value points an operator (or an LLM) straight at the setting to fix. Defaults
//! are applied here, once, so downstream code never re-derives them.

use std::net::SocketAddr;

use crate::raw::Raw;
use crate::{
    AdminPassthroughConfig, CaptureTlsConfig, Config, ConfigError, DiagBaseline, EtcdConfig,
    FanoutBodyEncoding, FanoutConfig, HeaderForwardingConfig, ObservabilityConfig,
    PassthroughConfig, TlsConfig,
};

mod resolve_capture;

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
        header_forwarding: HeaderForwardingConfig {
            enabled: bool_or(raw, "forward_client_headers", true)?,
            deny: csv(raw, "forward_header_deny"),
        },
        capture: resolve_capture::capture(raw)?,
        capture_default: bool_or(raw, "capture_default", false)?,
        fanout: fanout(raw)?,
        etcd: etcd(raw)?,
    })
}

/// etcd directive store: requires the endpoints, with a defaulted key. Unset
/// endpoints ⇒ `None` (use the in-memory store + admin publish).
fn etcd(raw: &Raw) -> Result<Option<EtcdConfig>, ConfigError> {
    let endpoints = csv(raw, "etcd_endpoints");
    if endpoints.is_empty() {
        if raw.get("etcd_directives_key").is_some() {
            return Err(ConfigError::invalid(
                "etcd_endpoints",
                "set etcd_endpoints when configuring an etcd directive store",
            ));
        }
        return Ok(None);
    }
    Ok(Some(EtcdConfig {
        endpoints,
        directives_key: string_or(raw, "etcd_directives_key", "osproxy/directives"),
    }))
}

/// Async fan-out queue: requires both the brokers and the topic, or neither.
fn fanout(raw: &Raw) -> Result<Option<FanoutConfig>, ConfigError> {
    match (opt(raw, "fanout_kafka_brokers"), opt(raw, "fanout_topic")) {
        (None, None) => {
            if raw.get("fanout_kafka_ca").is_some() {
                return Err(ConfigError::invalid(
                    "fanout_kafka_ca",
                    "set fanout_kafka_brokers and fanout_topic to enable fan-out",
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
                    "fanout_kafka_brokers",
                    "expected at least one `host:port` broker",
                ));
            }
            Ok(Some(FanoutConfig {
                brokers,
                topic,
                tls: fanout_tls(raw)?,
                body_encoding: fanout_body_encoding(raw)?,
                async_default: bool_or(raw, "fanout_async_default", false)?,
                expand_delete_by_query: bool_or(raw, "fanout_expand_delete_by_query", false)?,
            }))
        }
        _ => Err(ConfigError::invalid(
            "fanout_kafka_brokers",
            "set both fanout_kafka_brokers and fanout_topic, or neither",
        )),
    }
}

/// The fan-out body encoding: `cbor` (default) or `json`.
fn fanout_body_encoding(raw: &Raw) -> Result<FanoutBodyEncoding, ConfigError> {
    match raw.get("fanout_body_encoding").map(str::trim) {
        None => Ok(FanoutBodyEncoding::default()),
        Some(v) if v.eq_ignore_ascii_case("cbor") => Ok(FanoutBodyEncoding::Cbor),
        Some(v) if v.eq_ignore_ascii_case("json") => Ok(FanoutBodyEncoding::Json),
        Some(_) => Err(ConfigError::invalid(
            "fanout_body_encoding",
            "expected `cbor` or `json`",
        )),
    }
}

/// Fan-out broker TLS: enabled by a pinned CA; a client cert/key pair (both or
/// neither) adds mTLS and requires the CA. Mirrors capture broker TLS.
fn fanout_tls(raw: &Raw) -> Result<Option<CaptureTlsConfig>, ConfigError> {
    let client = match (
        opt(raw, "fanout_kafka_client_cert"),
        opt(raw, "fanout_kafka_client_key"),
    ) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some((cert, key)),
        _ => {
            return Err(ConfigError::invalid(
                "fanout_kafka_client_cert",
                "set both fanout_kafka_client_cert and fanout_kafka_client_key, or neither",
            ))
        }
    };
    let Some(ca_path) = opt(raw, "fanout_kafka_ca") else {
        if client.is_some() {
            return Err(ConfigError::invalid(
                "fanout_kafka_ca",
                "client-cert mTLS to the brokers requires fanout_kafka_ca",
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

/// Parses a required-positive `u64` setting, falling back to `default` when unset.
/// A non-numeric or zero value names the field rather than silently defaulting.
fn u64_or(raw: &Raw, key: &'static str, default: u64) -> Result<u64, ConfigError> {
    match raw.get(key) {
        None => Ok(default),
        Some(value) => match value.trim().parse::<u64>() {
            Ok(n) if n > 0 => Ok(n),
            _ => Err(ConfigError::invalid(key, "expected a positive integer")),
        },
    }
}

/// Tenant-agnostic passthrough: requires both the cluster and its endpoint, or
/// neither. With both set, requests matching `passthrough_indices` (a
/// comma-separated logical-index prefix list; empty ⇒ all requests) are forwarded
/// verbatim; the rest stay tenant-isolated.
fn passthrough(raw: &Raw) -> Result<Option<PassthroughConfig>, ConfigError> {
    match (
        opt(raw, "passthrough_cluster"),
        opt(raw, "passthrough_endpoint"),
    ) {
        (None, None) => Ok(None),
        (Some(cluster), Some(endpoint)) => Ok(Some(PassthroughConfig {
            cluster,
            endpoint,
            index_prefixes: csv(raw, "passthrough_indices"),
        })),
        _ => Err(ConfigError::invalid(
            "passthrough_cluster",
            "set both passthrough_cluster and passthrough_endpoint, or neither",
        )),
    }
}

/// A comma-separated list value, trimmed and empties dropped (`[]` when unset).
fn csv(raw: &Raw, key: &'static str) -> Vec<String> {
    raw.get(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
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
        log_diagnostic_captures: bool_or(raw, "log_diagnostic_captures", false)?,
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
