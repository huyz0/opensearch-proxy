//! Resolving the full-fidelity traffic-capture config (`capture_*` keys).
//!
//! Split from [`super`] so the resolver list stays within the file-length budget;
//! capture is a self-contained cluster of broker + TLS + WAL settings. Reuses the
//! parent module's small value helpers (`opt`, `bool_or`, `u64_or`).

use super::{bool_or, opt, u64_or};
use crate::raw::Raw;
use crate::{CaptureConfig, CaptureTlsConfig, ConfigError};

/// Full-fidelity capture: requires both the brokers and the topic, or neither.
/// Set, the binary builds a Kafka producer and tees every exchange to it.
pub(super) fn capture(raw: &Raw) -> Result<Option<CaptureConfig>, ConfigError> {
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
                max_inflight: usize::try_from(u64_or(raw, "capture_max_inflight", 1024)?)
                    .map_err(|_| ConfigError::invalid("capture_max_inflight", "value too large"))?,
                max_attempts: u32::try_from(u64_or(raw, "capture_max_attempts", 4)?)
                    .map_err(|_| ConfigError::invalid("capture_max_attempts", "value too large"))?,
                backoff_ms: u64_or(raw, "capture_backoff_ms", 50)?,
                wal_dir: opt(raw, "capture_wal_dir"),
                wal_max_bytes: u64_or(raw, "capture_wal_max_bytes", 256 * 1024 * 1024)?,
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
