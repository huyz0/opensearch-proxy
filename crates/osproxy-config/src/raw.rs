//! The raw (pre-validation) configuration layers and their merge.
//!
//! A setting is identified by one canonical `snake_case` key (e.g. `bind`). The
//! same key drives all three sources: a config-file line `bind = ...`, the
//! environment variable `OSPROXY_BIND`, and the flag `--bind`. Each source
//! produces a [`Raw`] map of `key -> string value`; [`Raw::layered`] merges them
//! with the documented precedence **file < environment < flags** (`docs/01` §6).
//! Validation into typed values happens later, in [`crate::resolve`].

use std::collections::BTreeMap;

use crate::ConfigError;

/// Every recognized setting, by canonical `snake_case` key. A key not in this list
/// is rejected (in a file or a flag) so a typo fails fast rather than being
/// silently ignored, the same fail-closed stance as the directive publish path.
pub(crate) const KEYS: &[&str] = &[
    "bind",
    "grpc_bind",
    "upstream",
    "index",
    "tokens",
    "allow_cleartext_mutation",
    "tls_cert",
    "tls_key",
    "tls_client_ca",
    "log_requests",
    "otlp_endpoint",
    "service_name",
    "diag_baseline",
    "debug_directive_key",
    "directive_admin_token",
    "debug_endpoints",
    "log_diagnostic_captures",
    "admin_passthrough_cluster",
    "admin_passthrough_prefixes",
    "admin_passthrough_endpoint",
    "cursor_affinity_key",
    "passthrough_cluster",
    "passthrough_endpoint",
    "passthrough_indices",
    "forward_client_headers",
    "forward_header_deny",
    "capture_default",
    "capture_kafka_brokers",
    "capture_topic",
    "capture_redact",
    "capture_kafka_ca",
    "capture_kafka_client_cert",
    "capture_kafka_client_key",
    "capture_max_inflight",
    "capture_max_attempts",
    "capture_backoff_ms",
    "capture_wal_dir",
    "capture_wal_max_bytes",
    "fanout_kafka_brokers",
    "fanout_topic",
    "fanout_kafka_ca",
    "fanout_kafka_client_cert",
    "fanout_kafka_client_key",
    "fanout_body_encoding",
    "fanout_async_default",
    "fanout_expand_delete_by_query",
    "etcd_endpoints",
    "etcd_directives_key",
];

/// The environment variable name for a canonical key: `OSPROXY_` + the
/// upper-cased key (e.g. `bind` -> `OSPROXY_BIND`).
#[must_use]
pub(crate) fn env_name(key: &str) -> String {
    format!("OSPROXY_{}", key.to_ascii_uppercase())
}

/// Returns the canonical `&'static str` for `key` (`snake_case`), or `None` if it
/// is not a recognized setting.
fn canonical(key: &str) -> Option<&'static str> {
    KEYS.iter().copied().find(|k| *k == key)
}

/// Resolves a file key that may be written bare inside a `[section]`: tries
/// `{section}_{key}` first, then the key as-is (so a fully-qualified key still
/// works inside a section, and a key outside any section is unchanged).
fn canonical_in(section: Option<&str>, key: &str) -> Option<&'static str> {
    section
        .and_then(|s| canonical(&format!("{s}_{key}")))
        .or_else(|| canonical(key))
}

/// One source's worth of raw string values, keyed by canonical setting key.
#[derive(Clone, Debug, Default)]
pub(crate) struct Raw {
    values: BTreeMap<&'static str, String>,
}

impl Raw {
    /// The raw string value for `key`, if this source set it.
    #[must_use]
    pub(crate) fn get(&self, key: &'static str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// Records a value under its canonical key (empty values are dropped, so an
    /// empty env var or `key =` line reads the same as "unset").
    fn set(&mut self, key: &'static str, value: String) {
        if value.is_empty() {
            return;
        }
        self.values.insert(key, value);
    }

    /// Reads every recognized setting from the environment (`OSPROXY_<KEY>`).
    #[must_use]
    pub(crate) fn from_env() -> Self {
        let mut raw = Raw::default();
        for &key in KEYS {
            if let Ok(value) = std::env::var(env_name(key)) {
                raw.set(key, value);
            }
        }
        raw
    }

    /// Parses the line-based config file: `# comment` and blank lines are ignored;
    /// every other line is `key = value`, where `value` may be wrapped in matching
    /// single or double quotes. An unrecognized key fails closed.
    ///
    /// A `[section]` header is optional grouping sugar: inside one, a bare key is
    /// resolved as `{section}_{key}` first (e.g. `kafka_brokers` under `[capture]`
    /// → `capture_kafka_brokers`), falling back to the key as-is, so a
    /// fully-qualified key still works anywhere and the canonical key, hence the
    /// `OSPROXY_<KEY>` env var and `--key` flag, is unchanged. `[]` clears the
    /// section. A file with no headers behaves exactly as before.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] for a malformed line or an unknown setting key.
    pub(crate) fn from_file(text: &str) -> Result<Self, ConfigError> {
        let mut raw = Raw::default();
        let mut section: Option<String> = None;
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(name) = line.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
                let name = name.trim();
                section = (!name.is_empty()).then(|| name.to_owned());
                continue;
            }
            let (key, value) = line.split_once('=').ok_or_else(|| {
                ConfigError::invalid("file", format!("line {}: expected `key = value`", n + 1))
            })?;
            let key = key.trim();
            let canonical =
                canonical_in(section.as_deref(), key).ok_or_else(|| ConfigError::unknown(key))?;
            raw.set(canonical, unquote(value.trim()).to_owned());
        }
        Ok(raw)
    }

    /// Parses `--key value` / `--key=value` flags into raw values. The reserved
    /// `--config <path>` flag is consumed by the caller before this and must not
    /// appear here. An unrecognized flag fails closed.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] for an unknown flag or a value-less flag.
    pub(crate) fn from_flags<I: IntoIterator<Item = String>>(args: I) -> Result<Self, ConfigError> {
        let mut raw = Raw::default();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            let body = arg.strip_prefix("--").ok_or_else(|| {
                ConfigError::invalid("flags", format!("unexpected argument {arg:?}"))
            })?;
            let (name, value) = if let Some((name, value)) = body.split_once('=') {
                (name.to_owned(), value.to_owned())
            } else {
                let value = args.next().ok_or_else(|| {
                    ConfigError::invalid("flags", format!("--{body} needs a value"))
                })?;
                (body.to_owned(), value)
            };
            let key = name.replace('-', "_");
            let canonical = canonical(&key).ok_or_else(|| ConfigError::unknown(&name))?;
            raw.set(canonical, value);
        }
        Ok(raw)
    }

    /// Builds a [`Raw`] directly from canonical `(key, value)` pairs (test/doc
    /// support). An unknown key fails closed, like the file and flag sources.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] for an unrecognized key.
    pub(crate) fn from_pairs(pairs: &[(&str, &str)]) -> Result<Self, ConfigError> {
        let mut raw = Raw::default();
        for (key, value) in pairs {
            let canonical = canonical(key).ok_or_else(|| ConfigError::unknown(*key))?;
            raw.set(canonical, (*value).to_owned());
        }
        Ok(raw)
    }

    /// Merges the three sources with precedence **file < env < flags**: a later
    /// source overrides an earlier one for the same key (`docs/01` §6).
    #[must_use]
    pub(crate) fn layered(file: Raw, env: Raw, flags: Raw) -> Raw {
        let mut merged = file;
        merged.values.extend(env.values);
        merged.values.extend(flags.values);
        merged
    }
}

/// Strips one pair of matching surrounding single or double quotes, if present.
fn unquote(value: &str) -> &str {
    let bytes = value.as_bytes();
    if value.len() >= 2
        && (bytes[0] == b'"' || bytes[0] == b'\'')
        && bytes[bytes.len() - 1] == bytes[0]
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}
