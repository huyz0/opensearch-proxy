//! Typed configuration.
//!
//! Loads and fully validates configuration (file → environment → flags) before
//! any socket opens, producing validated value objects the other crates consume
//! (`docs/01` §6). Invalid config fails fast with a typed, actionable
//! [`ConfigError`] naming the bad field. It contains no business logic — it only
//! turns strings into validated values; mapping those to domain types (the
//! crypto provider, the pipeline) is the binary's job. Hot-reloadable state
//! (directives, placement) goes through `osproxy-control`, not here.
//!
//! # Example
//!
//! ```
//! use osproxy_config::Config;
//! // Defaults apply when nothing is set; a bad value is a typed error.
//! let cfg = Config::resolve_for_test(&[("bind", "0.0.0.0:9000")]).unwrap();
//! assert_eq!(cfg.bind.port(), 9000);
//! assert!(cfg.require_tls_for_mutation, "enforced by default (NFR-S1)");
//! assert!(Config::resolve_for_test(&[("bind", "not-an-addr")]).is_err());
//! ```
#![deny(missing_docs)]

mod raw;
mod resolve;

use std::net::SocketAddr;

use raw::Raw;

/// The fully validated configuration the binary serves from. Every field is a
/// ready-to-use value object; no further parsing or fallbacks happen downstream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// The HTTP ingress bind address.
    pub bind: SocketAddr,
    /// The optional gRPC ingress bind address (off when `None`).
    pub grpc_bind: Option<SocketAddr>,
    /// The upstream OpenSearch base URL for the single configured cluster.
    pub upstream: String,
    /// The shared physical index the reference tenancy writes into.
    pub index: String,
    /// The `token -> principal` auth map; empty means permissive dev mode.
    pub tokens: Vec<(String, String)>,
    /// Whether a body-mutating request is refused over cleartext (NFR-S1). True
    /// (enforce) unless `allow_cleartext_mutation` opts out.
    pub require_tls_for_mutation: bool,
    /// TLS termination settings, or `None` for cleartext ingress.
    pub tls: Option<TlsConfig>,
    /// Observability + control-plane settings.
    pub observability: ObservabilityConfig,
    /// Admin (`_cat`/`_cluster`/`_nodes`) pass-through policy, or `None` to reject.
    pub admin_passthrough: Option<AdminPassthroughConfig>,
    /// The shared HMAC key enabling scroll/PIT cursor affinity, or `None` (off).
    pub cursor_affinity_key: Option<String>,
}

/// TLS termination settings: PEM file **paths** (the binary reads them — config
/// stays free of certificate material). mTLS is required when `client_ca_path`
/// is set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsConfig {
    /// Path to the server certificate chain PEM.
    pub cert_path: String,
    /// Path to the server private key PEM.
    pub key_path: String,
    /// Path to the client-CA PEM that client certs must chain to (enables mTLS).
    pub client_ca_path: Option<String>,
}

/// Observability and control-plane channel settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservabilityConfig {
    /// Whether to emit a structured JSON log line per request.
    pub log_requests: bool,
    /// The OTLP collector base URL for span export, or `None` (export off).
    pub otlp_endpoint: Option<String>,
    /// The `service.name` reported on exported spans.
    pub service_name: String,
    /// The baseline diagnostics verbosity applied before any directive.
    pub diag_baseline: DiagBaseline,
    /// The shared HMAC key verifying signed `X-Debug-Directive` headers, or `None`.
    pub debug_directive_key: Option<String>,
    /// The bearer token gating `POST/GET /admin/directives`, or `None` (disabled).
    pub directive_admin_token: Option<String>,
}

/// The admin pass-through policy: the cluster that answers admin requests and the
/// allow-listed path prefixes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminPassthroughConfig {
    /// The cluster id admin requests are forwarded to.
    pub cluster: String,
    /// The allow-listed path prefixes (e.g. `/_cat/`).
    pub prefixes: Vec<String>,
}

/// The baseline diagnostics verbosity. A config-local enum so this crate stays
/// independent of `osproxy-observe`; the binary maps it to the engine's level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DiagBaseline {
    /// Export nothing until a directive selects a request.
    Off,
    /// Shapes/ids/field-names only (the default).
    #[default]
    Shape,
    /// `Shape` plus per-stage timing.
    ShapeTiming,
    /// `Shape` plus the rewrite diff shape.
    ShapeRewriteDiff,
}

impl DiagBaseline {
    /// The canonical wire/config string for this level.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shape => "shape",
            Self::ShapeTiming => "shape-timing",
            Self::ShapeRewriteDiff => "shape-rewrite-diff",
        }
    }
}

/// A configuration failure: which setting was bad and why. `Display` is a single
/// actionable line for both an operator and an LLM (`docs/01` §6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError {
    field: String,
    reason: String,
}

impl ConfigError {
    /// An invalid value for a known `field`.
    #[must_use]
    pub fn invalid(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// An unrecognized setting key (typo / unsupported option).
    #[must_use]
    pub fn unknown(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: "unknown setting".to_owned(),
        }
    }

    /// The offending setting's name.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "config: `{}`: {}", self.field, self.reason)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Loads and validates configuration from the process environment plus
    /// `args` (CLI flags without the program name). A `--config <path>` flag — or
    /// the `OSPROXY_CONFIG` env var — names a config file read as the lowest layer.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] if a file/flag is malformed, a key is unknown, or
    /// any value fails validation — before any socket is opened.
    pub fn load<I: IntoIterator<Item = String>>(args: I) -> Result<Self, ConfigError> {
        let (file_flag, flags) = extract_config_flag(args)?;
        let file_path = file_flag.or_else(|| {
            std::env::var("OSPROXY_CONFIG")
                .ok()
                .filter(|v| !v.is_empty())
        });
        let file = match &file_path {
            Some(path) => {
                let text = std::fs::read_to_string(path)
                    .map_err(|e| ConfigError::invalid("config", format!("reading {path}: {e}")))?;
                Raw::from_file(&text)?
            }
            None => Raw::default(),
        };
        let raw = Raw::layered(file, Raw::from_env(), Raw::from_flags(flags)?);
        resolve::resolve(&raw)
    }

    /// Test-only: resolve a [`Config`] directly from an in-memory `(key, value)`
    /// list (canonical keys), skipping the file/env/flag layering. Lets tests and
    /// doc examples exercise validation without touching the process environment.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] if any value fails validation.
    pub fn resolve_for_test(pairs: &[(&str, &str)]) -> Result<Self, ConfigError> {
        resolve::resolve(&Raw::from_pairs(pairs)?)
    }
}

/// Splits a reserved `--config <path>` / `--config=<path>` flag out of the
/// argument list, returning the path (if any) and the remaining flags.
fn extract_config_flag<I: IntoIterator<Item = String>>(
    args: I,
) -> Result<(Option<String>, Vec<String>), ConfigError> {
    let mut file = None;
    let mut rest = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--config" {
            file = Some(
                args.next()
                    .ok_or_else(|| ConfigError::invalid("config", "--config needs a path"))?,
            );
        } else if let Some(path) = arg.strip_prefix("--config=") {
            file = Some(path.to_owned());
        } else {
            rest.push(arg);
        }
    }
    Ok((file, rest))
}
