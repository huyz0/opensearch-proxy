//! The `osproxy` binary.
//!
//! Owns process lifecycle and wires the crates together (`docs/01` §3): it
//! builds a concrete tenancy + sink, drives them through the engine pipeline,
//! and serves that over the HTTP/1.1 ingress. It holds no business logic of its
//! own — the tenancy here is a minimal *reference* implementation showing how a
//! library consumer wires the SPI.
//!
//! M1 serves single-document ingest over HTTP/1.1, cleartext or TLS
//! (`docs/11`); mTLS and the FIPS provider attach in later slices.

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use osproxy_config::{
    AdminPassthroughConfig, Config, DiagBaseline, ObservabilityConfig, TlsConfig,
};
use osproxy_core::{ClusterId, IndexName, SystemClock};
use osproxy_engine::{AdminPolicy, Pipeline};
use osproxy_observe::{DiagLevel, InMemoryDirectiveStore};
use osproxy_otlp::OtlpHttpExporter;
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::cursor::HmacCursorSigner;
use osproxy_server::directive::HmacDirectiveVerifier;
use osproxy_server::handler::AppHandler;
use osproxy_server::log::{NoLog, RequestLog, StdoutJsonLog};
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::{OpenSearchSink, Reader, Sink};
use osproxy_spi::TenancySpi;
use osproxy_tenancy::TenancyRouter;
use osproxy_transport::{DefaultCryptoProvider, IngressHandler};
use tokio::net::TcpListener;

/// Entry point. Returns a process exit code rather than panicking, consistent
/// with the no-panic reliability requirement (NFR-R1).
#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("osproxy: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Loads and validates configuration (file → env → flags), builds the pipeline,
/// and serves until interrupted.
async fn run() -> Result<(), String> {
    // Load + fully validate config (file → env → flags) before any socket opens;
    // an invalid value is a typed error naming the field (`docs/01` §6).
    let cfg = Config::load(std::env::args().skip(1)).map_err(|e| e.to_string())?;
    let cluster = ClusterId::from("default");

    let mut endpoints = HashMap::new();
    endpoints.insert(cluster.clone(), cfg.upstream.clone());
    let sink = OpenSearchSink::new(endpoints);

    let tenancy = ReferenceTenancy::new(cluster, IndexName::from(cfg.index.as_str()));
    // The fleet directive store the pipeline reads and the admin endpoint writes.
    let directive_store = Arc::new(InMemoryDirectiveStore::new());
    let pipeline = assemble_pipeline(tenancy, sink, directive_store.clone(), &cfg);

    let tokens: HashMap<String, String> = cfg.tokens.iter().cloned().collect();
    let auth_mode = if tokens.is_empty() {
        "dev (open)"
    } else {
        "token"
    };
    let handler = Arc::new(with_directive_admin(
        AppHandler::new(pipeline, ReferenceAuthenticator::new(tokens))
            .with_request_log(request_log(cfg.observability.log_requests))
            .with_require_tls_for_mutation(cfg.require_tls_for_mutation),
        directive_store,
        cfg.observability.directive_admin_token.as_deref(),
    ));

    let listener = TcpListener::bind(cfg.bind)
        .await
        .map_err(|e| format!("binding {}: {e}", cfg.bind))?;

    // TLS when cert + key paths are configured; cleartext otherwise. The same
    // provider terminates the HTTP and gRPC listeners.
    let provider = load_tls_provider(cfg.tls.as_ref())?.map(Arc::new);

    // Optional gRPC ingress on its own listener, driving the same handler
    // (same pipeline, tenancy, and observability) as the HTTP front door.
    if let Some(grpc_bind) = cfg.grpc_bind {
        let grpc_listener = TcpListener::bind(grpc_bind)
            .await
            .map_err(|e| format!("binding gRPC {grpc_bind}: {e}"))?;
        spawn_grpc(
            grpc_listener,
            provider.clone(),
            Arc::clone(&handler),
            &grpc_bind.to_string(),
        );
    }

    let (bind, upstream, index) = (cfg.bind, &cfg.upstream, &cfg.index);
    if let Some(provider) = provider {
        println!(
            "osproxy listening on https://{bind}, upstream {upstream}, shared index {index}, auth {auth_mode}"
        );
        osproxy_transport::serve_tls_with_shutdown(listener, provider, handler, shutdown_signal())
            .await
            .map_err(|e| format!("serving: {e}"))
    } else {
        println!(
            "osproxy listening on http://{bind}, upstream {upstream}, shared index {index}, auth {auth_mode}"
        );
        osproxy_transport::serve_with_shutdown(listener, handler, shutdown_signal())
            .await
            .map_err(|e| format!("serving: {e}"))
    }
}

/// Spawns the gRPC ingress on its own listener, over TLS when a `provider` is
/// configured (matching the HTTP listener) and cleartext otherwise.
fn spawn_grpc<H: IngressHandler>(
    listener: TcpListener,
    provider: Option<Arc<DefaultCryptoProvider>>,
    handler: Arc<H>,
    grpc_bind: &str,
) {
    if let Some(provider) = provider {
        println!("osproxy gRPC listening on grpcs://{grpc_bind}");
        tokio::spawn(async move {
            if let Err(e) = osproxy_transport::serve_grpc_tls(listener, provider, handler).await {
                eprintln!("osproxy: gRPC serve error: {e}");
            }
        });
    } else {
        println!("osproxy gRPC listening on grpc://{grpc_bind}");
        tokio::spawn(async move {
            if let Err(e) = osproxy_transport::serve_grpc(listener, handler).await {
                eprintln!("osproxy: gRPC serve error: {e}");
            }
        });
    }
}

/// Resolves on the first Ctrl-C (`SIGINT`). The transport takes this as the
/// signal to stop accepting and drain in-flight requests (NFR-R5) before the
/// serve future returns. A failed signal registration resolves immediately
/// (shut down rather than ignore the operator's intent).
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    println!("osproxy: shutdown signal received — draining in-flight requests");
}

/// The structured per-request logger: stdout JSON lines (each the shape-only
/// explain document, carrying `trace_id`) when `OSPROXY_LOG_REQUESTS` is set,
/// off otherwise. Correlates with the OTLP traces/spans by `trace_id`.
fn request_log(enabled: bool) -> Box<dyn RequestLog> {
    if enabled {
        println!("osproxy structured request logging: on (stdout JSON)");
        Box::new(StdoutJsonLog)
    } else {
        Box::new(NoLog)
    }
}

/// Wires OTLP span export onto the pipeline when `OSPROXY_OTLP_ENDPOINT` is set
/// (the collector base URL, e.g. `http://otel-collector:4318`); otherwise export
/// stays off (no telemetry cost). `OSPROXY_SERVICE_NAME` sets the reported
/// `service.name` (default `osproxy`).
fn with_otlp_export<T: TenancySpi, S: Sink + Reader>(
    pipeline: Pipeline<T, S>,
    obs: &ObservabilityConfig,
) -> Pipeline<T, S> {
    let Some(endpoint) = obs.otlp_endpoint.as_deref() else {
        return pipeline;
    };
    let service = obs.service_name.clone();
    println!("osproxy OTLP span export -> {endpoint}/v1/traces (service={service})");
    pipeline
        .with_exporter(Arc::new(OtlpHttpExporter::new(endpoint)))
        .with_service_name(service)
}

/// Sets the pipeline's baseline diagnostics level from the validated config
/// (`diag_baseline`, default `shape`). Set it to `off` so nothing is exported
/// until a directive — fleet store or signed `X-Debug-Directive` header — selects
/// a request. The value was already validated at load, so this cannot fail.
fn with_diag_baseline<T: TenancySpi, S: Sink + Reader>(
    pipeline: Pipeline<T, S>,
    baseline: DiagBaseline,
) -> Pipeline<T, S> {
    let level = match baseline {
        DiagBaseline::Off => DiagLevel::Off,
        DiagBaseline::Shape => DiagLevel::Shape,
        DiagBaseline::ShapeTiming => DiagLevel::ShapeTiming,
        DiagBaseline::ShapeRewriteDiff => DiagLevel::ShapeRewriteDiff,
    };
    println!("osproxy diagnostics baseline: {}", baseline.as_str());
    pipeline.with_baseline_level(level)
}

/// Wires the signed `X-Debug-Directive` header channel when
/// `OSPROXY_DEBUG_DIRECTIVE_KEY` holds the shared HMAC secret; otherwise the
/// pipeline keeps rejecting every such header (the default `NoVerifier`). The MAC
/// runs on the build's validated crypto module. Pair with a baseline of `Off` for
/// a deployment where verbose diagnostics are off until an operator-signed token
/// turns them on for a single request.
fn with_debug_directive<T: TenancySpi, S: Sink + Reader>(
    pipeline: Pipeline<T, S>,
    key: Option<&str>,
) -> Pipeline<T, S> {
    let Some(key) = key else {
        return pipeline;
    };
    println!("osproxy X-Debug-Directive header channel: on (HMAC-verified)");
    let verifier = HmacDirectiveVerifier::new(key.as_bytes(), Arc::new(SystemClock));
    pipeline.with_directive_verifier(Arc::new(verifier))
}

/// Assembles the engine pipeline the binary serves: the concrete tenancy + sink
/// wrapped with the config-gated observability and affinity layers (OTLP export,
/// diagnostics baseline, signed debug-directive header, fleet directive store,
/// cursor affinity). Each layer is off unless its setting is configured.
fn assemble_pipeline(
    tenancy: ReferenceTenancy,
    sink: OpenSearchSink,
    directive_store: Arc<InMemoryDirectiveStore>,
    cfg: &Config,
) -> Pipeline<ReferenceTenancy, OpenSearchSink> {
    let base = Pipeline::new(TenancyRouter::new(tenancy), sink);
    let observed = with_debug_directive(
        with_diag_baseline(
            with_otlp_export(base, &cfg.observability),
            cfg.observability.diag_baseline,
        ),
        cfg.observability.debug_directive_key.as_deref(),
    )
    .with_directive_store(directive_store);
    with_admin_passthrough(
        with_cursor_affinity(observed, cfg.cursor_affinity_key.as_deref()),
        cfg.admin_passthrough.as_ref(),
    )
}

/// Enables opt-in admin (`_cat`/`_cluster`/`_nodes`) pass-through when
/// `OSPROXY_ADMIN_PASSTHROUGH_CLUSTER` names the cluster that answers admin
/// requests; `OSPROXY_ADMIN_PASSTHROUGH_PREFIXES` is a comma-separated allow-list
/// of path prefixes (default `/_cat/,/_cluster/,/_nodes/`). Unset ⇒ admin
/// requests are rejected (the default; `docs/decisions/006`).
fn with_admin_passthrough<T: TenancySpi, S: Sink + Reader>(
    pipeline: Pipeline<T, S>,
    admin: Option<&AdminPassthroughConfig>,
) -> Pipeline<T, S> {
    let Some(admin) = admin else {
        return pipeline;
    };
    println!(
        "osproxy admin pass-through: on (cluster={}, prefixes={:?})",
        admin.cluster, admin.prefixes
    );
    pipeline.with_admin_passthrough(AdminPolicy::new(
        ClusterId::from(admin.cluster.as_str()),
        admin.prefixes.clone(),
    ))
}

/// Enables opt-in scroll/PIT cursor affinity when `OSPROXY_CURSOR_AFFINITY_KEY`
/// is set: the proxy signs the cluster-in-cursor envelope with that shared HMAC
/// key, so a continued scroll routes to its pinned cluster across the fleet with
/// no shared store (`docs/03` §6). The **same key must be set on every instance**.
/// Unset ⇒ affinity off and cursor requests fail closed (`CursorUnresolvable`).
fn with_cursor_affinity<T: TenancySpi, S: Sink + Reader>(
    pipeline: Pipeline<T, S>,
    key: Option<&str>,
) -> Pipeline<T, S> {
    let Some(key) = key else {
        return pipeline;
    };
    println!("osproxy scroll/PIT cursor affinity: on (HMAC-signed envelope)");
    pipeline.with_cursor_signer(Arc::new(HmacCursorSigner::new(key.as_bytes())))
}

/// Enables the `POST /admin/directives` channel when
/// `OSPROXY_DIRECTIVE_ADMIN_TOKEN` is set (the shared bearer token an operator
/// presents to publish a fleet directive set into `store`); otherwise the
/// endpoint stays disabled (reports `not_enabled`).
fn with_directive_admin<A: osproxy_spi::Authenticator>(
    handler: AppHandler<A>,
    store: Arc<InMemoryDirectiveStore>,
    token: Option<&str>,
) -> AppHandler<A> {
    let Some(token) = token else {
        return handler;
    };
    println!("osproxy fleet directive admin: on (POST /admin/directives)");
    handler.with_directive_admin(store, token.to_owned(), Arc::new(SystemClock))
}

/// Builds a TLS provider from `OSPROXY_TLS_CERT`/`OSPROXY_TLS_KEY` (PEM file
/// paths). Returns `None` if neither is set (cleartext), or an error if one is
/// set without the other or the files cannot be read/parsed. If
/// `OSPROXY_TLS_CLIENT_CA` is also set, mutual TLS is required and clients must
/// present a certificate chaining to that CA.
fn load_tls_provider(tls: Option<&TlsConfig>) -> Result<Option<DefaultCryptoProvider>, String> {
    let Some(tls) = tls else {
        return Ok(None);
    };
    let cert_pem =
        std::fs::read(&tls.cert_path).map_err(|e| format!("reading {}: {e}", tls.cert_path))?;
    let key_pem =
        std::fs::read(&tls.key_path).map_err(|e| format!("reading {}: {e}", tls.key_path))?;

    let provider = match &tls.client_ca_path {
        Some(ca) => {
            let ca_pem = std::fs::read(ca).map_err(|e| format!("reading {ca}: {e}"))?;
            DefaultCryptoProvider::from_pem_mtls(&cert_pem, &key_pem, &ca_pem)
        }
        None => DefaultCryptoProvider::from_pem(&cert_pem, &key_pem),
    }
    .map_err(|e| format!("building TLS config: {e}"))?;
    Ok(Some(provider))
}
