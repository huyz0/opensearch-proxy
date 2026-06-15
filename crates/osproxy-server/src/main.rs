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
use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;

use osproxy_core::{ClusterId, IndexName};
use osproxy_engine::Pipeline;
use osproxy_otlp::OtlpHttpExporter;
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
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

/// Reads configuration from the environment, builds the pipeline, and serves
/// until interrupted.
async fn run() -> Result<(), String> {
    let bind = env_or("OSPROXY_BIND", "127.0.0.1:8080");
    let upstream = env_or("OSPROXY_UPSTREAM", "http://127.0.0.1:9200");
    let index = env_or("OSPROXY_INDEX", "osproxy-shared");
    let cluster = ClusterId::from("default");

    let mut endpoints = HashMap::new();
    endpoints.insert(cluster.clone(), upstream.clone());
    let sink = OpenSearchSink::new(endpoints);

    let tenancy = ReferenceTenancy::new(cluster, IndexName::from(index.as_str()));
    let pipeline = with_otlp_export(Pipeline::new(TenancyRouter::new(tenancy), sink));

    let tokens = parse_tokens(&env_or("OSPROXY_TOKENS", ""));
    let auth_mode = if tokens.is_empty() {
        "dev (open)"
    } else {
        "token"
    };
    let handler = Arc::new(AppHandler::new(
        pipeline,
        ReferenceAuthenticator::new(tokens),
    ));

    let listener = TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("binding {bind}: {e}"))?;

    // TLS when both cert and key paths are configured; cleartext otherwise. The
    // same provider terminates the HTTP and gRPC listeners.
    let provider = load_tls_provider()?.map(Arc::new);

    // Optional gRPC ingress on its own listener, driving the same handler
    // (same pipeline, tenancy, and observability) as the HTTP front door.
    if let Some(grpc_bind) = std::env::var("OSPROXY_GRPC_BIND")
        .ok()
        .filter(|v| !v.is_empty())
    {
        let grpc_listener = TcpListener::bind(&grpc_bind)
            .await
            .map_err(|e| format!("binding gRPC {grpc_bind}: {e}"))?;
        spawn_grpc(
            grpc_listener,
            provider.clone(),
            Arc::clone(&handler),
            &grpc_bind,
        );
    }

    if let Some(provider) = provider {
        println!(
            "osproxy listening on https://{bind}, upstream {upstream}, shared index {index}, auth {auth_mode}"
        );
        serve_until_signal(osproxy_transport::serve_tls(listener, provider, handler)).await
    } else {
        println!(
            "osproxy listening on http://{bind}, upstream {upstream}, shared index {index}, auth {auth_mode}"
        );
        serve_until_signal(osproxy_transport::serve(listener, handler)).await
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

/// Runs a serve future until it errors or a shutdown signal arrives.
async fn serve_until_signal<F>(serve: F) -> Result<(), String>
where
    F: Future<Output = std::io::Result<()>>,
{
    tokio::select! {
        result = serve => result.map_err(|e| format!("serving: {e}")),
        _ = tokio::signal::ctrl_c() => {
            println!("osproxy: shutdown signal received");
            Ok(())
        }
    }
}

/// Wires OTLP span export onto the pipeline when `OSPROXY_OTLP_ENDPOINT` is set
/// (the collector base URL, e.g. `http://otel-collector:4318`); otherwise export
/// stays off (no telemetry cost). `OSPROXY_SERVICE_NAME` sets the reported
/// `service.name` (default `osproxy`).
fn with_otlp_export<T: TenancySpi, S: Sink + Reader>(pipeline: Pipeline<T, S>) -> Pipeline<T, S> {
    let Some(endpoint) = std::env::var("OSPROXY_OTLP_ENDPOINT")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        return pipeline;
    };
    let service = env_or("OSPROXY_SERVICE_NAME", "osproxy");
    println!("osproxy OTLP span export -> {endpoint}/v1/traces (service={service})");
    pipeline
        .with_exporter(Arc::new(OtlpHttpExporter::new(&endpoint)))
        .with_service_name(service)
}

/// Builds a TLS provider from `OSPROXY_TLS_CERT`/`OSPROXY_TLS_KEY` (PEM file
/// paths). Returns `None` if neither is set (cleartext), or an error if one is
/// set without the other or the files cannot be read/parsed. If
/// `OSPROXY_TLS_CLIENT_CA` is also set, mutual TLS is required and clients must
/// present a certificate chaining to that CA.
fn load_tls_provider() -> Result<Option<DefaultCryptoProvider>, String> {
    let cert_path = std::env::var("OSPROXY_TLS_CERT")
        .ok()
        .filter(|v| !v.is_empty());
    let key_path = std::env::var("OSPROXY_TLS_KEY")
        .ok()
        .filter(|v| !v.is_empty());
    let (cert, key) = match (cert_path, key_path) {
        (None, None) => return Ok(None),
        (Some(cert), Some(key)) => (cert, key),
        _ => return Err("set both OSPROXY_TLS_CERT and OSPROXY_TLS_KEY, or neither".to_owned()),
    };

    let cert_pem = std::fs::read(&cert).map_err(|e| format!("reading {cert}: {e}"))?;
    let key_pem = std::fs::read(&key).map_err(|e| format!("reading {key}: {e}"))?;

    let provider = match std::env::var("OSPROXY_TLS_CLIENT_CA")
        .ok()
        .filter(|v| !v.is_empty())
    {
        Some(ca) => {
            let ca_pem = std::fs::read(&ca).map_err(|e| format!("reading {ca}: {e}"))?;
            DefaultCryptoProvider::from_pem_mtls(&cert_pem, &key_pem, &ca_pem)
        }
        None => DefaultCryptoProvider::from_pem(&cert_pem, &key_pem),
    }
    .map_err(|e| format!("building TLS config: {e}"))?;
    Ok(Some(provider))
}

/// Reads an environment variable, falling back to `default` if unset or empty.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// Parses a `token=principal,token2=principal2` list into a map. An empty input
/// yields an empty map, which puts the authenticator in permissive dev mode.
fn parse_tokens(spec: &str) -> HashMap<String, String> {
    spec.split(',')
        .filter_map(|pair| pair.split_once('='))
        .map(|(token, principal)| (token.trim().to_owned(), principal.trim().to_owned()))
        .filter(|(token, principal)| !token.is_empty() && !principal.is_empty())
        .collect()
}
