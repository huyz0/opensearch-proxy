//! The `osproxy` binary.
//!
//! Owns process lifecycle and wires the crates together (`docs/01` §3): it
//! builds a concrete tenancy + sink, drives them through the engine pipeline,
//! and serves that over the HTTP/1.1 ingress. It holds no business logic of its
//! own — the tenancy here is a minimal *reference* implementation showing how a
//! library consumer wires the SPI.
//!
//! M1 serves single-document ingest in cleartext (`docs/11`); TLS, auth, and
//! observability attach in later slices.

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use osproxy_core::{ClusterId, IndexName};
use osproxy_engine::Pipeline;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_tenancy::TenancyRouter;
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
    let pipeline = Pipeline::new(TenancyRouter::new(tenancy), sink);
    let handler = Arc::new(AppHandler::new(pipeline));

    let listener = TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("binding {bind}: {e}"))?;
    println!("osproxy listening on {bind}, upstream {upstream}, shared index {index}");

    tokio::select! {
        result = osproxy_transport::serve(listener, handler) => {
            result.map_err(|e| format!("serving: {e}"))
        }
        _ = tokio::signal::ctrl_c() => {
            println!("osproxy: shutdown signal received");
            Ok(())
        }
    }
}

/// Reads an environment variable, falling back to `default` if unset or empty.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_owned())
}
