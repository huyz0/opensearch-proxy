//! Compiles the async fan-out op-envelope `.proto` into Rust prost messages.
//!
//! Messages only (no gRPC service), so the output is plain prost types pulled in
//! via `include!` from `src/fanout.rs`, it never lives in the source tree or
//! counts against the file-length budget. Requires `protoc` on the build host
//! (already a prerequisite for the gRPC ingress in `osproxy-transport`).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/fanout.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(&[proto], &["proto"])?;
    Ok(())
}
