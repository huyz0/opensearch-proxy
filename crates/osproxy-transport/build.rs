//! Compiles the gRPC ingress `.proto` into Rust (server stubs + prost messages).
//!
//! Generated code lands in `OUT_DIR` and is pulled in via `tonic::include_proto!`
//! in `src/grpc.rs`, so it never lives in the source tree or counts against the
//! file-length budget. Requires `protoc` on the build host.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/osproxy.proto";
    println!("cargo:rerun-if-changed={proto}");
    // Generate both server and client; the client is used only by the in-crate
    // round-trip test (and is otherwise dead code, allowed in the `pb` module).
    tonic_prost_build::configure().compile_protos(&[proto], &["proto"])?;
    Ok(())
}
