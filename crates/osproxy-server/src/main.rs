//! The `osproxy` binary.
//!
//! Owns process lifecycle and wires the crates together (`docs/01` §3). It
//! holds no business logic. At milestone M0 the pipeline is an empty scaffold:
//! the binary builds, reports its identity, and exits cleanly. Serving traffic
//! arrives in M1 (`docs/11`).

/// Entry point. Returns a process exit code rather than panicking, consistent
/// with the no-panic reliability requirement (NFR-R1).
fn main() -> std::process::ExitCode {
    println!(
        "{} {} — design-phase scaffold (M0); see docs/11-roadmap.md",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
    );
    std::process::ExitCode::SUCCESS
}
