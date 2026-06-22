//! Workspace automation. Run via `cargo xtask <command>`.
//!
//! Commands:
//!   ci        Run the full local gate: fmt, clippy, test, doc, budgets, skills.
//!   fmt       Check formatting (`cargo fmt --check`).
//!   clippy    Lint with warnings denied.
//!   test      Run all tests.
//!   doc       Build docs (warnings denied) and run doc tests.
//!   budgets   Check size budgets and that overflows carry a `// JUSTIFY`.
//!   skills    Validate .agents/skills/*/SKILL.md (size + frontmatter).
//!   spawn     Background-task discipline: no bare `tokio::spawn` in libraries.
//!   arch      Static crate dependency-direction / acyclicity check.
//!   bench     Deterministic instruction-count microbenchmarks (needs valgrind).
//!   check-fips Build + test the FIPS feature (needs cmake/C/Go; else skips).
//!
//! See docs/08-engineering-standards.md, docs/10-review-process.md, docs/12.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let cmd = std::env::args().nth(1).unwrap_or_else(|| "ci".to_owned());
    let result = match cmd.as_str() {
        "ci" => run_ci(),
        "fmt" => fmt(),
        "clippy" => clippy(),
        "test" => test(),
        "doc" => doc(),
        "budgets" => budgets(),
        "skills" => skills(),
        "spawn" => spawn_discipline(),
        "arch" => arch(),
        "bench" => bench(),
        "bench-local" => bench_local(),
        "check-fips" => check_fips(),
        other => Err(format!("unknown command: {other}\n{USAGE}")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("xtask: {msg}");
            ExitCode::FAILURE
        }
    }
}

const USAGE: &str = "usage: cargo xtask \
     <ci|fmt|clippy|test|doc|budgets|skills|spawn|arch|bench|bench-local|check-fips>";

fn run_ci() -> Result<(), String> {
    fmt()?;
    clippy()?;
    arch()?;
    test()?;
    check_fips()?;
    doc()?;
    budgets()?;
    skills()?;
    spawn_discipline()?;
    println!("\nxtask: all gates passed ✓");
    Ok(())
}

/// The allowed direct internal dependencies of each crate. This is the
/// authoritative dependency DAG (docs/01 §2); it is acyclic by construction, so
/// verifying each crate's actual internal deps are a subset proves the whole
/// graph stays downward-only and cycle-free (docs/12).
fn allowed_internal_deps(crate_name: &str) -> Option<&'static [&'static str]> {
    Some(match crate_name {
        "osproxy-core" => &[],
        "osproxy-spi" => &["osproxy-core"],
        "osproxy-tenancy" => &["osproxy-core", "osproxy-spi", "osproxy-rewrite"],
        "osproxy-transport" => &["osproxy-core", "osproxy-spi"],
        "osproxy-rewrite" => &["osproxy-core"],
        "osproxy-sink" => &["osproxy-core", "osproxy-spi"],
        "osproxy-control" => &["osproxy-core", "osproxy-spi", "osproxy-tenancy"],
        "osproxy-observe" => &["osproxy-core"],
        "osproxy-otlp" => &["osproxy-observe"],
        // Reference distributed DirectiveStore: implements the observe seam over
        // etcd. A leaf adapter, like otlp — nothing depends upward on it.
        "osproxy-etcd" => &["osproxy-core", "osproxy-observe"],
        "osproxy-config" => &["osproxy-core"],
        // The traffic-capture seam (a low leaf) and the queue writer that
        // implements it; the broker client is opt-in and not a workspace edge.
        "osproxy-capture" => &["osproxy-spi"],
        "osproxy-kafka" => &["osproxy-capture"],
        // The portable pure-Rust Producer (krafka + rustls); implements the
        // queue-writer seam. krafka feature-gates its rustls provider, so a FIPS
        // build links only aws-lc-rs (no ring).
        "osproxy-kafka-krafka" => &["osproxy-kafka"],
        // The durable spill buffer: a Producer that persists to a WAL and drains
        // to an AckProducer. Broker-agnostic, so its only internal edge is the
        // queue-writer seam crate.
        "osproxy-kafka-wal" => &["osproxy-kafka"],
        // Workspace-excluded (links librdkafka); its only internal edge is the
        // queue-writer crate whose `Producer` seam it implements.
        "osproxy-kafka-rdkafka" => &["osproxy-kafka"],
        "osproxy-engine" => &[
            "osproxy-core",
            "osproxy-spi",
            "osproxy-tenancy",
            "osproxy-rewrite",
            "osproxy-sink",
            "osproxy-control",
            "osproxy-observe",
        ],
        // The binary is the one place every layer meets: it wires a concrete
        // tenancy + sink into the engine pipeline and serves it over transport.
        "osproxy-server" => &[
            "osproxy-core",
            "osproxy-config",
            "osproxy-capture",
            "osproxy-engine",
            "osproxy-observe",
            "osproxy-transport",
            "osproxy-spi",
            "osproxy-tenancy",
            "osproxy-sink",
            "osproxy-otlp",
            // optional (feature `etcd`): the reference distributed directive store.
            "osproxy-etcd",
            // optional (feature `fanout`): the queue-writer seam and the
            // portable krafka producer it composes for traffic capture.
            "osproxy-kafka",
            "osproxy-kafka-krafka",
            "osproxy-kafka-wal",
            // dev-only: the #[ignore]'d perf harness reads NFR-P profile types.
            "osproxy-bench",
        ],
        // Pure NFR-P profile vocabulary (percentiles, profile, judge). The
        // Docker-backed load runner that fills a profile in is a later, env-gated
        // slice; the deterministic types here depend on no other osproxy crate.
        "osproxy-bench" => &[],
        _ => return None,
    })
}

/// Static architecture check: every crate's actual internal dependencies must be
/// a subset of its allowed set in `allowed_internal_deps`. Fails on an undeclared
/// dependency (an upward or sideways edge) or an unknown crate.
fn arch() -> Result<(), String> {
    println!("xtask: checking crate dependency graph");
    let crates_dir = workspace_root().join("crates");
    let mut manifests = Vec::new();
    collect_named_files(&crates_dir, "Cargo.toml", &mut manifests);

    let mut violations = Vec::new();
    for manifest in &manifests {
        let text = std::fs::read_to_string(manifest)
            .map_err(|e| format!("read {}: {e}", manifest.display()))?;
        let name = package_name(&text)
            .ok_or_else(|| format!("no [package] name in {}", manifest.display()))?;
        let Some(allowed) = allowed_internal_deps(&name) else {
            violations.push(format!(
                "  unknown crate `{name}` — add it to allowed_internal_deps"
            ));
            continue;
        };
        for dep in internal_deps(&text) {
            if !allowed.contains(&dep.as_str()) {
                violations.push(format!(
                    "  {name} -> {dep} is not an allowed dependency edge"
                ));
            }
        }
    }

    if violations.is_empty() {
        println!("xtask: dependency graph ok ({} crates)", manifests.len());
        Ok(())
    } else {
        Err(format!(
            "architecture check failed (see docs/01 §2):\n{}",
            violations.join("\n")
        ))
    }
}

/// Extracts the `name = "..."` from a manifest's `[package]` section.
fn package_name(manifest: &str) -> Option<String> {
    manifest
        .lines()
        .skip_while(|l| l.trim() != "[package]")
        .skip(1)
        .take_while(|l| !l.trim_start().starts_with('['))
        .find_map(|l| {
            let rest = l
                .trim()
                .strip_prefix("name")?
                .trim_start()
                .strip_prefix('=')?;
            Some(rest.trim().trim_matches('"').to_owned())
        })
}

/// Returns the internal (`osproxy-*`) crates this manifest depends on. Internal
/// deps are declared as `osproxy-x.workspace = true`.
fn internal_deps(manifest: &str) -> Vec<String> {
    manifest
        .lines()
        .filter_map(|l| {
            let t = l.trim();
            if t.starts_with("osproxy-") && t.contains("workspace") {
                Some(t.split(['.', ' ']).next()?.to_owned())
            } else {
                None
            }
        })
        .collect()
}

/// Runs the deterministic instruction-count microbenchmarks (iai-callgrind).
/// Requires valgrind + iai-callgrind-runner; intended for CI (docs/12). Not part
/// of `ci` because it needs valgrind, which is not present on every dev box.
fn bench() -> Result<(), String> {
    cargo(&["bench", "--workspace"], &[])
}

/// Runs the wall-clock micro-benchmarks (divan) — a local calibration tool that
/// needs no special tooling and runs on any dev box, unlike `bench`. *Not* a CI
/// gate: wall-clock is host-specific and noisy, so it must never fail a build;
/// the deterministic gates stay dhat (alloc) and iai-callgrind (instructions).
fn bench_local() -> Result<(), String> {
    for (pkg, bench) in [
        ("osproxy-rewrite", "hot_paths"),
        ("osproxy-engine", "search_transform"),
        ("osproxy-transport", "classify"),
        ("osproxy-observe", "directive"),
    ] {
        cargo(&["bench", "-p", pkg, "--bench", bench], &[])?;
    }
    Ok(())
}

/// Builds and tests the FIPS build (`--features fips`), which the default gates
/// never exercise (they run the `non-fips`/`ring` provider). It links aws-lc-rs
/// FIPS, whose native AWS-LC-FIPS build needs `cmake` + a C compiler + `go`; where
/// that toolchain is absent this skips with a warning rather than failing, so the
/// `ci` lane stays green on a dev box without the toolchain and is a real gate
/// wherever it is installed (docs/07). The fips tests assert the linked module
/// reports FIPS mode and offers exactly the approved suites (`tests/fips.rs`).
fn check_fips() -> Result<(), String> {
    // CI runs this in a dedicated `fips` job, so the main `gate` job sets this to
    // avoid building the (heavy) native AWS-LC-FIPS twice. Local `xtask ci` leaves
    // it unset and runs the fips gate when the toolchain is present.
    if std::env::var_os("OSPROXY_SKIP_FIPS").is_some() {
        println!(
            "xtask: check-fips SKIPPED — OSPROXY_SKIP_FIPS set (run in the dedicated CI job)."
        );
        return Ok(());
    }
    let missing: Vec<&str> = ["cmake", "cc", "go"]
        .into_iter()
        .filter(|tool| !tool_on_path(tool))
        .collect();
    if !missing.is_empty() {
        println!(
            "xtask: check-fips SKIPPED — missing FIPS build toolchain: {}. \
             Install it (see README) or run this on a CI runner that has it.",
            missing.join(", ")
        );
        return Ok(());
    }
    cargo(
        &[
            "test",
            "-p",
            "osproxy-transport",
            "-p",
            "osproxy-server",
            "--no-default-features",
            "--features",
            "fips",
        ],
        &[],
    )
}

/// Whether `tool` is resolvable on `PATH` (via `command -v`).
fn tool_on_path(tool: &str) -> bool {
    Command::new("sh")
        .args(["-c", &format!("command -v {tool}")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn fmt() -> Result<(), String> {
    cargo(&["fmt", "--all", "--", "--check"], &[])
}

fn clippy() -> Result<(), String> {
    cargo(
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
        &[],
    )
}

fn test() -> Result<(), String> {
    // `--lib --bins --tests` excludes `--benches`: iai-callgrind benches need
    // valgrind to run and would fail outside CI. clippy still lints them via its
    // own `--all-targets`.
    //
    // `--test-threads=1`: the dhat allocation-budget tests measure the
    // *process-global* heap-block counter around a tight operation. Under libtest's
    // default parallelism a concurrent test's allocation lands in that window and
    // inflates the count, which flakes the exact budgets on a busy CI runner (a
    // `&'static`-returning fn "allocating"). Serial execution keeps each window
    // quiet, so the budgets stay exact and deterministic everywhere.
    cargo(
        &[
            "test",
            "--workspace",
            "--lib",
            "--bins",
            "--tests",
            "--",
            "--test-threads=1",
        ],
        &[],
    )
}

fn doc() -> Result<(), String> {
    cargo(
        &[
            "doc",
            "--workspace",
            "--no-deps",
            "--document-private-items",
        ],
        &[("RUSTDOCFLAGS", "-D warnings")],
    )?;
    // Doc tests are not covered by `--all-targets` above.
    cargo(&["test", "--workspace", "--doc"], &[])
}

/// Enforces the file-length budget (docs/08 §1): a Rust source file may exceed
/// the hard limit only if it carries a `// JUSTIFY(file-length):` marker.
fn budgets() -> Result<(), String> {
    const HARD_LIMIT: usize = 400;
    let root = workspace_root();
    let mut violations = Vec::new();
    let mut files = Vec::new();
    collect_rs_files(&root.join("crates"), &mut files);
    collect_rs_files(&root.join("xtask"), &mut files);

    for file in files {
        let content =
            std::fs::read_to_string(&file).map_err(|e| format!("read {}: {e}", file.display()))?;
        let lines = content.lines().count();
        if lines > HARD_LIMIT && !content.contains("// JUSTIFY(file-length)") {
            violations.push(format!(
                "  {} has {lines} lines (limit {HARD_LIMIT})",
                file.display()
            ));
        }
    }

    if violations.is_empty() {
        println!("xtask: file-length budgets ok");
        Ok(())
    } else {
        Err(format!(
            "file-length budget exceeded without `// JUSTIFY(file-length)`:\n{}",
            violations.join("\n")
        ))
    }
}

/// Validates the agent skill system (see .agents/skills/manage-skills/SKILL.md):
/// every `SKILL.md` must be <=100 lines and carry frontmatter with a `name` and
/// a `description` following the `WHAT: ... USE WHEN: ...` pattern.
fn skills() -> Result<(), String> {
    const LINE_LIMIT: usize = 100;
    let dir = workspace_root().join(".agents/skills");
    let mut skill_files = Vec::new();
    collect_named_files(&dir, "SKILL.md", &mut skill_files);

    if skill_files.is_empty() {
        return Err(format!("no SKILL.md files found under {}", dir.display()));
    }

    let mut violations = Vec::new();
    for file in &skill_files {
        let content =
            std::fs::read_to_string(file).map_err(|e| format!("read {}: {e}", file.display()))?;
        let shown = file.strip_prefix(workspace_root()).unwrap_or(file);
        let lines = content.lines().count();
        if lines > LINE_LIMIT {
            violations.push(format!(
                "  {} has {lines} lines (limit {LINE_LIMIT})",
                shown.display()
            ));
        }
        let frontmatter = content.split("---").nth(1).unwrap_or_default();
        if !frontmatter.contains("name:") {
            violations.push(format!("  {} frontmatter missing `name:`", shown.display()));
        }
        if !(frontmatter.contains("WHAT:") && frontmatter.contains("USE WHEN:")) {
            violations.push(format!(
                "  {} description must match `WHAT: ... USE WHEN: ...`",
                shown.display()
            ));
        }
    }

    if violations.is_empty() {
        println!("xtask: {} skills ok", skill_files.len());
        Ok(())
    } else {
        Err(format!(
            "skill validation failed:\n{}",
            violations.join("\n")
        ))
    }
}

/// Background-task discipline (`docs/08`): a library crate must not call bare
/// `tokio::spawn` — it would panic (or silently no-op) when invoked outside a
/// running runtime, which a library cannot assume. Background work in a library
/// captures a `tokio::runtime::Handle` and spawns on it (as `osproxy-otlp` does),
/// so the absence of a runtime is handled, not assumed.
///
/// Exempt: the binary (`osproxy-server`) owns the runtime, and `osproxy-transport`
/// spawns only from inside its `async` accept loops where a runtime is guaranteed
/// by the awaiting caller. A deliberate exception elsewhere carries a
/// `// JUSTIFY(spawn):` marker on the same line.
fn spawn_discipline() -> Result<(), String> {
    const EXEMPT_CRATES: &[&str] = &["osproxy-server", "osproxy-transport"];
    let crates_dir = workspace_root().join("crates");
    let mut files = Vec::new();
    collect_rs_files(&crates_dir, &mut files);

    let mut violations = Vec::new();
    for file in files {
        let path = file.to_string_lossy();
        // Only library source is in scope: integration tests run inside a
        // `#[tokio::test]` runtime, so spawning there is always sound.
        if !path.contains("/src/")
            || EXEMPT_CRATES
                .iter()
                .any(|c| path.contains(&format!("crates/{c}/")))
        {
            continue;
        }
        let content =
            std::fs::read_to_string(&file).map_err(|e| format!("read {}: {e}", file.display()))?;
        let shown = file.strip_prefix(workspace_root()).unwrap_or(&file);
        for (n, line) in content.lines().enumerate() {
            if line.contains("tokio::spawn(") && !line.contains("// JUSTIFY(spawn)") {
                violations.push(format!("  {}:{}", shown.display(), n + 1));
            }
        }
    }

    if violations.is_empty() {
        println!("xtask: spawn discipline ok");
        Ok(())
    } else {
        Err(format!(
            "bare `tokio::spawn` in a library crate (capture a runtime Handle \
             instead, or mark `// JUSTIFY(spawn):`):\n{}",
            violations.join("\n")
        ))
    }
}

fn collect_named_files(dir: &Path, name: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_named_files(&path, name, out);
        } else if path.file_name().is_some_and(|n| n == name) {
            out.push(path);
        }
    }
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn workspace_root() -> PathBuf {
    // xtask lives at <root>/xtask; CARGO_MANIFEST_DIR points there.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn cargo(args: &[&str], envs: &[(&str, &str)]) -> Result<(), String> {
    println!("xtask: cargo {}", args.join(" "));
    let mut command = Command::new(env!("CARGO"));
    command.args(args).current_dir(workspace_root());
    for (k, v) in envs {
        command.env(k, v);
    }
    let status = command
        .status()
        .map_err(|e| format!("failed to spawn cargo: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`cargo {}` failed ({status})", args.join(" ")))
    }
}
