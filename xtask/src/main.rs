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
//!
//! See docs/08-engineering-standards.md and docs/10-review-process.md.

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

const USAGE: &str = "usage: cargo xtask <ci|fmt|clippy|test|doc|budgets|skills>";

fn run_ci() -> Result<(), String> {
    fmt()?;
    clippy()?;
    test()?;
    doc()?;
    budgets()?;
    skills()?;
    println!("\nxtask: all gates passed ✓");
    Ok(())
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
    cargo(&["test", "--workspace", "--all-targets"], &[])
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
