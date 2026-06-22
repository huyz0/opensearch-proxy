//! Markdown briefs: the consolidated, human- and LLM-readable view of an NFR-P
//! run. The runners emit machine-readable profile/verdict JSON for a judge to
//! consume programmatically; these render the *same* numbers as a brief an
//! operator reads in a CI run summary, or an LLM judges in prose.
//!
//! Shape-only, like everything in this crate: timings, counts, and verdicts,
//! never request data.

use std::fmt::Write as _;

use crate::footprint::FootprintProfile;
use crate::judge::Verdict;
use crate::profile::NfrProfile;
use crate::scale::ScalabilityCurve;

/// A Markdown brief for the single-operating-point latency/reuse profile.
#[must_use]
pub fn profile_brief(profile: &NfrProfile, verdict: &Verdict) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "### NFR-P latency & reuse\n");
    let _ = writeln!(
        out,
        "{} samples at concurrency {}.\n",
        profile.samples, profile.concurrency
    );
    let _ = writeln!(out, "| metric | value |\n| --- | --- |");
    row(&mut out, "added p50", &ms(profile.added_p50_ns()));
    row(&mut out, "added p99", &ms(profile.added_p99_ns()));
    row(
        &mut out,
        "baseline p50 → proxy p50",
        &latency_pair(profile.baseline.p50_ns, profile.proxy.p50_ns),
    );
    row(
        &mut out,
        "baseline p99 → proxy p99",
        &latency_pair(profile.baseline.p99_ns, profile.proxy.p99_ns),
    );
    row(
        &mut out,
        "pool reuse",
        &format!("{:.4}", profile.pool_reuse_rate),
    );
    row(
        &mut out,
        "throughput",
        &format!("{:.0} rps", profile.throughput_rps),
    );
    push_verdict(&mut out, verdict);
    out
}

/// A Markdown brief for the concurrency sweep: a per-point table plus the
/// tail-amplification / throughput-scaling summary.
#[must_use]
pub fn scalability_brief(curve: &ScalabilityCurve, verdict: &Verdict) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "### NFR-P2 scalability\n");
    let _ = writeln!(
        out,
        "| concurrency | p50 | p99 | throughput |\n| ---: | ---: | ---: | ---: |"
    );
    for p in &curve.points {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {:.0} rps |",
            p.concurrency,
            ms(p.latency.p50_ns),
            ms(p.latency.p99_ns),
            p.throughput_rps
        );
    }
    let _ = writeln!(
        out,
        "\ntail amplification **{:.2}×**, throughput scaling **{:.2}×**.",
        curve.tail_amplification(),
        curve.throughput_scaling()
    );
    push_verdict(&mut out, verdict);
    out
}

/// A Markdown brief for the idle/soak memory footprint.
#[must_use]
pub fn footprint_brief(profile: &FootprintProfile, verdict: &Verdict) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "### NFR-P6 footprint\n");
    let _ = writeln!(out, "| metric | value |\n| --- | --- |");
    row(&mut out, "idle RSS", &mib(profile.idle_rss_bytes));
    row(&mut out, "post-soak RSS", &mib(profile.soak_rss_bytes));
    row(
        &mut out,
        "growth",
        &format!(
            "{:.2}× ({}) over {} reqs",
            profile.growth_ratio(),
            mib(profile.growth_bytes()),
            profile.soak_requests
        ),
    );
    push_verdict(&mut out, verdict);
    out
}

/// Appends a verdict line and its per-finding bullets.
fn push_verdict(out: &mut String, verdict: &Verdict) {
    let _ = writeln!(
        out,
        "\n**Verdict: {}** _(provisional thresholds)_",
        if verdict.pass { "PASS ✅" } else { "FAIL ❌" }
    );
    for f in &verdict.findings {
        let _ = writeln!(
            out,
            "- {} **{}**: {}",
            if f.pass { "✅" } else { "❌" },
            f.nfr,
            f.detail
        );
    }
}

/// A `| key | value |` table row.
fn row(out: &mut String, key: &str, value: &str) {
    let _ = writeln!(out, "| {key} | {value} |");
}

/// `<baseline> → <proxy>` as a millisecond pair, for the latency rows.
fn latency_pair(baseline_ns: u64, proxy_ns: u64) -> String {
    format!("{} → {}", ms(baseline_ns), ms(proxy_ns))
}

/// Nanoseconds as a millisecond string. Lossy only above 2^52 ns (~52 days), far
/// beyond any sample, so the precision-loss lint is suppressed.
#[allow(clippy::cast_precision_loss)]
fn ms(ns: u64) -> String {
    format!("{:.3} ms", ns as f64 / 1_000_000.0)
}

/// Bytes as a mebibyte string. Lossy only above 2^52 bytes, far beyond any
/// process, so the precision-loss lint is suppressed.
#[allow(clippy::cast_precision_loss)]
fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Renders a header for a combined NFR-P brief, the runners append their own
/// sections under it. Kept tiny so callers can prepend a title once.
#[must_use]
pub fn brief_header(title: &str) -> String {
    format!("## {title}\n\n")
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
