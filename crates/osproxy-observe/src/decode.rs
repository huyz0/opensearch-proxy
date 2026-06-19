//! Decoding an operator-published fleet [`DirectiveSet`] from JSON (`docs/05` §3).
//!
//! One fail-closed decoder shared by every publish channel — the
//! `POST /admin/directives` admin endpoint and a distributed `DirectiveStore`
//! (e.g. etcd) — so a directive means the same thing however it arrives, and a
//! typo can never silently widen its blast radius.
//!
//! **Fail-closed**: any malformed or out-of-range field rejects the *whole* set
//! rather than publishing a partial or surprising directive. The vocabulary
//! matches the signed `X-Debug-Directive` token (`level`, optional
//! `tenant`/`index`/`principal`/`endpoint` targeting, `sample_per_mille`,
//! `ring_buffer`, `capture`) but with a relative `ttl_secs` resolved against the
//! injected clock, so a forgotten "on" still self-expires.
//!
//! Body shape: `{"directives": [ {"id","level","ttl_secs", ...}, ... ]}`.

use std::time::Duration;

use osproxy_core::{Clock, EndpointKind, IndexName, PartitionId, PrincipalId};
use serde_json::Value;

use crate::directive::{DiagLevel, DiagnosticsDirective, DirectiveMatch, DirectiveSet};

/// Decodes a publish body into a [`DirectiveSet`], or a stable value-free reason
/// slug on the first malformed field.
///
/// # Errors
/// A `&'static str` slug (e.g. `"unknown_field"`, `"zero_ttl"`) naming the first
/// rejection, suitable for a log or an HTTP reason — never echoes a value.
pub fn decode_directive_set(body: &[u8], clock: &dyn Clock) -> Result<DirectiveSet, &'static str> {
    let v: Value = serde_json::from_slice(body).map_err(|_| "invalid_json")?;
    reject_unknown_keys(&v, &["directives"])?;
    let items = v
        .get("directives")
        .and_then(Value::as_array)
        .ok_or("missing_directives")?;
    let mut directives = Vec::with_capacity(items.len());
    for item in items {
        directives.push(decode_one(item, clock)?);
    }
    Ok(DirectiveSet::from_directives(directives))
}

/// The directive fields a publish body may carry. A typo'd key (e.g. `"tennant"`)
/// is rejected rather than silently dropped — a mistyped `"tenant"` must not
/// quietly widen a directive to the whole fleet.
const DIRECTIVE_KEYS: &[&str] = &[
    "id",
    "level",
    "ttl_secs",
    "tenant",
    "index",
    "principal",
    "endpoint",
    "sample_per_mille",
    "ring_buffer",
    "capture",
];

/// Rejects an object carrying any key outside `allowed` (fail-closed; an unknown
/// key signals a typo or a mismatched client and could change the directive's
/// meaning if accepted).
fn reject_unknown_keys(v: &Value, allowed: &[&str]) -> Result<(), &'static str> {
    let obj = v.as_object().ok_or("not_an_object")?;
    if obj.keys().all(|k| allowed.contains(&k.as_str())) {
        Ok(())
    } else {
        Err("unknown_field")
    }
}

/// Decodes a single directive, resolving its relative TTL against `clock`.
fn decode_one(v: &Value, clock: &dyn Clock) -> Result<DiagnosticsDirective, &'static str> {
    reject_unknown_keys(v, DIRECTIVE_KEYS)?;
    let id = v
        .get("id")
        .and_then(Value::as_str)
        .ok_or("missing_id")?
        .to_owned();
    let level = DiagLevel::from_name(
        v.get("level")
            .and_then(Value::as_str)
            .ok_or("missing_level")?,
    )
    .ok_or("unknown_level")?;

    let ttl_secs = v
        .get("ttl_secs")
        .and_then(Value::as_u64)
        .ok_or("missing_ttl_secs")?;
    if ttl_secs == 0 {
        return Err("zero_ttl");
    }
    let expires_at = clock.now().saturating_add(Duration::from_secs(ttl_secs));

    // A present sampling rate must be a valid per-mille; out of range fails closed
    // rather than widening capture to always-on.
    let sample_per_mille = match v.get("sample_per_mille") {
        None => 1000,
        Some(n) => match n.as_u64() {
            Some(n) if n <= 1000 => u16::try_from(n).unwrap_or(1000),
            _ => return Err("bad_sample_rate"),
        },
    };

    let mut match_ = DirectiveMatch::all();
    if let Some(t) = v.get("tenant").and_then(Value::as_str) {
        match_ = match_.for_tenant(PartitionId::from(t));
    }
    if let Some(i) = v.get("index").and_then(Value::as_str) {
        match_ = match_.for_index(IndexName::from(i));
    }
    if let Some(p) = v.get("principal").and_then(Value::as_str) {
        match_ = match_.for_principal(PrincipalId::from(p));
    }
    // A present `endpoint` must name a known class; an unknown one fails closed
    // rather than silently widening the target (it round-trips with `as_str`).
    if let Some(e) = v.get("endpoint") {
        let name = e.as_str().ok_or("bad_endpoint")?;
        match_ = match_.for_endpoint(EndpointKind::from_name(name).ok_or("unknown_endpoint")?);
    }

    Ok(DiagnosticsDirective {
        id,
        match_,
        level,
        sample_per_mille,
        expires_at,
        ring_buffer: v
            .get("ring_buffer")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        capture: v.get("capture").and_then(Value::as_bool).unwrap_or(false),
    })
}

#[cfg(test)]
#[path = "decode_tests.rs"]
mod tests;
