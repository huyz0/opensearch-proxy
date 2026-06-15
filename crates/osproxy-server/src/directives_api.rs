//! Decoding an operator-published fleet directive set from the
//! `POST /admin/directives` JSON body (`docs/05` §3).
//!
//! **Fail-closed**: any malformed or out-of-range field rejects the *whole* set
//! rather than publishing a partial or surprising directive. The same vocabulary
//! as the signed `X-Debug-Directive` token ([`crate::directive`]) — `level`,
//! optional `tenant`/`index`/`principal` targeting, `sample_per_mille`,
//! `ring_buffer` — but expressed with a relative `ttl_secs` (resolved against the
//! injected clock) so an operator says "for N seconds" and a forgotten "on" still
//! self-expires.
//!
//! Body shape: `{"directives": [ {"id","level","ttl_secs", ...}, ... ]}`.

use std::time::Duration;

use osproxy_core::{Clock, IndexName, PartitionId, PrincipalId};
use osproxy_observe::{DiagnosticsDirective, DirectiveMatch, DirectiveSet};
use serde_json::Value;

use crate::directive::parse_level;

/// Decodes the publish body into a [`DirectiveSet`], or a stable value-free
/// reason slug on the first malformed field.
pub(crate) fn decode_directive_set(
    body: &[u8],
    clock: &dyn Clock,
) -> Result<DirectiveSet, &'static str> {
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
    "sample_per_mille",
    "ring_buffer",
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
    let level = parse_level(
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
    })
}

#[cfg(test)]
#[path = "directives_api_tests.rs"]
mod tests;
