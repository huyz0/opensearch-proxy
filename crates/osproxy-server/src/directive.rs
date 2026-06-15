//! Concrete HMAC verifier for the signed `X-Debug-Directive` header — the
//! surgical, single-request diagnostics channel (`docs/05` §3). An operator mints
//! a token off-band with the shared key; a client cannot forge one, so it cannot
//! self-enable verbose diagnostics (NFR-S3). The token rides the request and is
//! verified by whichever instance handles it.
//!
//! Token wire form: `{payload_hex}.{sig_hex}` where `payload` is a small JSON
//! object and `sig` is `HMAC-SHA256(key, payload_bytes)`. The MAC is computed and
//! checked through the build's **validated** crypto module (ring under `non-fips`,
//! aws-lc-rs under `fips`, cfg-selected exactly like the TLS cert fingerprint) so
//! a FIPS artifact never authenticates with a non-validated primitive.
//!
//! Payload fields: `level` (required, a [`DiagLevel`] name), `exp` (required,
//! absolute unix-seconds expiry), and optional targeting `tenant`/`index`/
//! `principal`, `sample_per_mille` (default 1000), `ring_buffer` (default false).

use std::sync::Arc;
use std::time::Duration;

use osproxy_core::{Clock, IndexName, PartitionId, PrincipalId};
use osproxy_observe::{DiagLevel, DiagnosticsDirective, DirectiveMatch, DirectiveVerifier};
use serde_json::Value;

// Exactly one validated crypto module must be linked, just like the transport
// crate's provider guard (ADR-009): catch a mis-invocation at compile time rather
// than failing opaquely on an unresolved `hmac::Key` or, worse, building an
// artifact that authenticates with no validated primitive.
#[cfg(all(feature = "fips", feature = "non-fips"))]
compile_error!(
    "features `fips` and `non-fips` are mutually exclusive — build with \
     `--no-default-features --features fips` for a FIPS artifact"
);
#[cfg(not(any(feature = "fips", feature = "non-fips")))]
compile_error!("enable exactly one crypto provider feature: `fips` or `non-fips`");

// The MAC stays on whichever validated module the build linked — same cfg-select
// as `cert_fingerprint` in the transport TLS path (ADR-009). ring and aws-lc-rs
// share this `hmac` API (`Key::new`, constant-time `verify`).
#[cfg(feature = "fips")]
use aws_lc_rs::hmac;
#[cfg(feature = "non-fips")]
use ring::hmac;

/// Verifies signed `X-Debug-Directive` tokens against a shared HMAC key.
pub struct HmacDirectiveVerifier {
    key: hmac::Key,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for HmacDirectiveVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the key. Shape only.
        f.debug_struct("HmacDirectiveVerifier")
            .finish_non_exhaustive()
    }
}

impl HmacDirectiveVerifier {
    /// Builds a verifier from the shared `secret` and a clock (used to enforce the
    /// token's absolute expiry against current time).
    #[must_use]
    pub fn new(secret: &[u8], clock: Arc<dyn Clock>) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, secret),
            clock,
        }
    }

    /// Turns a verified payload into the directive it authorizes, or `None` if the
    /// payload is malformed or already expired.
    fn to_directive(&self, payload: &[u8]) -> Option<DiagnosticsDirective> {
        let v: Value = serde_json::from_slice(payload).ok()?;
        let level = parse_level(v.get("level")?.as_str()?)?;
        let exp = v.get("exp")?.as_u64()?;

        // Convert the absolute unix-seconds expiry into an `Instant` on our clock;
        // a token whose expiry has already passed authorizes nothing.
        let now_secs = self.clock.unix_nanos() / 1_000_000_000;
        let remaining = exp.checked_sub(now_secs)?;
        if remaining == 0 {
            return None;
        }
        let expires_at = self
            .clock
            .now()
            .saturating_add(Duration::from_secs(remaining));

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
        // Default to always-sample; a present rate must be a valid per-mille
        // (`0..=1000`). An out-of-range value authorizes nothing rather than
        // failing open to the broadest capture — same strictness as `level`.
        let sample_per_mille = match v.get("sample_per_mille") {
            None => 1000,
            Some(n) => match n.as_u64() {
                Some(n) if n <= 1000 => u16::try_from(n).unwrap_or(1000),
                _ => return None,
            },
        };

        Some(DiagnosticsDirective {
            // A fixed label — never a tenant value — marks the header origin.
            id: "x-debug-header".to_owned(),
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
}

impl DirectiveVerifier for HmacDirectiveVerifier {
    fn verify(&self, header_value: &str) -> Option<DiagnosticsDirective> {
        let (payload_hex, sig_hex) = header_value.split_once('.')?;
        let payload = decode_hex(payload_hex)?;
        let sig = decode_hex(sig_hex)?;
        // Constant-time tag comparison inside the validated module.
        hmac::verify(&self.key, &payload, &sig).ok()?;
        self.to_directive(&payload)
    }
}

/// Maps a [`DiagLevel`] name to the level. The inverse of the variant names, so
/// the token vocabulary tracks the enum. Shared with the directive-admin decoder.
pub(crate) fn parse_level(name: &str) -> Option<DiagLevel> {
    match name {
        "Off" => Some(DiagLevel::Off),
        "Shape" => Some(DiagLevel::Shape),
        "ShapeTiming" => Some(DiagLevel::ShapeTiming),
        "ShapeRewriteDiff" => Some(DiagLevel::ShapeRewriteDiff),
        _ => None,
    }
}

/// Decodes a lowercase/uppercase hex string into bytes, or `None` if it is not
/// valid hex (odd length or a non-hex digit).
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16)?;
        let lo = (bytes[i + 1] as char).to_digit(16)?;
        out.push(u8::try_from(hi * 16 + lo).ok()?);
        i += 2;
    }
    Some(out)
}

/// Encodes bytes as lowercase hex. Mints tokens (operator tooling, exercised by
/// the verify-path tests); the verify path itself only decodes.
#[cfg(test)]
#[must_use]
pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(char::from_digit(u32::from(b >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(b & 0x0f), 16).unwrap_or('0'));
    }
    out
}

/// Mints a token string `{payload_hex}.{sig_hex}` for `payload` signed with
/// `secret`. Operator-side helper (and the basis for the verify-path tests).
#[cfg(test)]
#[must_use]
pub(crate) fn sign_token(secret: &[u8], payload: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let tag = hmac::sign(&key, payload);
    format!("{}.{}", encode_hex(payload), encode_hex(tag.as_ref()))
}

#[cfg(test)]
#[path = "directive_tests.rs"]
mod tests;
