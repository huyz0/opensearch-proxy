//! W3C Trace Context propagation — the identifiers the proxy continues from an
//! incoming request and forwards to every downstream call so the upstream's
//! spans join the same distributed trace (`docs/05` §2, `OTel`).
//!
//! **Shape-only by construction.** A [`TraceContext`] holds only opaque trace and
//! span ids — correlation identity, never tenant values, bodies, or secrets. The
//! ids are derived from the request id (not from request *data*), so propagation
//! cannot become a value-leak channel.

use crate::RequestId;

/// The only W3C `traceparent` version this proxy emits/accepts (`00`).
const VERSION: &str = "00";
/// Length of a well-formed `traceparent`: `00-<32hex>-<16hex>-<2hex>`.
const TRACEPARENT_LEN: usize = 2 + 1 + 32 + 1 + 16 + 1 + 2;

/// A W3C trace context: the distributed-trace identity the proxy propagates
/// downstream. Continued from an incoming `traceparent` when present (preserving
/// the `trace_id` so the trace stays connected end-to-end), or minted as a new
/// root when absent. Either way a fresh `span_id` identifies *this* hop, so the
/// upstream call is recorded as a child of the proxy's span.
///
/// It holds only what is needed to *forward* context downstream. The incoming
/// parent's span id is not retained — recording the proxy's own span as a child
/// of the caller's span belongs to the OTLP-export slice (a follow-up), which
/// will widen this type or re-read the header then.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TraceContext {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    sampled: bool,
}

impl TraceContext {
    /// Continues `incoming_traceparent` if it is present and well-formed, else
    /// mints a new root trace. A fresh `span_id` for this hop is always derived
    /// from `request`, so the downstream call chains under the proxy's span.
    #[must_use]
    pub fn propagate(incoming_traceparent: Option<&str>, request: &RequestId) -> Self {
        match incoming_traceparent.and_then(Self::parse) {
            // Continue the caller's trace: keep its trace_id and sampling, but
            // present our own span as the parent of the downstream call.
            Some(parent) => Self {
                trace_id: parent.trace_id,
                span_id: derive8(request, SPAN_SEED),
                sampled: parent.sampled,
            },
            // No usable upstream context: this request is the trace root. Sample
            // it so the trace is actually useful to whoever collects it.
            None => Self {
                trace_id: derive16(request),
                span_id: derive8(request, SPAN_SEED),
                sampled: true,
            },
        }
    }

    /// Parses a W3C `traceparent` value (`00-<32hex>-<16hex>-<2hex>`). Returns
    /// `None` if it is malformed, an unsupported version, or has an all-zero
    /// trace/span id (which the spec forbids) — the caller then mints a root.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        if value.len() != TRACEPARENT_LEN {
            return None;
        }
        let mut parts = value.split('-');
        let version = parts.next()?;
        let trace_hex = parts.next()?;
        let span_hex = parts.next()?;
        let flags_hex = parts.next()?;
        if parts.next().is_some() || version != VERSION {
            return None;
        }
        let mut trace_id = [0u8; 16];
        let mut span_id = [0u8; 8];
        decode_hex(trace_hex, &mut trace_id)?;
        decode_hex(span_hex, &mut span_id)?;
        let flags = {
            let mut b = [0u8; 1];
            decode_hex(flags_hex, &mut b)?;
            b[0]
        };
        // All-zero ids are invalid per the W3C spec.
        if trace_id == [0u8; 16] || span_id == [0u8; 8] {
            return None;
        }
        Some(Self {
            trace_id,
            span_id,
            sampled: flags & 0x01 != 0,
        })
    }

    /// The `traceparent` header value to send to the upstream.
    #[must_use]
    pub fn to_traceparent(&self) -> String {
        let mut out = String::with_capacity(TRACEPARENT_LEN);
        out.push_str(VERSION);
        out.push('-');
        push_hex(&mut out, &self.trace_id);
        out.push('-');
        push_hex(&mut out, &self.span_id);
        out.push('-');
        push_hex(&mut out, &[u8::from(self.sampled)]);
        out
    }

    /// The 32-hex trace id, for correlating this request's logs / `/debug/explain`
    /// with the distributed trace. An identifier, never a value.
    #[must_use]
    pub fn trace_id_hex(&self) -> String {
        let mut out = String::with_capacity(32);
        push_hex(&mut out, &self.trace_id);
        out
    }

    /// Whether the trace is sampled (the W3C sampled flag).
    #[must_use]
    pub fn sampled(&self) -> bool {
        self.sampled
    }
}

/// Distinct FNV seed for span ids, so a request's span id never coincides with
/// the low 8 bytes of its (root) trace id.
const SPAN_SEED: u64 = 0x27d4_eb2f_1656_67c5;
/// FNV-1a 64-bit offset basis (the trace-id seed).
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a hash of `bytes` from `seed`.
fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// A random per-process seed mixed into every derived id, so ids stay **unique
/// across instances and restarts** even though the request id they derive from is
/// only process-local (and W3C wants span ids effectively random). `RandomState`
/// is seeded from the OS at process start — randomness without pulling an RNG
/// crate into `core`. It is constant for the life of the process, so derivation
/// stays deterministic *within* a process (the same request id yields the same
/// span on every call, e.g. every op of one bulk request shares the proxy span).
fn process_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *SEED.get_or_init(|| {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(FNV_OFFSET);
        h.finish()
    })
}

/// A 16-byte trace id derived from the request id (two independent hashes),
/// salted with the process seed (see [`process_seed`]).
fn derive16(request: &RequestId) -> [u8; 16] {
    derive16_with(process_seed(), request.as_str().as_bytes())
}

/// An 8-byte span id derived from the request id with `sub`, salted with the
/// process seed so a span id is unique across instances.
fn derive8(request: &RequestId, sub: u64) -> [u8; 8] {
    let mut out = fnv1a(sub ^ process_seed(), request.as_str().as_bytes()).to_be_bytes();
    if out == [0u8; 8] {
        out[7] = 1;
    }
    out
}

/// The seedable core of [`derive16`], split out so the cross-seed uniqueness
/// invariant is unit-testable (different seeds ⇒ disjoint ids for the same input).
fn derive16_with(seed: u64, s: &[u8]) -> [u8; 16] {
    let hi = fnv1a(FNV_OFFSET ^ seed, s).to_be_bytes();
    let lo = fnv1a(FNV_OFFSET ^ FNV_PRIME ^ seed, s).to_be_bytes();
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&hi);
    out[8..].copy_from_slice(&lo);
    if out == [0u8; 16] {
        out[15] = 1;
    }
    out
}

/// Decodes lowercase/uppercase hex into `out`, requiring exactly `2 * out.len()`
/// hex digits. Returns `None` on any non-hex byte or length mismatch.
fn decode_hex(hex: &str, out: &mut [u8]) -> Option<()> {
    if hex.len() != out.len() * 2 {
        return None;
    }
    for (i, byte) in out.iter_mut().enumerate() {
        let hi = hex_val(hex.as_bytes()[i * 2])?;
        let lo = hex_val(hex.as_bytes()[i * 2 + 1])?;
        *byte = (hi << 4) | lo;
    }
    Some(())
}

/// The value of a single hex digit, or `None` if it is not one.
fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Appends the lowercase hex of `bytes` to `out`.
fn push_hex(out: &mut String, bytes: &[u8]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn parses_a_valid_traceparent_and_round_trips() {
        let ctx = TraceContext::parse(SAMPLE).expect("valid");
        assert!(ctx.sampled());
        assert_eq!(ctx.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        // Re-emitting the parsed context reproduces it verbatim.
        assert_eq!(ctx.to_traceparent(), SAMPLE);
    }

    #[test]
    fn rejects_malformed_traceparents() {
        for bad in [
            "",
            "trash",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // version
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0",  // short flags
            "00-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-00f067aa0ba902b7-01", // non-hex
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01", // zero trace
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // zero span
        ] {
            assert!(TraceContext::parse(bad).is_none(), "should reject: {bad:?}");
        }
    }

    #[test]
    fn propagation_preserves_the_incoming_trace_id_but_starts_a_new_span() {
        let rid = RequestId::from("req-1");
        let ctx = TraceContext::propagate(Some(SAMPLE), &rid);
        // Same trace: downstream stays connected to the caller's trace.
        assert_eq!(ctx.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        // New span: the downstream call is a child of the proxy, not the caller.
        let downstream = ctx.to_traceparent();
        assert!(downstream.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
        assert!(
            !downstream.contains("00f067aa0ba902b7"),
            "proxy must present its own span id, not the caller's"
        );
    }

    #[test]
    fn an_unsampled_parent_keeps_its_flag() {
        let unsampled = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
        let ctx = TraceContext::propagate(Some(unsampled), &RequestId::from("r"));
        assert!(
            !ctx.sampled(),
            "sampling decision is inherited from the parent"
        );
    }

    #[test]
    fn a_missing_or_malformed_parent_mints_a_sampled_root() {
        for incoming in [None, Some("garbage")] {
            let ctx = TraceContext::propagate(incoming, &RequestId::from("req-7"));
            assert!(ctx.sampled(), "a freshly minted root is sampled");
            assert_eq!(ctx.to_traceparent().len(), TRACEPARENT_LEN);
        }
    }

    #[test]
    fn a_different_process_seed_yields_disjoint_ids_for_the_same_request() {
        // The fleet-uniqueness invariant: two instances (distinct process seeds)
        // must not derive the same id for the same (process-local) request id —
        // otherwise unrelated requests on different instances would collide.
        let s = b"req-5";
        assert_ne!(
            derive16_with(1, s),
            derive16_with(2, s),
            "different seeds must give different trace ids"
        );
        assert_ne!(
            fnv1a(7 ^ 1, s),
            fnv1a(7 ^ 2, s),
            "different seeds must give different span ids"
        );
    }

    #[test]
    fn derived_ids_are_stable_per_request_and_distinct_across_requests() {
        let a1 = TraceContext::propagate(None, &RequestId::from("a")).to_traceparent();
        let a2 = TraceContext::propagate(None, &RequestId::from("a")).to_traceparent();
        let b = TraceContext::propagate(None, &RequestId::from("b")).to_traceparent();
        assert_eq!(a1, a2, "same request id derives the same context");
        assert_ne!(a1, b, "different requests get different traces");
    }
}
