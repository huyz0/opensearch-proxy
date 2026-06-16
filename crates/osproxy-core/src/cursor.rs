//! Stateless cursor-affinity envelope (`docs/03` §6).
//!
//! A scroll / PIT cursor is bound to the one physical cluster that created it, so
//! every follow-up request must reach that same cluster. In a **fleet** the
//! create and the continue may land on different proxy instances, so a binding
//! kept in one instance's memory is invisible to the others.
//!
//! This module makes the binding **travel with the cursor** instead of living in
//! a shared store: on create the proxy wraps the upstream cursor id together with
//! its cluster into a signed token the client echoes back; on continue *any*
//! instance recovers the cluster from the token alone — no shared state, no
//! replication lag, no read-after-write race. The upstream id is carried
//! verbatim as the payload (we never need spare room *inside* it), and the proxy
//! strips the envelope before talking upstream, so OpenSearch never sees it.
//!
//! The signature binds the cluster to *this* cursor: a client cannot redirect a
//! cursor to another cluster (the tag would not verify), and a tampered token
//! fails closed to "unresolvable cursor" — never a wrong-cluster dispatch.

use crate::ids::ClusterId;

/// Signs the cluster↔cursor binding. The concrete HMAC implementation (behind the
/// build's crypto provider) lives in the binary; this seam keeps the codec pure
/// and lets tests inject a deterministic signer.
///
/// The tag MUST be a deterministic function of `msg` and a fleet-wide shared key,
/// so an instance that did not create the cursor still verifies it.
pub trait CursorSigner: Send + Sync {
    /// A tag authenticating `msg`. Same key + same `msg` ⇒ same tag, on every
    /// instance.
    fn tag(&self, msg: &[u8]) -> Vec<u8>;
}

/// Wraps `cursor` (the upstream scroll/PIT id) with `cluster` into a signed,
/// self-describing token for the client. Format `{cluster_hex}.{tag_hex}.{cursor}`
/// — the cursor verbatim (it is base64, so it never contains the `.` delimiter).
#[must_use]
pub fn wrap(signer: &dyn CursorSigner, cluster: &ClusterId, cursor: &str) -> String {
    let tag = signer.tag(&binding(cluster, cursor));
    // Pre-size to the exact token length so the String never reallocates while
    // framing: two hex fields (2 chars/byte), two `.` separators, the cursor.
    let mut out =
        String::with_capacity(cluster.as_str().len() * 2 + tag.len() * 2 + 2 + cursor.len());
    push_hex(&mut out, cluster.as_str().as_bytes());
    out.push('.');
    push_hex(&mut out, &tag);
    out.push('.');
    out.push_str(cursor);
    out
}

/// Recovers `(cluster, upstream cursor)` from a token produced by [`wrap`], or
/// `None` if it is malformed or its signature does not verify (**fail-closed** —
/// a bad token is never routed anywhere).
#[must_use]
pub fn unwrap(signer: &dyn CursorSigner, token: &str) -> Option<(ClusterId, String)> {
    let mut parts = token.splitn(3, '.');
    let cluster_hex = parts.next()?;
    let tag_hex = parts.next()?;
    let cursor = parts.next()?;
    // Decode the cluster hex straight into the owned String the id will hold, so
    // there is no intermediate byte Vec to free (the id is move-constructed).
    let cluster = ClusterId::from(decode_hex_to_string(cluster_hex)?);
    // Verify by re-deriving the expected tag and comparing it against the
    // provided hex *in place* — no decoded-tag Vec is allocated. The compare is
    // constant-time over content for an equal length, like `constant_time_eq`.
    let expected = signer.tag(&binding(&cluster, cursor));
    if hex_eq_ct(tag_hex, &expected) {
        Some((cluster, cursor.to_owned()))
    } else {
        None
    }
}

/// The signed message: `cluster` and `cursor` joined by a byte that cannot appear
/// in either (a unit separator), so neither field can be shifted into the other.
fn binding(cluster: &ClusterId, cursor: &str) -> Vec<u8> {
    let mut msg = Vec::with_capacity(cluster.as_str().len() + 1 + cursor.len());
    msg.extend_from_slice(cluster.as_str().as_bytes());
    msg.push(0x1f);
    msg.extend_from_slice(cursor.as_bytes());
    msg
}

/// Constant-time equality of `expected` (raw bytes) against a hex string, without
/// decoding the hex into its own buffer. A length mismatch is unequal; for an
/// equal length the loop runs to completion without an early return, so a forged
/// tag leaks no timing signal about how many bytes matched. A non-hex digit makes
/// the whole comparison unequal (a malformed tag is a forged tag).
fn hex_eq_ct(hex: &str, expected: &[u8]) -> bool {
    let hex = hex.as_bytes();
    if hex.len() != expected.len() * 2 {
        return false;
    }
    let mut diff = 0u8;
    for (pair, &want) in hex.chunks_exact(2).zip(expected.iter()) {
        match (hex_val(pair[0]), hex_val(pair[1])) {
            (Some(hi), Some(lo)) => diff |= ((hi << 4) | lo) ^ want,
            // Mark a mismatch but keep scanning: no early return on content.
            _ => diff |= 1,
        }
    }
    diff == 0
}

/// Appends the lowercase hex of `bytes` to `out`.
fn push_hex(out: &mut String, bytes: &[u8]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
}

/// Decodes a lowercase/uppercase hex string directly into an owned UTF-8 string,
/// or `None` on an odd length, a non-hex digit, or non-UTF-8 bytes. The decode
/// buffer becomes the `String` without a second allocation, so recovering the
/// cluster id costs one allocation rather than a byte `Vec` plus a copy.
fn decode_hex_to_string(hex: &str) -> Option<String> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let bytes = hex.as_bytes();
    let mut out = Vec::with_capacity(hex.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = hex_val(pair[0])?;
        let lo = hex_val(pair[1])?;
        out.push((hi << 4) | lo);
    }
    String::from_utf8(out).ok()
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

#[cfg(test)]
#[path = "cursor_tests.rs"]
mod tests;
