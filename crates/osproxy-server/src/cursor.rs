//! Concrete HMAC signer for the stateless scroll/PIT affinity envelope
//! (`docs/03` §6). The cluster a cursor is pinned to travels *with* the cursor in
//! a signed token, so any fleet instance can recover it with no shared store; the
//! signature stops a client redirecting a cursor to another cluster.
//!
//! The MAC is computed through the build's **validated** crypto module (ring
//! under `non-fips`, aws-lc-rs under `fips`, cfg-selected exactly like the
//! directive verifier and the TLS cert fingerprint, ADR-009), so a FIPS artifact
//! never signs with a non-validated primitive. The mutual-exclusion compile
//! guards live in [`crate::directive`].

use osproxy_core::CursorSigner;

// Same cfg-select as `HmacDirectiveVerifier`: ring and aws-lc-rs share this
// `hmac` API (`Key::new`, `sign`).
#[cfg(feature = "fips")]
use aws_lc_rs::hmac;
#[cfg(feature = "non-fips")]
use ring::hmac;

/// Signs cursor-affinity envelopes with a shared `HMAC-SHA256` key. The same key
/// must be configured on every proxy instance so a token wrapped on one verifies
/// on another (the whole point of the stateless design).
pub struct HmacCursorSigner {
    key: hmac::Key,
}

impl std::fmt::Debug for HmacCursorSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the key.
        f.debug_struct("HmacCursorSigner").finish_non_exhaustive()
    }
}

impl HmacCursorSigner {
    /// Builds a signer from the shared `secret`.
    #[must_use]
    pub fn new(secret: &[u8]) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, secret),
        }
    }
}

impl CursorSigner for HmacCursorSigner {
    fn tag(&self, msg: &[u8]) -> Vec<u8> {
        hmac::sign(&self.key, msg).as_ref().to_vec()
    }
}

#[cfg(test)]
#[path = "cursor_tests.rs"]
mod tests;
