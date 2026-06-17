//! Runtime diagnostics directives — verbosity as **data**, not a code path
//! (`docs/05` §3-4).
//!
//! A [`DiagnosticsDirective`] says "record this much detail for requests matching
//! this target, at this sample rate, until this time." The [`DirectiveSet`]
//! evaluator turns the active directives plus a request's attributes into the
//! effective [`DiagLevel`] — the single decision the pipeline reads to decide how
//! much to record/export. It is the hot path, so evaluation is allocation-free
//! and the default (no directive matches) is [`DiagLevel::Off`] at near-zero cost.
//!
//! This module is the **spine** both delivery channels feed: the signed
//! `X-Debug-Directive` request header (surgical) and the control-plane store
//! (fleet-wide). Targeting is the cost lever and the TTL the safety net — a
//! forgotten "on" expires instead of silently burning cost.

use osproxy_core::{EndpointKind, IndexName, Instant, PartitionId, PrincipalId, RequestId};

/// How much detail to record for a request (ordered: higher = more verbose).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub enum DiagLevel {
    /// No recording/export beyond the always-on minimum. Near-zero cost.
    #[default]
    Off,
    /// Shape-only spans (ids, names, sizes) — the standard causal trace.
    Shape,
    /// Shapes plus per-stage timing.
    ShapeTiming,
    /// Shapes, timing, and the rewrite before/after *shape* diff (never values).
    ShapeRewriteDiff,
}

impl DiagLevel {
    /// The level's stable wire name — the inverse of the publish/​header parser,
    /// so an introspected directive's `level` re-publishes verbatim.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "Off",
            Self::Shape => "Shape",
            Self::ShapeTiming => "ShapeTiming",
            Self::ShapeRewriteDiff => "ShapeRewriteDiff",
        }
    }
}

/// What a directive targets. A request matches when **every set field** equals
/// the request's corresponding attribute; an unset field is a wildcard, so an
/// all-unset match targets every request.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct DirectiveMatch {
    /// Match only this partition/tenant (once resolved).
    pub tenant: Option<PartitionId>,
    /// Match only this logical index.
    pub index: Option<IndexName>,
    /// Match only this principal.
    pub principal: Option<PrincipalId>,
    /// Match only this endpoint class.
    pub endpoint: Option<EndpointKind>,
}

impl DirectiveMatch {
    /// A match targeting every request (all fields wildcard).
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Narrows the match to `tenant` (builder style).
    #[must_use]
    pub fn for_tenant(mut self, tenant: PartitionId) -> Self {
        self.tenant = Some(tenant);
        self
    }

    /// Narrows the match to `index` (builder style).
    #[must_use]
    pub fn for_index(mut self, index: IndexName) -> Self {
        self.index = Some(index);
        self
    }

    /// Narrows the match to `principal` (builder style).
    #[must_use]
    pub fn for_principal(mut self, principal: PrincipalId) -> Self {
        self.principal = Some(principal);
        self
    }

    /// Narrows the match to `endpoint` (builder style).
    #[must_use]
    pub fn for_endpoint(mut self, endpoint: EndpointKind) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Whether `attrs` satisfies every set field of this match.
    #[must_use]
    pub fn matches(&self, attrs: &RequestAttrs<'_>) -> bool {
        self.tenant.as_ref().is_none_or(|t| attrs.tenant == Some(t))
            && self
                .index
                .as_ref()
                .is_none_or(|i| i.as_str() == attrs.index)
            && self.principal.as_ref().is_none_or(|p| p == attrs.principal)
            && self.endpoint.is_none_or(|e| e == attrs.endpoint)
    }
}

/// The attributes of a request, evaluated against directive matches. The tenant
/// is optional because it is only known after partition resolution.
#[derive(Clone, Copy, Debug)]
pub struct RequestAttrs<'a> {
    /// The resolved partition, if resolution has happened.
    pub tenant: Option<&'a PartitionId>,
    /// The logical index from the request path.
    pub index: &'a str,
    /// The authenticated principal.
    pub principal: &'a PrincipalId,
    /// The endpoint classification.
    pub endpoint: EndpointKind,
}

/// One diagnostics directive: target, verbosity, sampling, and expiry.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DiagnosticsDirective {
    /// A stable id for management/correlation (never a tenant value).
    pub id: String,
    /// What this directive targets.
    pub match_: DirectiveMatch,
    /// The verbosity to apply to matching requests.
    pub level: DiagLevel,
    /// Fraction of matching requests to record, in `0..=1` (scaled to per-mille
    /// so the directive stays `Eq`/`Hash`-friendly; `1000` = always).
    pub sample_per_mille: u16,
    /// Absolute expiry: a request at or after this instant does not match (the
    /// TTL was applied when the directive was created).
    pub expires_at: Instant,
    /// Single-instance break-glass: capture into the local ring buffer.
    pub ring_buffer: bool,
    /// Fleet traffic capture: tee the matching exchanges to the configured
    /// capture sink (e.g. Kafka). The runtime on/off switch for capture — off in
    /// the baseline, flipped on by publishing a directive, so capture is on demand
    /// and fleet-wide with no restart. Distinct from [`Self::ring_buffer`], which
    /// is the single-instance forensic tape.
    pub capture: bool,
}

impl DiagnosticsDirective {
    /// This directive's [`DiagLevel`] if it applies to `attrs` at `now` for
    /// `request` (target matches, not expired, in sample), else `None`. Used to
    /// fold a single-request (signed-header) directive into the evaluation.
    #[must_use]
    pub fn level_if_applies(
        &self,
        attrs: &RequestAttrs<'_>,
        now: Instant,
        request: &RequestId,
    ) -> Option<DiagLevel> {
        self.applies(attrs, now, request).then_some(self.level)
    }

    /// Whether this directive applies to `attrs` at `now` for `request`: not
    /// expired, target matches, and the request falls within the sample.
    #[must_use]
    fn applies(&self, attrs: &RequestAttrs<'_>, now: Instant, request: &RequestId) -> bool {
        now < self.expires_at && self.match_.matches(attrs) && self.is_sampled(request)
    }

    /// Whether `request` is in this directive's sample. Deterministic per request
    /// id (no RNG): the same request always decides the same way, so a retry is
    /// recorded consistently.
    #[must_use]
    fn is_sampled(&self, request: &RequestId) -> bool {
        if self.sample_per_mille >= 1000 {
            return true;
        }
        if self.sample_per_mille == 0 {
            return false;
        }
        // Map the request id into 0..1000 and keep it under the threshold.
        let bucket = fnv1a(request.as_str().as_bytes()) % 1000;
        u16::try_from(bucket).unwrap_or(u16::MAX) < self.sample_per_mille
    }
}

/// The set of active directives, evaluated per request. Cheap to evaluate (a
/// filtered scan); typically a handful of directives are active at once.
#[derive(Clone, Debug, Default)]
pub struct DirectiveSet {
    directives: Vec<DiagnosticsDirective>,
}

impl DirectiveSet {
    /// An empty set — every request evaluates to [`DiagLevel::Off`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a set from active directives.
    #[must_use]
    pub fn from_directives(directives: Vec<DiagnosticsDirective>) -> Self {
        Self { directives }
    }

    /// How many directives the set holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.directives.len()
    }

    /// Whether the set is empty (every request evaluates to `Off`).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.directives.is_empty()
    }

    /// The effective level for a request: the **highest** level among the
    /// directives that apply (target matches, not expired, in sample), or
    /// [`DiagLevel::Off`] if none do.
    #[must_use]
    pub fn evaluate(
        &self,
        attrs: &RequestAttrs<'_>,
        now: Instant,
        request: &RequestId,
    ) -> DiagLevel {
        self.directives
            .iter()
            .filter(|d| d.applies(attrs, now, request))
            .map(|d| d.level)
            .max()
            .unwrap_or(DiagLevel::Off)
    }

    /// Whether any applying directive requests local ring-buffer capture.
    #[must_use]
    pub fn wants_ring_buffer(
        &self,
        attrs: &RequestAttrs<'_>,
        now: Instant,
        request: &RequestId,
    ) -> bool {
        self.directives
            .iter()
            .any(|d| d.ring_buffer && d.applies(attrs, now, request))
    }

    /// Whether any applying directive turns on fleet traffic capture for this
    /// request. The runtime gate for capture-on-demand: with no matching
    /// directive (the baseline), this is `false` and nothing is teed.
    #[must_use]
    pub fn wants_capture(
        &self,
        attrs: &RequestAttrs<'_>,
        now: Instant,
        request: &RequestId,
    ) -> bool {
        self.directives
            .iter()
            .any(|d| d.capture && d.applies(attrs, now, request))
    }

    /// A well-defined, shape-only introspection of the active settings: for each
    /// directive, what it targets, at what verbosity and sample, whether it
    /// captures to the ring buffer, and whether it has expired at `now`.
    ///
    /// This is the **read** side of the control-plane store — an agent fetches it
    /// to see exactly what an instance is applying. The schema mirrors the publish
    /// body ([`crate::DirectiveSet`] decoding), except the relative `ttl_secs` is
    /// reported as a computed `expired` flag, since expiry is held as an absolute
    /// monotonic instant that has no portable numeric form. Value-free throughout:
    /// the only strings are operator-authored ids and targeting selectors.
    #[must_use]
    pub fn introspect(&self, now: Instant) -> serde_json::Value {
        let directives: Vec<serde_json::Value> = self
            .directives
            .iter()
            .map(|d| {
                let mut obj = serde_json::Map::new();
                obj.insert("id".into(), d.id.clone().into());
                obj.insert("level".into(), d.level.as_str().into());
                if let Some(t) = &d.match_.tenant {
                    obj.insert("tenant".into(), t.as_str().into());
                }
                if let Some(i) = &d.match_.index {
                    obj.insert("index".into(), i.as_str().into());
                }
                if let Some(p) = &d.match_.principal {
                    obj.insert("principal".into(), p.as_str().into());
                }
                if let Some(e) = d.match_.endpoint {
                    obj.insert("endpoint".into(), e.as_str().into());
                }
                obj.insert("sample_per_mille".into(), d.sample_per_mille.into());
                obj.insert("ring_buffer".into(), d.ring_buffer.into());
                obj.insert("capture".into(), d.capture.into());
                obj.insert("expired".into(), (now >= d.expires_at).into());
                serde_json::Value::Object(obj)
            })
            .collect();
        serde_json::json!({ "directives": directives })
    }
}

/// Verifies a signed `X-Debug-Directive` request header into the directive it
/// authorizes — the **surgical, single-request** delivery channel (`docs/05`
/// §3). The signature means a client cannot self-enable verbose diagnostics
/// without the operator's key (NFR-S3); the directive follows the request to
/// whatever instance handles it. The concrete implementation (HMAC + the token
/// format) lives in a crypto-capable crate behind this seam.
pub trait DirectiveVerifier: Send + Sync {
    /// The directive a valid header authorizes, or `None` if the header is
    /// absent, malformed, incorrectly signed, or already expired.
    fn verify(&self, header_value: &str) -> Option<DiagnosticsDirective>;
}

/// The default: no header channel is configured, so every header is rejected.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoVerifier;

impl DirectiveVerifier for NoVerifier {
    fn verify(&self, _header_value: &str) -> Option<DiagnosticsDirective> {
        None
    }
}

/// FNV-1a 64-bit hash, for deterministic sampling (no RNG, no dependency).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = u64::wrapping_mul(h, 0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
#[path = "directive_tests.rs"]
mod tests;
