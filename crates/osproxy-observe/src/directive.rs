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
}

impl DiagnosticsDirective {
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
mod tests {
    use super::*;
    use osproxy_core::{Clock, ManualClock};
    use std::time::Duration;

    fn at(secs: u64) -> Instant {
        let clock = ManualClock::new();
        clock.advance(Duration::from_secs(secs));
        clock.now()
    }

    fn directive(
        level: DiagLevel,
        match_: DirectiveMatch,
        sample_per_mille: u16,
    ) -> DiagnosticsDirective {
        DiagnosticsDirective {
            id: "d1".to_owned(),
            match_,
            level,
            sample_per_mille,
            expires_at: at(100),
            ring_buffer: false,
        }
    }

    fn attrs<'a>(
        tenant: Option<&'a PartitionId>,
        index: &'a str,
        principal: &'a PrincipalId,
        endpoint: EndpointKind,
    ) -> RequestAttrs<'a> {
        RequestAttrs {
            tenant,
            index,
            principal,
            endpoint,
        }
    }

    #[test]
    fn an_empty_set_is_off() {
        let set = DirectiveSet::new();
        let p = PrincipalId::from("svc");
        let a = attrs(None, "orders", &p, EndpointKind::Search);
        assert_eq!(
            set.evaluate(&a, at(1), &RequestId::from("r")),
            DiagLevel::Off
        );
    }

    #[test]
    fn the_highest_matching_level_wins() {
        let set = DirectiveSet::from_directives(vec![
            directive(DiagLevel::Shape, DirectiveMatch::all(), 1000),
            directive(
                DiagLevel::ShapeRewriteDiff,
                DirectiveMatch::all().for_index(IndexName::from("orders")),
                1000,
            ),
        ]);
        let p = PrincipalId::from("svc");
        let a = attrs(None, "orders", &p, EndpointKind::Search);
        assert_eq!(
            set.evaluate(&a, at(1), &RequestId::from("r")),
            DiagLevel::ShapeRewriteDiff
        );
    }

    #[test]
    fn each_target_field_narrows_the_match() {
        let acme = PartitionId::from("acme");
        let p = PrincipalId::from("svc");
        let m = DirectiveMatch::all()
            .for_tenant(acme.clone())
            .for_endpoint(EndpointKind::Search);
        let set = DirectiveSet::from_directives(vec![directive(DiagLevel::Shape, m, 1000)]);

        // Matches: right tenant + endpoint.
        let hit = attrs(Some(&acme), "orders", &p, EndpointKind::Search);
        assert_eq!(
            set.evaluate(&hit, at(1), &RequestId::from("r")),
            DiagLevel::Shape
        );
        // Misses: wrong endpoint.
        let miss = attrs(Some(&acme), "orders", &p, EndpointKind::Count);
        assert_eq!(
            set.evaluate(&miss, at(1), &RequestId::from("r")),
            DiagLevel::Off
        );
        // Misses: tenant not yet resolved.
        let unresolved = attrs(None, "orders", &p, EndpointKind::Search);
        assert_eq!(
            set.evaluate(&unresolved, at(1), &RequestId::from("r")),
            DiagLevel::Off
        );
    }

    #[test]
    fn an_expired_directive_does_not_apply() {
        let set = DirectiveSet::from_directives(vec![directive(
            DiagLevel::Shape,
            DirectiveMatch::all(),
            1000,
        )]);
        let p = PrincipalId::from("svc");
        let a = attrs(None, "orders", &p, EndpointKind::Search);
        // expires_at is at(100); a request at(150) is past it.
        assert_eq!(
            set.evaluate(&a, at(150), &RequestId::from("r")),
            DiagLevel::Off
        );
    }

    #[test]
    fn sampling_is_deterministic_and_bounded() {
        // rate 0 never records; rate 1000 always; partial is stable per request.
        let p = PrincipalId::from("svc");
        let a = attrs(None, "orders", &p, EndpointKind::Search);
        let never = DirectiveSet::from_directives(vec![directive(
            DiagLevel::Shape,
            DirectiveMatch::all(),
            0,
        )]);
        let always = DirectiveSet::from_directives(vec![directive(
            DiagLevel::Shape,
            DirectiveMatch::all(),
            1000,
        )]);
        assert_eq!(
            never.evaluate(&a, at(1), &RequestId::from("r")),
            DiagLevel::Off
        );
        assert_eq!(
            always.evaluate(&a, at(1), &RequestId::from("r")),
            DiagLevel::Shape
        );

        let half = DirectiveSet::from_directives(vec![directive(
            DiagLevel::Shape,
            DirectiveMatch::all(),
            500,
        )]);
        let r = RequestId::from("req-123");
        let first = half.evaluate(&a, at(1), &r);
        assert_eq!(
            first,
            half.evaluate(&a, at(1), &r),
            "same request decides the same way"
        );
        // Across many requests, partial sampling admits some and not others.
        let admitted = (0..1000)
            .filter(|n| {
                half.evaluate(&a, at(1), &RequestId::from(format!("r{n}").as_str()))
                    == DiagLevel::Shape
            })
            .count();
        assert!(
            (300..700).contains(&admitted),
            "≈half admitted, got {admitted}"
        );
    }
}
