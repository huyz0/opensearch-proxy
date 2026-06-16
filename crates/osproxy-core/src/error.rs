//! The request-path error taxonomy.
//!
//! Every failure reachable from the request path is typed and carries an
//! [`ErrorContext`]: a stable code, the decision chain that led there, whether
//! it is retryable, and an actionable remediation hint. This is what lets an
//! LLM diagnose a failure from telemetry alone, with no source reading
//! (`docs/02` §4, `docs/05`, NFR-T5).
//!
//! The context carries **ids and shapes only — never tenant values or secrets**
//! (`docs/05` §7).

use std::fmt;

use crate::ids::{ClusterId, IndexName, PartitionId, PrincipalId};

/// A stable, documented, machine-matchable error code.
///
/// Codes are part of the public contract: operators and LLMs match on them and
/// look them up in the generated error reference. `#[non_exhaustive]` so new
/// codes are additive (`docs/08` §7).
#[non_exhaustive]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ErrorCode {
    /// The partition could not be resolved from the request.
    PartitionUnresolved,
    /// No placement exists for the resolved partition.
    PlacementMissing,
    /// The placement-lookup backend was unavailable.
    PlacementBackendUnavailable,
    /// The endpoint is not supported for tenancy rewriting in this mode.
    UnsupportedEndpoint,
    /// A write was rejected because its stamped epoch is stale for a migrating
    /// partition (`docs/06` §2). Retryable: the client re-resolves.
    StaleEpoch,
    /// Client authentication failed.
    AuthFailed,
    /// The authenticated principal is not authorized for the action.
    Unauthorized,
    /// The upstream cluster failed (timeout, reset, 5xx).
    UpstreamFailed,
    /// The proxy is shedding load.
    Overloaded,
    /// A scroll/PIT cursor could not be resolved to its pinned cluster — its
    /// affinity envelope is missing, malformed, or unverifiable. The client must
    /// re-issue the originating search (`docs/03` §6).
    CursorUnresolvable,
}

impl ErrorCode {
    /// A short, stable, machine-readable slug for logs and trace attributes.
    #[must_use]
    pub fn as_slug(self) -> &'static str {
        match self {
            Self::PartitionUnresolved => "partition_unresolved",
            Self::PlacementMissing => "placement_missing",
            Self::PlacementBackendUnavailable => "placement_backend_unavailable",
            Self::UnsupportedEndpoint => "unsupported_endpoint",
            Self::StaleEpoch => "stale_epoch",
            Self::AuthFailed => "auth_failed",
            Self::Unauthorized => "unauthorized",
            Self::UpstreamFailed => "upstream_failed",
            Self::Overloaded => "overloaded",
            Self::CursorUnresolvable => "cursor_unresolvable",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_slug())
    }
}

/// The ordered chain of routing decisions made before a failure occurred.
///
/// Each field is populated as the request advances through the pipeline so a
/// failure at any stage carries the full upstream context. Every field is an
/// id (never a value), keeping the chain safe to emit in telemetry.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct DecisionChain {
    /// The authenticated principal, if authentication succeeded.
    pub principal: Option<PrincipalId>,
    /// The resolved partition, if resolution succeeded.
    pub partition: Option<PartitionId>,
    /// The target cluster, if placement resolved.
    pub cluster: Option<ClusterId>,
    /// The target index, if placement resolved.
    pub index: Option<IndexName>,
}

impl DecisionChain {
    /// An empty chain (nothing decided yet).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// The structured context attached to every request-path error.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ErrorContext {
    /// The stable error code.
    pub code: ErrorCode,
    /// The decision chain leading to the failure (ids/shapes only).
    pub decision_chain: DecisionChain,
    /// Whether the caller may retry (possibly after re-resolving).
    pub retryable: bool,
    /// A short, actionable hint for an operator or LLM.
    pub remediation: &'static str,
}

impl ErrorContext {
    /// Builds a context for `code` with an empty decision chain. Stages enrich
    /// the chain via [`ErrorContext::with_chain`] as context becomes available.
    #[must_use]
    pub fn new(code: ErrorCode, retryable: bool, remediation: &'static str) -> Self {
        Self {
            code,
            decision_chain: DecisionChain::new(),
            retryable,
            remediation,
        }
    }

    /// Attaches a decision chain (builder style).
    #[must_use]
    pub fn with_chain(mut self, chain: DecisionChain) -> Self {
        self.decision_chain = chain;
        self
    }
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (retryable={}): {}",
            self.code, self.retryable, self.remediation
        )
    }
}

impl std::error::Error for ErrorContext {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant's slug and Display, so each `as_slug` arm is exercised and
    /// slugs stay stable, distinct, and lowercase (they are part of the public
    /// contract — operators and LLMs match on them).
    #[test]
    fn every_error_code_has_a_stable_distinct_slug() {
        let all = [
            ErrorCode::PartitionUnresolved,
            ErrorCode::PlacementMissing,
            ErrorCode::PlacementBackendUnavailable,
            ErrorCode::UnsupportedEndpoint,
            ErrorCode::StaleEpoch,
            ErrorCode::AuthFailed,
            ErrorCode::Unauthorized,
            ErrorCode::UpstreamFailed,
            ErrorCode::Overloaded,
            ErrorCode::CursorUnresolvable,
        ];
        let mut seen = std::collections::HashSet::new();
        for code in all {
            let slug = code.as_slug();
            assert_eq!(slug, code.to_string(), "Display must equal as_slug");
            assert!(
                slug.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{slug} must be lowercase snake_case"
            );
            assert!(seen.insert(slug), "duplicate slug {slug}");
        }
        assert_eq!(seen.len(), all.len());
    }

    #[test]
    fn context_carries_chain_and_displays_actionably() {
        let chain = DecisionChain {
            partition: Some(PartitionId::from("t-1")),
            ..DecisionChain::new()
        };
        let ctx = ErrorContext::new(
            ErrorCode::PlacementMissing,
            false,
            "register a placement for the partition",
        )
        .with_chain(chain.clone());

        assert_eq!(ctx.decision_chain, chain);
        assert!(!ctx.retryable);
        assert!(ctx.to_string().contains("placement_missing"));
        assert!(ctx.to_string().contains("register a placement"));
    }

    #[test]
    fn context_is_a_std_error() {
        fn assert_error<E: std::error::Error>(_: &E) {}
        let ctx = ErrorContext::new(ErrorCode::Overloaded, true, "retry with backoff");
        assert_error(&ctx);
    }
}
