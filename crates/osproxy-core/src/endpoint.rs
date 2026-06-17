//! Classification of OpenSearch requests into handling categories.
//!
//! The OpenSearch REST surface is large; [`EndpointKind`] mirrors the supported
//! matrix in `docs/specs/opensearch-endpoints.md` and decides how a request is
//! treated by the tenancy layer. Adding a variant to a tenancy-aware class
//! requires a symmetry test (`docs/09`).

/// How a classified request must be handled by the routing/tenancy layer.
///
/// `#[non_exhaustive]` because the supported matrix grows over time and adding
/// an endpoint class must not be a breaking change (`docs/08` §7).
#[non_exhaustive]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EndpointKind {
    /// Single-document ingest (`_doc`, `_create`, `_update`): inject/construct,
    /// single target.
    IngestDoc,
    /// Bulk ingest (`_bulk`): demux by partition, re-interleave `items[]`.
    IngestBulk,
    /// Search/read (`_search`): partition filter + response field strip,
    /// single target.
    Search,
    /// Multi-search (`_msearch`): per-search partition filter + hit strip,
    /// demux by target, re-interleave `responses[]` — the search counterpart of
    /// `_bulk`.
    MultiSearch,
    /// Count (`_count`): same partition filter as search, but returns a count
    /// rather than hits, so no response field strip.
    Count,
    /// Read by id (`GET _doc/{id}`): logical→physical id transform.
    GetById,
    /// Multi-get (`_mget`): per-doc partition resolve, demux by target,
    /// re-interleave `docs[]` — the read counterpart of `_bulk`.
    MultiGet,
    /// Delete by id: logical→physical id transform.
    DeleteById,
    /// Delete by query (`_delete_by_query`): in async fan-out mode the proxy
    /// runs the partition-scoped query, then enqueues a concrete delete per
    /// match (`docs/04` §9). No synchronous implementation — rejected otherwise.
    DeleteByQuery,
    /// Cursor lifecycle (scroll, PIT): affinity pinning.
    Cursor,
    /// Administrative endpoints (`_cat`, `_cluster`, …): pass-through allow-list
    /// or reject; no tenancy semantics.
    Admin,
    /// Unmatched endpoint: rejected by default, pass-through if configured.
    Unknown,
}

impl EndpointKind {
    /// A stable, value-free name for this class — used in introspection readouts
    /// (e.g. a control-plane directive's `endpoint` target). Matches the variant
    /// name so it round-trips with a parser built from the same list.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IngestDoc => "IngestDoc",
            Self::IngestBulk => "IngestBulk",
            Self::Search => "Search",
            Self::MultiSearch => "MultiSearch",
            Self::Count => "Count",
            Self::GetById => "GetById",
            Self::MultiGet => "MultiGet",
            Self::DeleteById => "DeleteById",
            Self::DeleteByQuery => "DeleteByQuery",
            Self::Cursor => "Cursor",
            Self::Admin => "Admin",
            Self::Unknown => "Unknown",
        }
    }

    /// The inverse of [`EndpointKind::as_str`]: parses a class name back, or
    /// `None` if it is not a known class. Lets a control-plane directive target an
    /// endpoint over the wire (round-tripping with introspection).
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "IngestDoc" => Some(Self::IngestDoc),
            "IngestBulk" => Some(Self::IngestBulk),
            "Search" => Some(Self::Search),
            "MultiSearch" => Some(Self::MultiSearch),
            "Count" => Some(Self::Count),
            "GetById" => Some(Self::GetById),
            "MultiGet" => Some(Self::MultiGet),
            "DeleteById" => Some(Self::DeleteById),
            "DeleteByQuery" => Some(Self::DeleteByQuery),
            "Cursor" => Some(Self::Cursor),
            "Admin" => Some(Self::Admin),
            "Unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    /// Whether this class participates in tenancy rewriting (inject/filter/strip
    /// or id mapping). Used to decide whether a [`crate::ids::PartitionId`] must
    /// be resolvable for the request.
    #[must_use]
    pub fn is_tenancy_aware(self) -> bool {
        matches!(
            self,
            Self::IngestDoc
                | Self::IngestBulk
                | Self::Search
                | Self::MultiSearch
                | Self::Count
                | Self::GetById
                | Self::MultiGet
                | Self::DeleteById
                | Self::DeleteByQuery
                | Self::Cursor
        )
    }

    /// Whether this class writes data (and therefore must be epoch-stamped at
    /// the sink, `docs/06` §2).
    #[must_use]
    pub fn is_write(self) -> bool {
        matches!(self, Self::IngestDoc | Self::IngestBulk | Self::DeleteById)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_and_unknown_are_not_tenancy_aware() {
        assert!(!EndpointKind::Admin.is_tenancy_aware());
        assert!(!EndpointKind::Unknown.is_tenancy_aware());
    }

    #[test]
    fn ingest_and_read_paths_are_tenancy_aware() {
        for kind in [
            EndpointKind::IngestDoc,
            EndpointKind::IngestBulk,
            EndpointKind::Search,
            EndpointKind::MultiSearch,
            EndpointKind::Count,
            EndpointKind::GetById,
            EndpointKind::MultiGet,
            EndpointKind::DeleteById,
            EndpointKind::Cursor,
        ] {
            assert!(kind.is_tenancy_aware(), "{kind:?} should be tenancy-aware");
        }
    }

    #[test]
    fn write_classification_matches_intent() {
        assert!(EndpointKind::IngestDoc.is_write());
        assert!(EndpointKind::IngestBulk.is_write());
        assert!(EndpointKind::DeleteById.is_write());
        assert!(!EndpointKind::Search.is_write());
        assert!(!EndpointKind::GetById.is_write());
    }

    #[test]
    fn every_kind_round_trips_through_its_name() {
        // The introspection ↔ publish round-trip depends on as_str/from_name being
        // exact inverses for every variant; a new variant with a missed arm fails.
        for kind in [
            EndpointKind::IngestDoc,
            EndpointKind::IngestBulk,
            EndpointKind::Search,
            EndpointKind::MultiSearch,
            EndpointKind::Count,
            EndpointKind::GetById,
            EndpointKind::MultiGet,
            EndpointKind::DeleteById,
            EndpointKind::Cursor,
            EndpointKind::Admin,
            EndpointKind::Unknown,
        ] {
            assert_eq!(EndpointKind::from_name(kind.as_str()), Some(kind));
        }
        assert_eq!(EndpointKind::from_name("nope"), None);
    }
}
