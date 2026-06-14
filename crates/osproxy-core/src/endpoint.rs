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
    /// Search/read (`_search`, `_msearch`): partition filter + response field
    /// strip, single target.
    Search,
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
    /// Cursor lifecycle (scroll, PIT): affinity pinning.
    Cursor,
    /// Administrative endpoints (`_cat`, `_cluster`, …): pass-through allow-list
    /// or reject; no tenancy semantics.
    Admin,
    /// Unmatched endpoint: rejected by default, pass-through if configured.
    Unknown,
}

impl EndpointKind {
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
                | Self::Count
                | Self::GetById
                | Self::MultiGet
                | Self::DeleteById
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
}
