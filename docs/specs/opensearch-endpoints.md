# OpenSearch Endpoint Matrix (supported handling)

> Status: **implemented** — the classes below are live in `osproxy-transport`'s
> classifier and `osproxy-engine`'s dispatch. Remaining to-do is version-pinning
> the exact request/response shapes against a specific OpenSearch release
> (`[PIN: e.g. 2.x / 3.x]`); the handling classes themselves are stable.

This is the authoritative list of which OpenSearch REST endpoints the proxy
handles and how. `osproxy-core::EndpointKind` mirrors this table; adding a row to
a tenancy-aware class requires a symmetry test (docs/09).

> **Tenant-agnostic passthrough** (docs/04 §10) overrides this matrix for the
> indices it matches: those requests are forwarded verbatim with no tenancy
> handling at all. Everything below describes the default tenanted path.

## Classes

See docs/02 §5 for the class definitions. Default for unmatched: **reject**
(configurable to pass-through with operator acceptance of the isolation caveat).

## Matrix

| Method + Path | Class | Tenancy handling | Notes |
|---------------|-------|------------------|-------|
| `POST/PUT /{index}/_doc[/{id}]` | IngestDoc | inject + construct id + routing | single target |
| `POST /{index}/_create/{id}` | IngestDoc | construct id, fail-if-exists | |
| `POST /{index}/_update/{id}` | IngestDoc | id map + (partial) inject | scripted update review |
| `POST /_bulk`, `POST /{index}/_bulk` | IngestBulk | demux by partition, re-interleave | streaming |
| `GET/POST /{index}/_search` | Search | filter + strip | single target |
| `GET/POST /_search` (cross-index) | Search | resolve per partition; reject if ambiguous | see note |
| `GET/POST /{index}/_count` | Search | filter | |
| `POST /_msearch` | Search | per-subquery, each single-target | demux |
| `GET /{index}/_doc/{id}` | GetById | logical→physical id | |
| `GET /_mget`, `POST /_mget` | GetById | demux by doc | re-interleave |
| `DELETE /{index}/_doc/{id}` | DeleteById | id map | |
| `POST /{index}/_delete_by_query` | DeleteByQuery | reject by default; **async opt-in** expansion (`fanout_expand_delete_by_query`) runs the partition-scoped query and enqueues a delete per matched id | docs/04 §9 |
| `POST /{index}/_update_by_query` | (unsupported) | not classified → `Unknown` → reject | needs read-modify-write |
| `POST /{index}/_search/scroll`, `_search/scroll` | Cursor | affinity pin | |
| `POST /{index}/_search/point_in_time` (create), `POST /_search` w/ body `pit.id` (search), `DELETE /_search/point_in_time` w/ `pit_id[]` (close) | Cursor | affinity pin | OpenSearch PIT shape (not ES `_pit`); create/search re-wrap `pit_id`, close groups by cluster |
| `_sql`, scripted/`_render` | (unsupported) | reject in shared mode unless allow-listed | isolation risk |
| `_cat/*`, `_cluster/*`, `_nodes/*` | Admin | pass-through allow-list or reject | no tenancy semantics |
| index create/mapping/settings | Admin | policy-dependent | usually operator-only |
| anything else | Unknown | reject (default) / pass-through (configured) | |

### Note — ambiguous multi-index / cross-index search

A search that does not resolve to a single partition (e.g. a wildcard across
partitions) cannot be served single-target and is **rejected** with
`SpiError::UnsupportedEndpoint` / a typed ambiguity error — consistent with the
no-fan-out decision (docs/00 §non-goals). The operator may define explicit logical
indices that map to a single placement to support such patterns.

## To verify per row (M1+)

- Exact request/response shapes from the pinned OpenSearch version.
- Whether the endpoint accepts `_routing`, `_source` filtering, stored fields —
  affects strip logic.
- Bulk action-line grammar edge cases.
