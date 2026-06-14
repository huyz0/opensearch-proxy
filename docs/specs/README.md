# Vendored Specs & References

This folder holds the **authoritative external references** the design depends
on, plus our own derived spec tables. The point is that the design is
self-contained — an implementer (human or LLM) does not need to go find these
elsewhere, and we pin versions so behavior does not drift under us.

| File | What it is |
|------|-----------|
| [opensearch-endpoints.md](opensearch-endpoints.md) | The OpenSearch REST endpoint matrix we support, classified by handling, version-tracked |
| [fips-boundary.md](fips-boundary.md) | The FIPS compliance boundary artifact (CMVP cert, pinned versions, platforms, suites) |
| [observability-otel.md](observability-otel.md) | OpenTelemetry conventions we follow for span/trace export |

## How to use this folder

- When official upstream docs are consulted, capture the **relevant excerpt +
  the source URL + the version/date retrieved** here, rather than relying on a
  link that may rot or a model's memory that may be stale.
- Each derived table (e.g. the endpoint matrix) cites the upstream version it was
  derived from.
- Updating any spec here is a design-review event if it changes supported
  behavior (docs/10).

## To populate (action items)

These are placeholders to be filled with verified, version-pinned content during
M0–M1; they are listed as tasks in docs/11:

- [ ] OpenSearch REST API reference excerpts for every endpoint in the supported
      matrix, pinned to a target OpenSearch version.
- [ ] OpenSearch `_bulk` NDJSON format spec excerpt (action/source line grammar).
- [ ] OpenSearch query DSL `bool`/`filter` semantics excerpt (for the rewrite
      correctness argument).
- [ ] `_routing` and shard-allocation semantics excerpt.
- [ ] scroll & PIT lifecycle/TTL semantics excerpt.
- [ ] AWS-LC-FIPS CMVP certificate details (number, module version, tested
      configurations).
- [ ] aws-lc-rs `fips` feature build requirements.
- [ ] OpenTelemetry semantic-conventions version we target.
