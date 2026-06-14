# osproxy — OpenSearch Routing Proxy

A high-performance, low-footprint, low-latency proxy for routing OpenSearch
requests. Accepts HTTP/1.1, HTTP/2, and gRPC over cleartext or TLS (with an
optional **FIPS 140-3** validated crypto build), and routes each request to the
correct physical OpenSearch cluster/index based on a pluggable **placement
policy** keyed on a **partition** (tenant) concept.

It is designed to be consumed **as a library**: implementers depend on the
`osproxy-spi` crate, `impl` a small set of traits, and compile their routing
logic statically into the proxy. No dynamic plugin loading (no WASM, no
dylibs).

## What it does

- **Routes** every request to exactly one physical placement (dedicated
  cluster, dedicated index, or shared index) based on a partition key.
- **Injects** partition id / synthetic fields and **constructs `_id`** on
  ingest; **filters** by partition and **strips** injected fields on
  query/search — so each tenant sees a clean logical view.
- **Pools** connections on both the downstream (client) and upstream (cluster)
  sides, reusing TCP and TLS sessions.
- **Authenticates** clients (mTLS + token) and manages upstream credentials.
- Is built to be **observed and debugged by an LLM** with no human source-diving
  required — structured, causal, security-aware traces, togglable at runtime.

## What it explicitly does *not* do (v1)

- **No synchronous fan-out / scatter-gather.** Every search resolves to a single
  cluster. A partition's data normally lives in one place.
- **No synchronous multi-write redundancy.** Dual/triple-write redundancy is a
  *future* mode built on a queue (Kafka) + pull-based ingesters, not the
  synchronous path.

## Documentation

Read the docs in order; they are the source of truth for the design.

| Doc | Purpose |
|-----|---------|
| [docs/00-goals.md](docs/00-goals.md) | Project goal, scope, non-goals, success criteria |
| [docs/01-architecture.md](docs/01-architecture.md) | Architecture, crate layout, **non-functional requirements** |
| [docs/02-spi.md](docs/02-spi.md) | **SPI reference** — the public traits, heavily documented |
| [docs/03-tenancy-and-placement.md](docs/03-tenancy-and-placement.md) | Partition model, placement table, epochs |
| [docs/04-request-pipeline.md](docs/04-request-pipeline.md) | Ingest demux, query rewrite, field strip, affinity |
| [docs/05-observability.md](docs/05-observability.md) | Diagnostics directives, span schema, `/debug/explain` |
| [docs/06-partition-migration.md](docs/06-partition-migration.md) | Epoch-gated migration contract |
| [docs/07-fips-and-crypto.md](docs/07-fips-and-crypto.md) | FIPS path, aws-lc-rs, compliance boundary |
| [docs/08-engineering-standards.md](docs/08-engineering-standards.md) | Code structure, no-god-module rules, folder layout |
| [docs/09-testing-and-quality.md](docs/09-testing-and-quality.md) | Test strategy, **≥90% semantic coverage**, test quality |
| [docs/10-review-process.md](docs/10-review-process.md) | Design & code review gates |
| [docs/11-roadmap.md](docs/11-roadmap.md) | Milestones and the first vertical slice |
| [docs/specs/](docs/specs/) | Vendored external specs & references (OpenSearch API, FIPS, OTel) |

## Status

Design phase. No code yet. The first vertical slice is defined in
[docs/11-roadmap.md](docs/11-roadmap.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE).
