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

📖 **Documentation site: https://huyz0.github.io/opensearch-proxy/**

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

## Building & development setup

### Required tools

| Tool | Why | Needed for |
|------|-----|------------|
| Rust (stable, see `rust-toolchain`) | builds the workspace | always |
| `protoc` (Protocol Buffers compiler) | gRPC ingress codegen (`tonic-prost-build`) | always |
| `cmake` + a C compiler (`cc`/`gcc`/`clang`) + `go` | builds AWS-LC-FIPS (the validated crypto module) | **FIPS builds only** |
| Docker | the `--ignored` testcontainer suite (real OpenSearch) | optional |

The **default (non-FIPS) build needs no native toolchain** beyond `protoc` — the
crypto provider is pure-Rust `ring`. `cmake`/C/Go are required *only* for a FIPS
build, because the FIPS crypto module compiles AWS-LC from C.

### Install (Debian / Ubuntu)

```sh
# Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Always-required: protobuf compiler
sudo apt-get update && sudo apt-get install -y protobuf-compiler

# FIPS builds only: cmake + C toolchain + Go
sudo apt-get install -y cmake build-essential golang-go
```

### Install (macOS, Homebrew)

```sh
# Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Always-required: protobuf compiler
brew install protobuf

# FIPS builds only: cmake + Go (Xcode CLT provides the C compiler)
xcode-select --install   # if you don't already have the C toolchain
brew install cmake go
```

### Build modes (crypto provider selected at build time)

The crypto provider is chosen by a **mutually-exclusive build feature**, so a
FIPS artifact never links a non-validated crypto crate — it is a *separate
compiled binary*, not a runtime switch (ADR-009, [docs/07](docs/07-fips-and-crypto.md)):

```sh
# Dev / non-FIPS (default): pure-Rust ring provider, no native toolchain.
cargo build -p osproxy
cargo xtask ci            # fmt, clippy, arch graph, tests, docs, budgets

# FIPS release artifact: aws-lc-rs FIPS module (requires cmake + C + Go above).
cargo build -p osproxy-server --release --no-default-features --features fips

# Build + test the FIPS feature (skips with a warning if the toolchain is absent).
cargo xtask check-fips
```

> **FIPS toolchain note:** AWS-LC-FIPS's integrity transform (`delocate`) only
> supports specific compiler versions; a bleeding-edge `gcc` (e.g. 15) can fail
> the FIPS build at `-O3`. CI pins the image for this reason — see
> [docs/specs/fips-boundary.md](docs/specs/fips-boundary.md) §4. Do not inject
> `CFLAGS` to work around it; that would alter the validated build.

Enabling both (or neither) provider feature is a compile error by design. The
`--ignored` integration tests need Docker:

```sh
cargo test --workspace -- --ignored
```

## Documentation

**New to osproxy?** Read the **[User Guide](https://huyz0.github.io/opensearch-proxy/)**
(rendered site, or the [source in `docs/guide/`](docs/guide/README.md)). It walks
through the intent, requirements, architecture with diagrams, the SPI, a full wiring
example, configuration, and observability.

The numbered docs below are the design source of truth; read them in order for the
deeper rationale.

| Doc | Purpose |
|-----|---------|
| **[docs/guide/](docs/guide/README.md)** | **User Guide** — overview, NFRs, architecture, components, SPI, wiring, configuration, observability |
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
| [docs/12-quality-system.md](docs/12-quality-system.md) | Two-tier quality: deterministic gates + LLM semantic review |
| [docs/specs/](docs/specs/) | Vendored external specs & references (OpenSearch API, FIPS, OTel) |

## Status

Design phase. No code yet. The first vertical slice is defined in
[docs/11-roadmap.md](docs/11-roadmap.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE).
