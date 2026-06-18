# 01 — Architecture & Non-Functional Requirements

## 1. Language & runtime decision

**Rust.** Chosen for low footprint, predictable latency (no GC pauses), and a
type system that lets us make "every failure is a typed, contextual error" a
compile-time property rather than a convention.

**Go** was the fallback only if FIPS had no credible Rust path. It does — see
[07-fips-and-crypto.md](07-fips-and-crypto.md) — so Rust stands.

The SPI is **compiled in statically**. Implementers depend on `osproxy-spi` and
`impl` traits; there is no WASM, dylib, or runtime plugin discovery. Routing
logic is monomorphized into the binary.

## 2. Crate layout

A workspace of small, single-responsibility crates. The dependency direction is
strictly downward; `core` and `spi` have **zero I/O dependencies** so the public
surface an implementer compiles against is tiny and fast.

```
osproxy/
  crates/
    osproxy-core        # types, the request/decision model, the error taxonomy. No I/O.
    osproxy-spi         # public traits users implement. Depends only on core.
    osproxy-tenancy     # high-level TenancySpi -> implements low-level RoutingSpi.
    osproxy-transport   # h1/h2/grpc ingress, upstream pools, TLS, CryptoProvider.
    osproxy-engine      # the pipeline orchestration: auth -> resolve -> rewrite -> sink -> reverse.
    osproxy-rewrite     # NDJSON/bulk demux, query-DSL rewrite, response field strip.
    osproxy-sink        # Sink trait + OpenSearchSink (Kafka/QueueSink later).
    osproxy-control     # watched-store client: placement table + diagnostics directives, epochs.
    osproxy-observe     # tracing layers, directive evaluation, /debug/explain assembly.
    osproxy-config      # typed config load/validate (figment/serde), no business logic.
    osproxy-server      # the binary. Wires everything; owns main(), signals, lifecycle.
  xtask/                # build/test/lint/coverage automation (cargo xtask ...).
  docs/
```

**Dependency rules (enforced in CI, see [08](08-engineering-standards.md)):**

- `core` depends on nothing in the workspace.
- `spi` depends only on `core`.
- `tenancy` depends on `core` + `spi`.
- No crate depends "upward" on `server`/`engine` except `server` itself.
- `rewrite`, `sink`, `transport`, `control` are siblings; they communicate only
  through `core` types and `spi` traits, never each other directly.

Rationale: this is the structural defense against a "god module." A change in
the bulk parser cannot reach into the TLS pool; both only see `core` types.

## 3. Component responsibilities (one job each)

| Crate | Single responsibility | Must NOT contain |
|-------|----------------------|------------------|
| `core` | Data model + error taxonomy | Any `async`, any socket, any serde-of-the-wire |
| `spi` | Trait contracts | Any concrete impl beyond trivial defaults |
| `tenancy` | Translate tenancy rules into routing decisions | Transport, pooling |
| `transport` | Bytes on/off the wire, TLS, pools | Routing decisions, tenancy semantics |
| `rewrite` | Body/query transforms | Network, placement lookup |
| `sink` | Deliver a write batch to a target | Routing decisions |
| `control` | Distribute placement table + directives, epochs | Request handling |
| `observe` | Emit/aggregate/serve diagnostics | Mutating any cluster or routing state |
| `engine` | Orchestrate the pipeline | Low-level wire or parsing details |
| `server` | Process lifecycle + wiring | Business logic |

## 4. Request pipeline (high level)

```
                 ┌──────────────────────── observe (spans, directive-gated) ───────────────────────┐
                 │                                                                                   │
client ──TLS──> ingress(transport) ──> auth(engine) ──> resolve(tenancy/spi) ──> rewrite ──> sink ──> upstream pool(transport) ──> cluster
                 │         h1/h2/grpc        mTLS+token       partition->placement   inject/   OpenSearchSink
                 │                                            (epoch-stamped)        construct/
                 │                                                                   query-filter
   client <──────┴───────────────────────── response: strip injected fields, re-interleave items[] ─┘
```

Detail in [04-request-pipeline.md](04-request-pipeline.md).

## 5. Non-Functional Requirements (NFRs)

These are **testable** targets. Where a number is a placeholder pending
benchmark calibration it is marked `[CALIBRATE]`; the *method* of measurement is
fixed even where the threshold is tuned later.

### 5.1 Performance & efficiency

| ID | Requirement | Measurement |
|----|-------------|-------------|
| NFR-P1 | Added p50 latency over direct-to-cluster ≤ `[CALIBRATE: target ~1–2ms]` for a pass-through (no body rewrite) request | Bench harness vs. baseline |
| NFR-P2 | Added p99 latency ≤ `[CALIBRATE]`; no tail amplification from pooling | Load test, steady state |
| NFR-P3 | Zero heap allocation on the pass-through hot path beyond unavoidable buffers; bulk rewrite streams without buffering the whole body | `dhat`/alloc counters in tests |
| NFR-P4 | Upstream TLS session reuse rate ≥ `[CALIBRATE: e.g. 99%]` under steady load | Pool metrics |
| NFR-P5 | Downstream keep-alive reuse honored; no per-request connection churn | Pool metrics |
| NFR-P6 | Idle memory footprint ≤ `[CALIBRATE]`; bounded by config, no unbounded buffers/queues | RSS under idle + soak |
| NFR-P7 | Bulk demux is O(body size) single-pass, no full-document re-serialization where avoidable | Bench + alloc profile |

### 5.2 Reliability

| ID | Requirement |
|----|-------------|
| NFR-R1 | **No panics** reachable from the request path. Enforced by `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]` on request-path crates; panics only allowed in startup/config with justification. |
| NFR-R2 | Every fallible operation returns a typed error from the taxonomy ([02](02-spi.md) §errors); no `anyhow`/string errors on the request path. |
| NFR-R3 | Backpressure: bounded queues everywhere; overload returns `429` with retry guidance, never OOM. |
| NFR-R4 | Upstream failures (timeout, connection reset, 5xx) are classified retryable/terminal and surfaced with the decision chain. |
| NFR-R5 | Graceful shutdown drains in-flight requests within a deadline; new requests rejected during drain. |
| NFR-R6 | No data corruption across partition migration (epoch gating — see [06](06-partition-migration.md)). |
| NFR-R7 | Survives fault-injection suite (slow/dropped upstreams, malformed bodies, partial writes) without panic or stuck request. |

### 5.3 Traceability / observability

| ID | Requirement |
|----|-------------|
| NFR-T1 | Every request emits one causal trace whose spans reconstruct *why* the request routed where it did, with **no source reading required** (verified by the "blind diagnosis" test, [09](09-testing-and-quality.md)). |
| NFR-T2 | Default verbosity emits **shapes, ids, and field names only** — never tenant values, document bodies, tokens, or credentials. |
| NFR-T3 | Verbosity is runtime-togglable fleet-wide **without restart**, targeted by tenant/index/principal/endpoint, with TTL auto-expiry. |
| NFR-T4 | `/debug/explain/{request_id}` returns the full decision chain as one LLM-consumable JSON document. |
| NFR-T5 | Every error carries: code, decision chain, `retryable`, and a remediation hint. |

### 5.4 Security

| ID | Requirement |
|----|-------------|
| NFR-S1 | TLS termination required for any request that is mutated (cannot rewrite an encrypted stream). Enforced at ingress on the endpoint classification, before dispatch — so it holds even in **tenant-agnostic passthrough** (docs/04 §10): a write to a tenancy-aware endpoint over cleartext is refused whether it is tenanted or forwarded verbatim. (Read-only admin/`Unknown` pass-through is unaffected.) |
| NFR-S2 | No secret/credential/token/tenant value in any log or trace at any verbosity level. Diagnostic capture is shape-only by construction. |
| NFR-S3 | Debug directives delivered via header are HMAC-signed; clients cannot self-enable expensive tracing. |
| NFR-S4 | Partition isolation enforced on the read path (query filter cannot be bypassed by client-supplied query) — see [03](03-tenancy-and-placement.md) §isolation for the isolation guarantee level. |
| NFR-S5 | FIPS build negotiates only FIPS-approved TLS versions and cipher suites. |

### 5.5 Maintainability / quality

| ID | Requirement |
|----|-------------|
| NFR-Q1 | No "god" file/module/type — size and cohesion budgets enforced in CI ([08](08-engineering-standards.md)). |
| NFR-Q2 | ≥90% semantic test coverage overall; SPI + routing core higher ([09](09-testing-and-quality.md)). |
| NFR-Q3 | Every public item (type, trait, fn) on the SPI surface has doc comments with intent, invariants, and an example. |
| NFR-Q4 | Public SPI changes require a design-review note ([10](10-review-process.md)). |

## 6. Configuration model

Typed, validated-at-startup config (`osproxy-config`). Layered: file →
environment → flags, with full validation before any socket opens. Invalid
config fails fast with a typed, human+LLM-readable error pointing at the bad
field. No business logic in config; it only produces validated value objects the
other crates consume. Hot-reloadable subset (pool sizes, directives, placement
table) goes through `osproxy-control`, not config-file reload.

## 7. Threading & async model

Tokio multi-threaded runtime. The request path is fully async and non-blocking;
any CPU-heavy transform (large bulk rewrite) is bounded and yields. No blocking
syscalls on runtime threads. Per-cluster upstream pools are sharded to avoid a
central lock becoming the bottleneck (a god-lock is the runtime analog of a god
module).
