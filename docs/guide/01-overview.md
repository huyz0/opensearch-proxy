# 1. Overview & Intent

## The goal in one sentence

Route each OpenSearch request to the correct physical placement based on a pluggable,
partition-based policy, run as a low-latency Rust library, with observability built
for LLM-driven debugging and a FIPS-capable crypto build.

## The problem

You run OpenSearch for many tenants (customers, teams, regions) and you want each
tenant to address a single logical index like `orders` or `logs`. Underneath, their
data is placed by a policy you control. A tenant might live in a shared index (many
tenants in one index, isolated by an injected field plus a mandatory query filter),
in a dedicated index on a shared cluster, or on a dedicated cluster of its own.

You also want to move a tenant from one placement to another without losing or
corrupting writes during the cutover, apply auth, TLS, and telemetry uniformly at one
boundary, and debug a routing problem without SSHing into a box to read code.

osproxy is that boundary.

## What it does

Every request routes to exactly one physical placement based on a partition key. On
ingest the proxy injects partition fields and constructs the document `_id` (and
`_routing`); on read it adds a mandatory partition filter and strips the injected
fields back out. The two halves are provably symmetric, verified by round-trip
property tests, so what goes in transformed comes back out clean and isolation cannot
be bypassed by a client-supplied query.

A single mixed-partition `_bulk` body is split into per-placement sub-batches,
dispatched concurrently, and the `items[]` are re-interleaved in the original order.
Connections are pooled on both sides: client keep-alive downstream, per-cluster pools
with TLS session reuse upstream.

The proxy authenticates clients (mTLS and token) and can authorize the resolved
action; the client's `Authorization` is consumed at the edge and never reaches the
upstream or the trace. Partition migrations are gated by an epoch so a write that
resolved against a stale placement is rejected rather than misrouted. And every
request can emit a structured, security-aware causal trace that an LLM can read,
togglable at runtime across the fleet without a restart.

```mermaid
%%{init: {'theme':'base','themeVariables':{'primaryColor':'#e6f4ea','primaryTextColor':'#0b1f33','primaryBorderColor':'#188038','lineColor':'#5f6368','fontSize':'14px'}}}%%
flowchart TB
    subgraph ingest["Write path (ingest)"]
        direction LR
        wi["client doc<br/>{tenant_id, …}"] --> wj["inject partition field<br/>construct _id / _routing"] --> wk[("physical write")]
    end
    subgraph read["Read path (search / get)"]
        direction LR
        ri["client query"] --> rj["wrap in mandatory<br/>partition filter"] --> rk["fetch"] --> rl["strip injected<br/>fields from hits"] --> rm["clean logical view"]
    end

    classDef step fill:#e6f4ea,stroke:#188038,stroke-width:1.4px,color:#0b1f33;
    classDef store fill:#fef7e0,stroke:#f9ab00,stroke-width:1.4px,color:#3c2a00;
    class wi,wj,ri,rj,rk,rl,rm step;
    class wk store;
```

## What it does not do (v1)

These are deliberate cuts, each with a home elsewhere.

| Non-goal | Why, or where it goes |
|----------|---------------------|
| Synchronous fan-out / scatter-gather search | Search is always single-cluster. A partition lives in one place. ([ADR-002](../decisions/002-single-target-search.md)) |
| Cross-cluster result/agg merge, cross-cluster scoring | Removed by single-target search. |
| Synchronous dual/triple-write redundancy | Deferred to a future queue-based (Kafka) plus pull-ingester mode behind the `Sink` trait. |
| Copying partition data during migration | External reindex/snapshot tooling does the copy; the proxy only gates the routing flip. |
| Dynamic plugin loading (WASM/dylib) | The SPI is compiled in statically. ([ADR-007](../decisions/007-static-spi.md)) |
| The proxy mutating cluster state via AI | Observability is read-only. The AI observes; humans or automation act. ([ADR-005](../decisions/005-readonly-ai-observability.md)) |

## Who uses it, and how

Three jobs drove the design. Logical-index tenancy, where clients address logical
indices and the proxy resolves the physical cluster and index from a partition key.
Interception, where profiling, telemetry, and auth apply uniformly to all traffic at
the proxy boundary. And operational agility, where partitions migrate between
placements with the proxy guaranteeing write correctness across the cutover.

You consume it by depending on `osproxy-spi`, implementing a `TenancySpi` (and
optionally an `Authenticator`, `Authorizer`, custom `Sink`, or `Router`), and
assembling the pipeline, handler, and ingress. See [The SPI](05-spi-guide.md) and
[Wiring It Together](06-wiring-example.md).

## Tenant-agnostic mode

osproxy also runs without tenancy. Set `passthrough_cluster` (+ `passthrough_endpoint`)
and the proxy forwards every request verbatim to one cluster with no partition
resolution, no body rewrite, and no isolation. On its own that is a plain reverse proxy
with osproxy's auth, TLS, pooling, and observability.

**One proxy, both modes.** Add `passthrough_indices` (a comma-separated list of
logical-index prefixes) and *only* those indices pass through verbatim — every other
index stays fully tenant-isolated. This is the migration shape: a not-yet-onboarded
(legacy) index flows through untouched while the indices you have onboarded are
tenanted, on the same instance, with no second deployment. The match is per request,
**fail-closed** (an index that does not match keeps tenancy), and keyed on the
operator-configured index list only — never a client header — so a client cannot opt
itself out of isolation.

Pair it with the `Capture` seam and you get a capture proxy in the style of the
[OpenSearch Migration Assistant](https://docs.opensearch.org/latest/migration-assistant/architecture/):
forward to the source cluster while teeing the raw request and response to a durable
stream (a queue) for later replay against a target. Capture is off by default and
records full-fidelity bodies, so the stream is privileged and you enable it
deliberately; redaction (dropping `Authorization`) composes in via `RedactingCapture`.
The queue writer itself is a separate `Capture` implementation.

→ [Requirements & NFRs](02-requirements-and-nfrs.md)
