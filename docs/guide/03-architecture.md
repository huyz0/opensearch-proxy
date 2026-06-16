# 3. Architecture

osproxy is a statically-linked Rust library and binary. You implement the SPI; the
engine runs each request through a fixed pipeline. Nothing on the hot path is
dynamically dispatched. Your `TenancySpi` and `Sink` are monomorphized into the
pipeline at compile time.

## Request lifecycle

```mermaid
%%{init: {'theme':'base','themeVariables':{'primaryColor':'#e8f0fe','primaryTextColor':'#0b1f33','primaryBorderColor':'#1a73e8','lineColor':'#5f6368','fontSize':'13px'}}}%%
flowchart TB
    A["Client request"] --> B["Ingress (osproxy-transport)<br/>parse · classify · admission · TLS"]
    B --> C{"Pre-auth route?<br/>/metrics · /debug/* · /admin/*"}
    C -- yes --> C1["Serve introspection/admin<br/>(short-circuit)"]
    C -- no --> D["TLS gate (NFR-S1)<br/>refuse mutating cleartext"]
    D --> E["Authenticate<br/>(Authenticator: mTLS + token)"]
    E --> F["Authorize<br/>(Authorizer, default allow-all)"]
    F --> G["Resolve<br/>(Router → TenancyRouter)<br/>partition → placement → target"]
    G --> H["Write gate (epoch)<br/>admit / stale-epoch 409"]
    H --> I["Transform (osproxy-rewrite)<br/>inject + construct id / filter + strip"]
    I --> J["Dispatch (osproxy-sink)<br/>per-cluster pool · TLS reuse"]
    J --> K["Reverse-transform<br/>strip injected fields from response"]
    K --> L["Response to client<br/>+ x-request-id"]

    M["Trace recorder (osproxy-observe)"] -. "shape-only spans<br/>every stage" .- G
    M -. "/debug/explain · OTLP · logs" .- L

    classDef step fill:#e8f0fe,stroke:#1a73e8,stroke-width:1.3px,color:#0b1f33;
    classDef gate fill:#fef7e0,stroke:#f9ab00,stroke-width:1.3px,color:#3c2a00;
    classDef obs fill:#f3e8fd,stroke:#a142f4,stroke-width:1.3px,color:#2a0b3c;
    class A,B,E,F,G,I,J,K,L,C1 step;
    class C,D,H gate;
    class M obs;
```

A few things are worth understanding about this flow.

The introspection surfaces (`/metrics`, `/debug/*`, `/admin/directives`) short-circuit
before authentication, and each is individually gated (see
[Observability](08-observability.md)). Everything below them is the data plane.

The TLS gate is a hard rule (NFR-S1): a body-mutating request over cleartext is refused
with `403` before any work happens. You cannot rewrite an encrypted stream, so the
proxy has to terminate TLS to do tenancy at all.

Credentials are consumed at the edge. The `Authenticator` reads the client
`Authorization`, then the handler strips it before the request enters the pipeline, so
it never reaches the engine, the upstream, observability, or logs.

Resolution is partition-first. The `Router` turns the request into a `(partition,
placement, target)` triple plus a body transform. The engine needs the partition (not
just a routing decision) to construct ids and demux bulk, which is why it consumes the
richer result. During a migration the write gate re-checks the epoch at dispatch and
rejects a write that resolved against a now-stale placement as a retryable `409`.

Around all of it, the trace recorder emits shape-only spans for every request, success
or failure.

## The two body transforms

```mermaid
%%{init: {'theme':'base','themeVariables':{'primaryColor':'#e6f4ea','primaryTextColor':'#0b1f33','primaryBorderColor':'#188038','lineColor':'#5f6368','fontSize':'13px'}}}%%
flowchart LR
    subgraph W["Ingest (shared index)"]
        direction TB
        w1["client body<br/><code>{tenant_id:'acme', amount:42}</code>"]
        w2["inject partition field<br/><code>_tenant:'acme'</code>"]
        w3["construct id<br/><code>acme:1</code> + routing <code>acme</code>"]
        w1 --> w2 --> w3
    end
    subgraph R["Search (shared index)"]
        direction TB
        r1["client query<br/><code>{match:{amount:42}}</code>"]
        r2["wrap: bool.filter<br/><code>{term:{_tenant:'acme'}}</code>"]
        r3["strip <code>_tenant</code> from hits"]
        r1 --> r2 --> r3
    end

    classDef step fill:#e6f4ea,stroke:#188038,stroke-width:1.3px,color:#0b1f33;
    class w1,w2,w3,r1,r2,r3 step;
```

The partition filter is a **structural enclosure**: your query becomes the `must`
clause inside a `bool` that the proxy controls, with the partition `term` as a
mandatory `filter`. A client cannot remove or escape it (NFR-S4). For shared-index
placements the partition id is also mandatory in the document id template, so by-id
reads and writes can't collide across tenants. The router fails closed if a
shared-index placement lacks a partition-scoped id.

## Placement kinds

```mermaid
%%{init: {'theme':'base','themeVariables':{'primaryColor':'#e8f0fe','primaryTextColor':'#0b1f33','primaryBorderColor':'#1a73e8','lineColor':'#5f6368','fontSize':'13px'}}}%%
flowchart TB
    P["partition (tenant)"] --> Q{"placement kind"}
    Q --> S["SharedIndex<br/>cluster + index + inject<br/>isolate by field + filter + id"]
    Q --> D["DedicatedIndex<br/>cluster + physical index<br/>isolate by index"]
    Q --> C["DedicatedCluster<br/>cluster, logical index kept<br/>isolate by cluster"]

    classDef step fill:#e8f0fe,stroke:#1a73e8,stroke-width:1.3px,color:#0b1f33;
    class P,S,D,C step;
    classDef dec fill:#fef7e0,stroke:#f9ab00,stroke-width:1.3px,color:#3c2a00;
    class Q dec;
```

See [Tenancy & Placement](../03-tenancy-and-placement.md) for the full model and
[Partition Migration](../06-partition-migration.md) for epoch-gated cutover.

## Configuration model

Configuration is typed and **fully validated at startup, before any socket opens**.
It is layered with the precedence **file < environment < flags**; an invalid value is
a typed error naming the field. Live, fleet-wide changes (placement table,
diagnostics directives) flow through the **control plane** at runtime, not via
config-file reload. See [Configuration](07-configuration.md) and
[Observability & Control Plane](08-observability.md).

→ [Components (Package View)](04-components.md)
