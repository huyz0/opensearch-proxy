# Choosing a mode

osproxy has a handful of independent "modes", tenanted vs tenant-agnostic, sync
vs async writes, capture on/off, FIPS or not. The thing to internalize first is
that **each one is selected at a different layer**: some are a build flag, some
are a startup config key, some are a per-request header, and one (capture) is a
live runtime control. Pick the wrong layer in your head and the knob seems to be
missing.

This page is the map. Each section links to the page with the full story.

## The map

| Axis | Where you set it | When it binds | Default |
|------|------------------|---------------|---------|
| **Tenanted vs tenant-agnostic** | Implement `TenancySpi` (code) → tenanted; `passthrough_cluster` + `passthrough_indices` (config) → agnostic, whole-instance or per-index | compile + startup | Tenanted (you must provide a tenancy) |
| **Sync vs async writes** | `fanout` feature (build) + `fanout_*` (config baseline) + `X-Write-Mode` (per request) | compile + startup + per-request | Sync |
| **Capture on/off** | `capture` feature (build) + `capture_*` (config) + `capture` diagnostics directive (runtime) | compile + startup + **runtime** | Off |
| **FIPS crypto** | `--no-default-features --features fips` (build), a separate artifact | compile | Non-FIPS (`ring`) |

Two rules of thumb fall out of this table:

- **A build flag is a property of the binary, not the deployment.** Async fan-out
  needs the `fanout` feature and capture needs the `capture` feature, each linking
  only the broker crates it uses (or `--features kafka` for both). A binary built
  without the relevant feature treats a configured `fanout_*`/`capture_*` as a
  *loud startup error*, never a silent no-op, so you find out at boot, not in
  production.
- **Tenanted is code; agnostic is config.** Going tenant-agnostic is a config key.
  Going tenanted means implementing `TenancySpi` and compiling it into your own
  binary (the shipped binary's `ReferenceTenancy` is a sample, not a product). If
  you are evaluating osproxy, budget for "build a binary," not "edit a file."

## Tenanted vs tenant-agnostic (and both at once)

Tenanted is the point of osproxy: a request names a *logical* index, the tenancy
resolves a partition and a placement, and the body/query are rewritten so a tenant
sees only its own data. You get this by implementing [`TenancySpi`](05-spi-guide.md).

Tenant-agnostic (passthrough) forwards a request verbatim to one cluster with no
rewrite, a transparent / capture / migration proxy. Set `passthrough_cluster` +
`passthrough_endpoint`.

**One proxy can do both at once.** Add `passthrough_indices` (a comma-separated
list of logical-index prefixes) and *only* those indices pass through verbatim,
while every other index stays fully tenanted. This is the migration shape: a
not-yet-onboarded legacy index flows through untouched while onboarded indices are
isolated, on the same instance. It is **fail-closed** (a non-match keeps tenancy)
and keyed on the operator's index list only, never a client header, so a client
cannot opt itself out of isolation. See [Overview → Tenant-agnostic mode](01-overview.md#tenant-agnostic-mode).

```ini
# Tenant some indices, pass legacy ones through verbatim, one instance.
passthrough_cluster  = legacy-1
passthrough_endpoint = https://legacy-1.internal:9200
passthrough_indices  = legacy-, archive_
```

## Sync vs async writes

Sync is the default and the honest one: a write returns OpenSearch's real result.
Async durably enqueues the write to Kafka and returns `202` + an `op_id`; a
downstream component applies it. Async needs the `fanout` feature built in and
`fanout_*` configured.

The key design choice: **async is negotiated per request** with `X-Write-Mode:
async|sync` over a deployment baseline (`fanout_async_default`). That is the right
granularity, async changes the consistency contract (no read-after-write), so a
client opts in rather than having it flipped on underneath it. See
[Async Fan-out Clients](09-async-clients.md).

## Capture on/off

Capture tees a full-fidelity copy of each exchange to a stream for replay. It is
the most dynamic axis on purpose: built in with the `capture` feature, pointed at a
topic with `capture_*`, and then **turned on at runtime, fleet-wide, with no
restart** via a `capture` diagnostics directive, targeted by tenant/index, sampled,
and TTL'd. It is observability, not a correctness boundary, so it is safe to flip
live; the default is off and the stream is redacted. See
[Observability](08-observability.md).

## FIPS

FIPS is a *separate build artifact*, not a runtime switch: build with
`--no-default-features --features fips` to link the validated `aws-lc-rs` provider
instead of `ring`. See [the FIPS boundary](../specs/fips-boundary.md).

## Why not one big "make it all dynamic" switch?

Deliberately, the dynamism of each axis is rationed by its blast radius. Capture is
fully runtime-dynamic because a wrong setting is bounded and fail-safe. Async is
per-request because flipping it fleet-wide would silently change clients'
consistency guarantees. Tenant isolation is a per-request, fail-closed *routing*
decision, never a global mutable "isolation off" bit, because a wrong value there
leaks data silently and irreversibly. Composability lives in one per-request
decision over operator config, not a pile of global mode flags.
