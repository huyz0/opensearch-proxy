# ADR-011 — Traffic capture: tenant-agnostic tee, durable at-least-once, on-demand

**Status:** Accepted

## Context

Operators need a full-fidelity copy of the traffic the proxy handles — for
replay against a target cluster (migration, in the style of the OpenSearch
Migration Assistant), audit, or debugging. This is distinct from observability
(docs/05), which is deliberately **shape-only**: capture records real request and
response bodies, so it is a privileged stream with very different security and
cost properties.

Several choices had to be made: where capture sits relative to the request path;
whether it is best-effort or durable; whether it is a build-time, deploy-time, or
runtime decision; and how it stays free of the broker dependency in a default
binary.

## Decision

- **A tee behind a `Capture` seam, not a `Sink`.** Capture observes the exchange
  (request + upstream response) out of band; it never alters routing or the
  client's result. The seam (`osproxy-capture::Capture`, with `NoCapture`,
  `RedactingCapture`, `MemoryCapture`) has no broker dependency, so an external
  recorder can implement it. Contrast async fan-out (ADR-010), which *is* a write
  path (`WriteQueue`) and changes the response.
- **Tenant-agnostic and full-fidelity.** Capture records verbatim bytes; it does
  not apply tenancy rewrite or shape-only suppression. It is therefore privileged:
  off by default, and the `Authorization` header is redacted unless explicitly
  opted out (`capture_redact`, default on).
- **Two delivery tiers, one seam.** `capture_wal_dir` unset ⇒ bounded in-memory
  best-effort; set ⇒ a durable write-ahead log (`osproxy-kafka-wal`) that survives
  restart and replays until broker-acknowledged (**at-least-once**). A generic
  `wrap_capture` composes either over the same producer.
- **The sink and the switch are separate.** *Where* captured traffic goes (the
  broker/topic config) is independent of *when* to capture. The switch is either
  the `capture_default` baseline or a runtime `capture` diagnostics directive — so
  capture is turned on **on demand, fleet-wide, with no restart**, targeted by
  tenant/index and sampled, through the same control store as diagnostics.
- **Behind a `capture` cargo feature.** The default binary links no broker client;
  a configured capture on a binary built without the feature is a loud startup
  error, never a silent no-op.

## Why

- A tee keeps capture off the correctness path: it can never change what a client
  sees or where a write lands, so enabling it carries no routing risk.
- Durability is opt-in because migration/audit need at-least-once, but a debugging
  tee should not pay disk cost or block on a broker — the tier is the operator's
  call per deployment.
- On-demand-via-directive matches the blast-radius principle (ADR-012): capture is
  cost + data-sensitivity, both bounded and fail-safe (default off, redacted,
  sampled, TTL'd), so it is the *most* dynamic axis — safe to flip live.
- The feature gate keeps the standard artifact small and broker-free.

## Consequences

- The capture stream is privileged infrastructure: secure its broker/topic and
  keep `capture_redact` on unless the consumer is itself secured.
- At-least-once means a downstream replayer must tolerate duplicates (idempotent
  apply), the same contract as the async-fan-out op stream.
- Capture and fan-out are independent opt-ins (`capture` / `fanout` features,
  ADR-010); a deployment can run either, both, or neither.
- See docs/guide/08 (operation) and docs/guide/10 (where capture sits among modes).
