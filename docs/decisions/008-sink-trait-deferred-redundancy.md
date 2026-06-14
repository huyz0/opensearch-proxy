# ADR-008 — Write `Sink` trait; queue-based redundancy deferred behind it

**Status:** Accepted

## Context

The user wants future dual/triple-write redundancy, but explicitly as a
**pull-based** design: the proxy writes to a queue (Kafka) and separate
pull-ingesters replicate into 1..N OpenSearch targets — not synchronous multi-write
in the proxy.

## Decision

The write path goes through a `Sink` trait. v1 ships `OpenSearchSink` (direct,
single target). The future redundancy mode is a `QueueSink` (Kafka) drop-in
behind the same trait. The same `RouteDecision`/placement feeds both; the routing
core does not change when redundancy is added.

## Why

- Keeps the synchronous path simple and single-target (ADR-002, ADR-003).
- Isolates "where writes go" from "how routing is decided" — a clean seam so
  redundancy is additive, not a core rewrite.
- Matches the user's pull-based redundancy intent (ingesters own replication).

## Consequences

- The `Sink` trait's contract (batch write, ack, typed `SinkError`) must be
  general enough for both a synchronous OpenSearch write and an async enqueue.
- Epoch stamping (ADR-003) lives at the `Sink` boundary so both sinks enforce it.
- Redundancy concerns (replica count, ordering, idempotency) are the ingester's,
  not the proxy's — documented when that mode is built.
