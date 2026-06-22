# ADR-005: Observability is read-only and shape-only

**Status:** Accepted

## Context

A first-class requirement: develop with AI/LLMs such that a human never has to
drop in to debug. The user clarified the AI should **observe** to debug, not
**change** things, and to be careful about security (sensitive data).

## Decision

Observability is **read-only**: the AI/LLM consumes rich, causal telemetry to
diagnose; it never mutates routing, placement, or cluster state. The control
plane (migrations, pool sizing) is operator/automation driven.

Telemetry is **shape-only by construction**: spans carry shapes, ids, field
names, sizes, counts, never tenant values, document bodies, query literals,
tokens, or credentials. The trace API types make a value-leak unrepresentable,
rather than relying on after-the-fact redaction.

Verbosity is runtime-togglable without restart, **targeted** (tenant/index/
principal/endpoint) and **TTL-bounded** so detail is cheap-when-off and cannot
silently bleed cost.

## Why

- "No human gathers context" requires telemetry rich enough to diagnose from
  alone, verified by the blind-diagnosis test (docs/09 §3).
- Security/PII and cost constraints forbid value capture by default; making leaks
  unrepresentable is stronger than redaction.
- Targeting + TTL is the cost lever satisfying the low-cost requirement.

## Consequences

- `/debug/explain/{request_id}` assembles the causal story for LLM consumption.
- Two directive channels: signed header (surgical) + control-plane (fleet),
  docs/05.
- A "no value leaks" test (canary secrets) gates merge.
- Some failures needing a literal value to diagnose use the explicit,
  short-lived, single-instance ring-buffer break-glass, never default, never
  persisted.
