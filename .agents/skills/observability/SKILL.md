---
name: observability
description: "WHAT: Shape-only causal traces, runtime diagnostics directives, /debug/explain, and the no-value-leak rule. USE WHEN: editing osproxy-observe, adding a span/trace attribute, a new failure mode, or anything that logs."
---

# Observability (LLM-debuggable, security-aware)

A failure must be diagnosable by an LLM from telemetry alone — no source reading,
no human gathering context. Observability is **read-only**: it never mutates
routing or cluster state.

## Rules

- **Shape-only, by construction.** Spans/logs carry shapes, ids, field *names*,
  sizes, counts — **never** tenant values, document bodies, query literals,
  tokens, or credentials. The trace API types make a value-leak unrepresentable;
  this is not after-the-fact redaction.
- **Every failure is self-describing.** Errors attach `ErrorContext` (code,
  decision chain, retryable, remediation) to the failing span and surface in
  `/debug/explain/{request_id}`.
- **Runtime-togglable, targeted, TTL-bounded.** Verbosity is data (a directive),
  not a code path — flip it without restart, scoped by tenant/index/principal/
  endpoint, auto-expiring. Two channels: signed `X-Debug-Directive` header
  (surgical) and control-plane directive (fleet). "Off" cost is near-zero.

## Enforced by

- **No-value-leak test**: fuzz documents/queries with canary secrets, assert they
  never appear in any emitted telemetry or `/debug/explain` (docs/09 §2.7).
- **Blind-diagnosis test**: telemetry alone must identify stage, cause, decision
  chain, retryable, and remediation for each catalogued failure (docs/09 §3).
- New failure mode → extend both tests in the same change.

## Deep dive

[docs/05-observability.md](../../../docs/05-observability.md),
[docs/specs/observability-otel.md](../../../docs/specs/observability-otel.md),
ADR-005.
