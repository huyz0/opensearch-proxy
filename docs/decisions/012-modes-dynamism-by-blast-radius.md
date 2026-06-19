# ADR-012 — Proxy modes: dynamism rationed by blast radius; isolation is routing, not a switch

**Status:** Accepted

## Context

osproxy accumulated several independent "modes": tenanted vs tenant-agnostic,
sync vs async writes, capture on/off, FIPS or not. A natural pull is to make all
of them maximally dynamic and runtime-configurable — "one proxy that can be
anything, flipped live." That pull is dangerous, because the modes do not carry
equal risk: some change operational behavior, others change a security or
correctness invariant. Treating them uniformly would put a fleet-wide,
silently-wrong kill switch next to a harmless tuning knob.

In particular, "serve tenanted and tenant-agnostic traffic from one proxy" (the
migration shape) could be built as a global `passthrough = on/off` bit. A single
mutable bit that disables tenant isolation fleet-wide — wrong value silently leaks
cross-tenant data, irreversibly — is the worst possible design.

## Decision

**Ration each mode's dynamism by its blast radius**, using three questions:
(1) does flipping it change a security/correctness invariant or just operational
behavior? (2) is a wrong setting loud or silent? (3) is it cheaply reversible?

- **Capture → fully runtime-dynamic** (ADR-011). Observability-class, bounded,
  fail-safe (default off, redacted, sampled, TTL'd). Flip it live via a directive.
- **Sync ↔ async → per-request only** (ADR-010), over a deploy baseline, via
  `X-Write-Mode`. Async changes the read-after-write contract, so a client *opts
  in*; it is never flipped on underneath a client by a global switch.
- **Tenant isolation → a per-request, fail-closed routing decision, never a
  global mutable bit.** Tenant-agnostic passthrough is selected per request by an
  **operator-configured logical-index prefix list** (`passthrough_indices`); a
  non-match keeps full tenancy. The decision is keyed on operator config only —
  never a client header — so a client cannot opt itself out of isolation, and the
  safe direction (more isolation) is the default for anything unmatched. An empty
  list means whole-instance passthrough (a deliberate transparent-proxy config),
  not an accident.
- **FIPS → a build-time artifact choice** (ADR-004/009), not a runtime switch.

The unifying rule: **composability lives in one per-request decision that is a
pure function of (request, operator config / current placement+directive state)**,
pushed behind the seams that already enforce fail-closed + epoch + signing — not a
pile of independent global mutable mode flags.

## Why

- A wrong isolation setting is silent and catastrophic; making it a fail-closed
  routing outcome means a typo under-exposes (more isolation), never over-exposes.
- The migration use case ("legacy index passes through, onboarded indices are
  tenanted, same instance") needs *per-request routing*, which the pipeline
  already does — not a new global mode. It composes with epoch-gated migration
  (ADR-003) for free.
- Per-request async keeps the proxy honest: it never silently changes a client's
  consistency guarantees.
- Every runtime toggle is control-plane attack surface and a multiplier on the
  test matrix; spending that complexity only where the dynamism is safe keeps both
  bounded.

## Consequences

- Passthrough is documented as a pipeline routing path (docs/04 §10), not a mode
  flag; its match list is operator config (docs/guide/07), reviewable in git.
- There is intentionally **no** runtime "isolation off" control and **no**
  fleet-wide async-baseline flip; an operator wanting those changes redeploys
  (config is enumerable and audited) or uses the per-request mechanisms.
- New mode-like features must declare where they sit on the blast-radius axis and
  justify any runtime dynamism against these three questions.
- See docs/guide/10 (the per-layer mode map) for the operator-facing view.
