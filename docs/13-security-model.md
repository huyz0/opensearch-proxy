# 13 — Security Model

osproxy sits on the data path of a multi-tenant store, so its security posture is
a first-class design concern, not an add-on. This doc consolidates the threat
model and names where each control lives; the controls themselves are specified
in the docs referenced inline. The default posture is **fail-closed**: the safe
direction is the default, and an unsafe configuration is a loud error, not a
silent downgrade.

## 1. Actors and trust boundaries

| Actor | Trust | Boundary |
|-------|-------|----------|
| **Tenant client** | Untrusted | Authenticated at ingress; may send adversarial queries/bodies. |
| **Operator** | Trusted | Owns config (file/env/flags) and the admin token; config is reviewed in git. |
| **Control plane** | Semi-trusted | Publishes directives; gated by a bearer token and (for the header channel) HMAC signatures. |
| **Upstream cluster(s)** | Trusted infra | Reached over pooled connections; endpoints come from the tenancy, never client input. |
| **Capture / fan-out broker** | Privileged sink | Carries verbatim bodies (capture) or resolved ops (fan-out); operator-secured. |

## 2. Threats and controls

### T1 — Cross-tenant data leak (the headline threat)
- **Read isolation is filter-or-reject** (ADR-006, NFR-S4): every shared-index
  query is wrapped `bool { must: [client_query], filter: [partition term] }`; the
  client query is nested and cannot remove the filter. Endpoints that can't be
  provably filtered are rejected, not best-effort.
- **By-id collisions are fail-closed**: in `SharedIndex` the partition id is
  **mandatory** in the constructed `_id`; a missing/partition-free id rule is
  rejected in the router (docs/03), so by-id reads/writes can't collide across
  tenants.
- **Tenant-agnostic passthrough cannot be client-triggered**: it is selected by an
  operator-configured index-prefix list only, never a client header, and a
  non-match keeps full tenancy (ADR-012, docs/04 §10).
- **Search is single-target** (ADR-002): no cross-cluster fan-out that could merge
  another tenant's results.
- Enforced by adversarial bypass tests (nested bool/`should`/scripts/`_sql`) and a
  round-trip symmetry property (docs/09 §2.7).

### T2 — Secret / value leak through telemetry
- **No-value-leak by construction** (NFR-S2, ADR-005, docs/05 §7): the trace API
  only accepts shape/id/name types in value-bearing positions, so a document
  value, query literal, or credential cannot reach a log/span at any verbosity.
- **`sensitive_fields`** (SPI) is deny-by-default; observability never captures
  declared-sensitive values.
- Enforced by a static check + a canary "no value leaks" fuzz test.

### T3 — Credential exposure on the wire
- **TLS-for-mutation** (NFR-S1): a write to a tenancy-aware endpoint over cleartext
  is refused at ingress (classification-based, before dispatch), including in
  passthrough. The admin publish path enforces the same.
- **mTLS** optional via `tls_client_ca`; the crypto module is FIPS-selectable at
  build time (ADR-004/009, docs/07).
- **Bearer tokens are compared in constant time** and **stripped before the
  pipeline/telemetry** (`osproxy-server::bearer`), so a token never reaches a log
  or upstream.

### T4 — Unauthorized control-plane mutation
- **`POST /admin/directives` is token-gated** (bearer, constant-time) and **refused
  over cleartext** so the token isn't exposed; `GET` introspection is at parity.
- **The publish decoder rejects unknown keys** (`directives_api`), so a misspelled
  tenant/index can't silently widen a directive's blast radius fleet-wide.
- **The `X-Debug-Directive` header channel is HMAC-signed** (constant-time verify,
  clock-enforced expiry, fails closed); unset key ⇒ the channel rejects everything.
- **Cursor affinity envelopes are HMAC-signed** so a continued scroll/PIT can't be
  redirected to another cluster by tampering (docs/03 §6).

### T5 — Blind / unsafe routing
- The sink has **no static endpoint catalog**; every upstream URL comes from the
  tenancy's placement or `cluster_endpoint`. An unknown cluster ⇒ fail closed, not
  a blind connection.
- **Epoch-gated migration write gate** (ADR-003): a write resolved at a stale epoch
  is rejected (retryable), so a placement flip mid-flight can't double-apply.

### T6 — Privileged stream exposure (capture / fan-out)
- **Capture is off by default, redacts `Authorization`** by default, and is
  full-fidelity — so it is treated as privileged infrastructure the operator
  secures (ADR-011). Turning it on is operator-gated (baseline or signed directive).
- **Fan-out** carries resolved ops, not credentials; the queue is operator-secured
  and TLS/mTLS-capable like capture.

## 3. Posture invariants

- **Fail-closed everywhere**: unmatched passthrough → tenancy; unknown cluster →
  reject; missing HMAC key → reject; unknown config/ directive key → error;
  unresolved partition → reject.
- **No runtime control weakens isolation**: there is deliberately no runtime
  "isolation off" switch and no fleet-wide async-baseline flip (ADR-012). Dynamism
  is rationed by blast radius — only observability-class capture is runtime-flippable.
- **Loud over silent**: a misconfiguration (e.g. fan-out configured without the
  `fanout` feature, cleartext mutation) is a startup error or a typed rejection,
  never a silent downgrade.

## 4. What is out of scope

- Authentication *identity* providers (the `Authenticator` SPI is the seam; the
  proxy ships a reference token map, not an IdP).
- Authorization policy beyond the `Authorizer` seam.
- Securing the operator's control store, broker, and upstream clusters — those are
  the operator's infrastructure, reached through documented seams.
- DoS/rate-limiting (handled upstream / at the edge), beyond the bounded-memory
  and circuit-breaker reliability controls (NFR-R, docs/04 §7).
