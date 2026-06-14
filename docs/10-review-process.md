# 10 — Design & Code Review Process

The review process exists to keep the codebase coherent as it is built largely
by an LLM, and to keep humans out of the debugging loop by catching
traceability/quality gaps before merge.

## 1. Two kinds of review

### Design review (before code)

Required when a change touches: the SPI surface (docs/02), the placement/epoch
model (docs/03, 06), the observability schema (docs/05), the FIPS boundary
(docs/07), or any NFR target (docs/01).

A design-review note (a short markdown in `docs/decisions/NNN-title.md`) states:
problem, options, decision, why, and which invariants/NFRs are affected. This is
the lightweight ADR (Architecture Decision Record) trail so future readers
(human or LLM) can re-derive intent, never guess.

### Code review (per PR)

Every PR, against the checklist below.

## 2. Code review checklist

**Correctness**
- [ ] Behavior matches the relevant doc; deviations are documented and intended.
- [ ] All error paths return typed, contextual errors (no `unwrap`/`anyhow` on
      request path).
- [ ] Invariants touched (symmetry, isolation, epoch, order) have tests.

**Structure / quality**
- [ ] No god file/module/type; size & complexity budgets respected (or justified).
- [ ] Single responsibility per module; no `util`/`misc` dumping ground.
- [ ] Dependency direction respected (no upward deps).
- [ ] Newtypes for ids; `#[non_exhaustive]` where appropriate; no bare `unsafe`.

**Tests**
- [ ] Unit + property/integration as applicable; failure modes covered.
- [ ] Coverage thresholds hold; meaningful assertions (not just line coverage).
- [ ] Deterministic (no sleeps/wall-clock/network flakiness).

**Traceability / observability**
- [ ] New decision points emit shape-only span attributes.
- [ ] New failure modes appear in `/debug/explain` with remediation.
- [ ] Blind-diagnosis test updated if a new failure mode is introduced.

**Security**
- [ ] No value/secret can reach a log/trace (no-value-leak holds).
- [ ] Isolation preserved for any new endpoint handling.
- [ ] FIPS suite pinning unaffected (or boundary doc updated).

**Docs**
- [ ] Public SPI items documented with intent/invariants/example.
- [ ] Relevant `docs/` updated in the same PR.

## 3. Self-review by the implementing agent

Because development is LLM-driven, the implementing agent runs an explicit
self-review pass before opening a PR: re-read the touched docs, run the full CI
gate locally (`cargo xtask ci`), and write the PR description against this
checklist. A PR that cannot truthfully tick the boxes is not ready.

The repo's own `/code-review` and `/security-review` tooling are run on the diff
as an additional gate before merge.

## 4. Release gates

A release additionally requires:
- FIPS boundary doc current; CMVP cert + platform verified (docs/07 §5).
- Performance baselines met (NFR-P), not just functional tests.
- Blind-diagnosis suite green across the catalogued failure modes.
- No `// JUSTIFY` budget overrides without an accepted reason.

## 5. Decision log

`docs/decisions/` holds the ADRs. The major decisions already made (Rust;
single-target search; epoch-gated migration without in-path dual-write;
aws-lc-rs FIPS; read-only AI observability; isolation = filtered-or-rejected) are
backfilled there as ADR-001..00N so the rationale is permanent and greppable.
