---
name: quality-review
description: "WHAT: The LLM semantic/design review rubric for what linters can't judge. USE WHEN: reviewing a diff, finishing a feature, or running /code-review; complements the deterministic gates."
---

# Quality review (LLM semantic & design tier)

Tier 1 deterministic gates (docs/12) decide everything mechanical. This skill is
the **Tier 2** rubric: judgment about design and meaning that a linter cannot
see. Only review a **green** diff — fix mechanical failures first.

## Rubric

- **Altitude / abstraction.** Is logic at the right level and in the right crate
  per the `architecture` skill? Is a new seam justified, or is this leaking
  detail upward / reaching sideways?
- **Cohesion / god-detection.** Is each module/type doing one thing? A type
  accreting unrelated fields is a god-type the size budget won't catch — flag it.
- **Naming & intent.** Names reveal intent; code reads like its neighbours;
  newtypes for ids, not bare strings.
- **Error quality.** Failures are typed `ErrorContext` with a real decision chain
  and an *actionable* remediation — not a restated message.
- **Doc quality.** SPI docs state intent + invariants + an example, not a
  paraphrase of the signature.
- **Test meaningfulness.** Assertions constrain behavior (would they catch a real
  bug?), not just execute lines. Prefer a property test for an invariant.
- **Invariant adherence.** Respects the `AGENTS.md` invariants and the owning
  skill (tenancy/observability/spi/...).

## How to apply

- Produce findings as: location, which rubric item, why it matters, suggested
  fix. High-confidence findings gate; uncertain ones advise (say which).
- If a finding is a recurring, checkable rule, propose graduating it to a Tier 1
  gate (a lint or xtask check) instead of re-reviewing it forever.

## Enforced by

- The **`quality-reviewer` subagent** (`.claude/agents/quality-reviewer.md`),
  spawned via the Task tool before finishing a unit of work — this is the primary
  Tier 2 mechanism (no CI secret, runs in the agent itself).
- The **`/quality-review`** command (`.claude/commands/quality-review.md`) to run
  it on demand against the current diff or a range.
- The repo `/code-review` and `/security-review` commands as additional passes.

## Deep dive

[docs/12-quality-system.md](../../../docs/12-quality-system.md) §Tier 2,
[docs/10-review-process.md](../../../docs/10-review-process.md).
