---
name: manage-skills
description: "WHAT: Governance for this repo's agent skill system. USE WHEN: creating or editing any file under .agents/skills/, or changing AGENTS.md."
---

# Managing skills

The skill system makes osproxy AI-native: an agent finds the right rule by
*trigger*, not by reading the whole repo. This skill governs that system.

## Rules

- **Pointer-only, not a rulebook.** A `SKILL.md` routes to the deep-dive in
  `docs/` and names the gate that enforces it. It must stay **under 100 lines**
  and must not duplicate doc content — on conflict, the doc wins; fix the drift.
- **Frontmatter is the trigger.** Exactly two keys: `name` and `description`.
  The description MUST follow `WHAT: <summary>. USE WHEN: <concrete triggers>` —
  triggers are specific crate names, file paths, commands, or doc sections, not
  vague topics. This is the only place triggers live (no separate "trigger"
  section in the body).
- **Bind to an enforcer.** Every skill names the mechanical check that proves
  its rule (a `cargo xtask` subcommand, a clippy lint, a CI job, or an ADR).
  If a rule cannot be mechanically checked, say so explicitly.
- **Self-maintenance.** A change that alters a skill's subject MUST update the
  skill in the same change. Doc/skill drift is a bug.

## Enforced by

- `cargo xtask skills` — checks every `SKILL.md` is ≤100 lines and its
  frontmatter matches the `WHAT:/USE WHEN:` pattern. Part of `cargo xtask ci`.

## Layout

```
.agents/skills/<name>/SKILL.md   # one skill, one responsibility
.agents/skills/<name>/scripts/   # optional helper scripts
```

Deep detail lives in `docs/`; this folder only routes to it.
