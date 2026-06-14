---
name: git-workflow
description: "WHAT: Commit message format, the gate-before-done rule, and hooks. USE WHEN: committing, writing a commit message, or finishing a unit of work."
---

# Git workflow

Single developer, work directly on `main` (no feature branches). Every commit
must leave the tree green.

## Rules

- **Conventional commit subject**: `type(scope): lowercase description` where
  `type ∈ {feat, fix, docs, test, chore, refactor, perf, build, ci}`. Example:
  `feat(tenancy): construct partition-prefixed doc id`.
- **End with the trailer** `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Gate before done.** Run `cargo xtask ci` and ensure it is green before
  considering a task complete or committing. Commit/push only when the user asks.
- **Docs/skills update in the same commit** as the code they describe — drift is
  a bug.

## Enforced by

- `.githooks/commit-msg` — validates the subject format and trailer presence.
- `.githooks/pre-commit` — runs `cargo xtask ci`, blocks on failure.
- Install once: `git config core.hooksPath .githooks` (done by
  `scripts/setup-hooks.sh`).

## Deep dive

[docs/10-review-process.md](../../../docs/10-review-process.md).
