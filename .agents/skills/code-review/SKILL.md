---
name: code-review
description: "WHAT: The self-review checklist run before opening a PR or finishing a change. USE WHEN: finishing a unit of work, before committing, or reviewing a diff."
---

# Code review / self-review

Because development is LLM-driven, the implementing agent self-reviews before
declaring work done. A change that cannot truthfully tick these boxes is not
ready.

## Checklist

- **Correctness**: behavior matches the relevant doc; all error paths return
  typed, contextual errors (no `unwrap`/`expect`/`panic`/`anyhow` on the request
  path); touched invariants (symmetry, isolation, epoch, order) have tests.
- **Structure**: no god file/module/type; budgets respected or `// JUSTIFY`'d;
  single responsibility; dependency direction intact; newtypes for ids; no
  `unsafe`.
- **Tests**: unit + property/integration as applicable; failure modes covered;
  coverage thresholds hold; deterministic.
- **Traceability**: new decision points emit shape-only span attributes; new
  failure modes appear in `/debug/explain` with remediation; blind-diagnosis
  extended.
- **Security**: no value/secret can reach a log/trace; isolation preserved for
  new endpoints; FIPS suite pinning unaffected (or boundary doc updated).
- **Docs**: public SPI items documented with example; relevant `docs/` and
  skills updated in the same change.

## Enforced by

- `cargo xtask ci` (fmt, clippy `-D warnings`, arch, test, doc, budgets, skills).
- **Tier 2 before done**: spawn the `quality-reviewer` subagent (or run
  `/quality-review`) on the green diff — see the `quality-review` skill.
- Repo's `/code-review` and `/security-review` tooling on the diff (docs/10 §3).

## Deep dive

[docs/10-review-process.md](../../../docs/10-review-process.md) (full checklist +
release gates).
