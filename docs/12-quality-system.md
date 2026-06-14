# 12 — The Quality System

Quality is enforced in **two tiers** that do not overlap:

- **Tier 1 — Deterministic gates.** Mechanical checks with a yes/no answer and no
  human judgment: they pass identically on every machine and every run. These
  *block* merge.
- **Tier 2 — LLM semantic review.** Judgment-based review of design, naming,
  abstraction quality, and test meaningfulness — the things a linter cannot see.
  Driven by the skills system. These *advise* (and gate when high-confidence).

The rule: **if a quality property can be made deterministic, it must be Tier 1.**
The LLM is spent only on what genuinely needs judgment, never on what a check can
decide. This keeps quality reproducible and cheap, and keeps the LLM focused.

## Tier 1 — Deterministic gates

| Property | Mechanism | Command | Why deterministic |
|----------|-----------|---------|-------------------|
| Formatting | `rustfmt --check` | `cargo xtask fmt` | canonical formatter |
| Lint / no-panic / complexity | clippy `-D warnings` + budgets in `clippy.toml` | `cargo xtask clippy` | fixed rule set |
| **Determinism of code** | clippy `disallowed-methods` bans `Instant::now`/`SystemTime::now`; code takes a `core::time::Clock` | `cargo xtask clippy` | banned at compile time |
| **Architecture** | crate dependency-direction + acyclicity check against the declared DAG | `cargo xtask arch` | graph subset check |
| Size / god-module budgets | file-length + complexity budgets | `cargo xtask budgets` | line/complexity counts |
| **Correctness** | unit + `proptest` property tests of invariants | `cargo xtask test` | seeded, shrinking |
| **Memory** | `dhat` allocation-count budgets on hot paths | `cargo xtask test` | alloc counts are exact |
| **Micro-performance** | `iai-callgrind` **instruction-count** benchmarks | `cargo xtask bench` (CI, needs valgrind) | callgrind counts, not wall-clock |
| I/O usage | byte-counting wrappers asserted in integration tests | `cargo xtask test` (M4+) | counted bytes are exact |
| Coverage ≥90% semantic | `cargo llvm-cov --fail-under-lines 90` | CI | line counts |
| Skill-system integrity | size + frontmatter lint | `cargo xtask skills` | text checks |
| Supply chain | `cargo-deny` (licenses, advisories, sources) | CI | fixed policy |

### Why these tools

- **Determinism is enforced, not hoped for.** Wall-clock reads and unseeded
  randomness are the usual sources of flakiness; we ban them at the clippy layer
  and inject `core::time::Clock` instead. Tests advance a `ManualClock` and are
  reproducible to the nanosecond.
- **Performance is measured in instruction counts** (`iai-callgrind` over
  callgrind), not wall-clock time. Wall-clock benchmarks are noisy and machine-
  dependent — useless as a CI gate. Instruction counts are exact, so a real
  regression is visible and a refactor that doesn't change work shows zero delta.
  For **local exploration on a dev box without valgrind**, `cargo xtask
  bench-local` runs the same hot paths under `divan` (wall-clock); it is a
  calibration/comparison aid only and never gates a build.
- **Memory budgets are allocation counts** (`dhat` testing mode), which are exact
  for a given input — a change that adds a heap allocation to a hot path fails CI.
- **Architecture is a graph check.** The allowed dependency DAG lives in
  `xtask`; each crate's actual internal deps must be a subset, which proves the
  graph stays downward-only and acyclic (no god-coupling creeps in).
- **Correctness uses property tests** for the invariants that define the proxy
  (round-trip symmetry, isolation, order preservation, id-collision-freedom),
  not just example tests — see [09](09-testing-and-quality.md).

## Tier 2 — LLM semantic & design review

What a linter cannot judge, the LLM reviews against the skills system:

- **Altitude / abstraction quality** — is this the right level; is the seam in the
  right place; is logic where it belongs per the `architecture` skill?
- **Naming & intent** — do names reveal intent; does the code read like its
  neighbours?
- **Cohesion** — is a module doing one thing; is a type accreting unrelated
  fields (a god-type the size budget alone won't catch)?
- **Doc quality** — do SPI docs state intent + invariants, not restate the
  signature?
- **Test meaningfulness** — do assertions actually constrain behavior, or just
  inflate coverage? (Backed periodically by `cargo-mutants` in Tier 1.)
- **Adherence to the invariants** in `AGENTS.md` and the relevant skill.

Driven by the `quality-review` skill, which defines the rubric. It runs through
the **AI agent's own capabilities — not a CI secret**: the `quality-reviewer`
subagent (`.claude/agents/`) is spawned before finishing a unit of work, and the
`/quality-review` command runs it on demand; the repo's `/code-review` and
`/security-review` are additional passes. High-confidence findings gate; uncertain
ones advise.

Why agent-native rather than a CI bot: the review belongs in the same loop that
writes the code, needs no shared secret or external service, and reads the same
skills the author follows — so the bar is identical and the feedback is immediate.

## How the tiers interact

1. Tier 1 runs first (`cargo xtask ci` locally, the CI gate remotely). If it is
   red, Tier 2 is not worth spending — fix the mechanical failures first.
2. Tier 2 reviews the green diff for what judgment is needed: spawn the
   `quality-reviewer` subagent (or run `/quality-review`) before declaring the
   work done.
3. A finding that recurs and *can* be made deterministic graduates to Tier 1
   (e.g. a naming convention becomes a lint). The deterministic tier grows;
   the LLM is never asked to police what a check could.
