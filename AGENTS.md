# AGENTS.md

Orientation for AI agents on **osproxy**. A router + invariants list; it does
**not** repeat the detail in [`.agents/skills/`](.agents/skills/) and
[`docs/`](docs/). On conflict, the skill/doc wins for its topic — fix the drift.

## What this is

A high-performance OpenSearch routing proxy (Rust library + binary). It routes
each request to exactly one physical placement (cluster/index) based on a
**partition**-keyed placement policy, injecting partition fields and constructing
doc ids on write and reversing both on read. Consumed as a library: implementers
`impl` the `osproxy-spi` traits, compiled in statically (no dynamic plugins).

Design phase. The full mental model is [`docs/00-goals.md`](docs/00-goals.md) →
[`docs/01-architecture.md`](docs/01-architecture.md); decisions and rationale are
ADRs in [`docs/decisions/`](docs/decisions/).

## Invariants (don't break)

1. **Downward-only crate deps.** `core` depends on nothing; `spi` only on `core`;
   siblings talk only through `core`/`spi`. `core`/`spi` have no I/O deps. See the
   **architecture** skill.
2. **One partition, one placement, at any instant.** Search is single-target — no
   synchronous fan-out (ADR-002). See the **tenancy** skill.
3. **No write commits against a stale epoch for a migrating partition** (ADR-003).
4. **Read isolation is filtered-or-rejected**, never best-effort (ADR-006).
5. **No panics / no `anyhow` on the request path.** Every failure is a typed
   `ErrorContext` (code, decision chain, retryable, remediation). See the **spi**
   skill.
6. **Telemetry is shape-only and read-only** — never tenant values or secrets in
   any log/trace. See the **observability** skill.
7. **Time is injected, never read directly.** `Instant::now`/`SystemTime::now`
   are banned; take a `osproxy_core::time::Clock`. See the **performance** skill.
8. **Keep the gates green** — `cargo xtask ci` must pass before a task is done;
   new behavior needs tests (≥90% semantic coverage). Quality is two-tier:
   deterministic gates + LLM semantic review (docs/12).

## Commands

`.githooks/pre-commit` (install via `scripts/setup-hooks.sh`) runs the gate and
blocks on failure. Run it before calling a task done:

| Step | Command |
|------|---------|
| Full gate | `cargo xtask ci` |
| Format | `cargo xtask fmt` (`cargo fmt --all`) |
| Lint | `cargo xtask clippy` (clippy `-D warnings`) |
| Tests | `cargo xtask test` |
| Docs + doc tests | `cargo xtask doc` |
| Size/complexity budgets | `cargo xtask budgets` |
| Skill-system lint | `cargo xtask skills` |
| Architecture (dep graph) | `cargo xtask arch` |
| Deterministic perf (CI) | `cargo xtask bench` (needs valgrind) |

**Commits**: `commit-msg` allows only `feat|fix|docs|test|chore|refactor|perf|
build|ci` + optional `(scope)` + lowercase description, and requires the
`Co-Authored-By:` trailer. Single dev — work directly on `main`. See the
**git-workflow** skill.

## Where to look (task → skill → doc)

Skills are the process source of truth; `docs/` are the deep-dives.

| Task | Skill | Doc |
|------|-------|-----|
| Crates / modules / deps / budgets | `architecture` | [01](docs/01-architecture.md), [08](docs/08-engineering-standards.md) |
| Public traits / SPI contract | `spi` | [02](docs/02-spi.md) |
| Partition / placement / migration | `tenancy` | [03](docs/03-tenancy-and-placement.md), [06](docs/06-partition-migration.md) |
| Bulk demux / query rewrite / pooling | `pipeline` | [04](docs/04-request-pipeline.md) |
| Traces / `/debug/explain` / no-leak | `observability` | [05](docs/05-observability.md) |
| Tests / coverage / property tests | `testing` | [09](docs/09-testing-and-quality.md) |
| Perf / memory / determinism / `core::time` | `performance` | [12](docs/12-quality-system.md) |
| Reviewing a diff (semantic/design) | `quality-review` | [12](docs/12-quality-system.md) |
| TLS / FIPS / crypto boundary | `fips` | [07](docs/07-fips-and-crypto.md) |
| Commits / hooks / finishing | `git-workflow`, `code-review` | [10](docs/10-review-process.md) |
| Editing skills | `manage-skills` | — |

## Workflow

- **Read the matching skill before acting** — it encodes the rule and names the
  gate that enforces it.
- **Lowest useful test layer** (unit > property > integration > e2e);
  deterministic, inject clocks/stores.
- **Minimal, in the right place** — a cross-layer import, a panic on the request
  path, or a value in a log is the signal to invert a dependency, return a typed
  error, or pass a shape instead.
- **Before declaring work done**, after `cargo xtask ci` is green, run the Tier 2
  semantic review: spawn the `quality-reviewer` subagent or run `/quality-review`
  (docs/12). It is agent-native — no CI secret.
- **Update docs/skills in the same change** — drift is a bug.
- **Roadmap**: build in milestone order, [`docs/11-roadmap.md`](docs/11-roadmap.md).
