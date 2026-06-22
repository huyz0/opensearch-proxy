# 09: Testing & Quality

Coverage is a floor, not the goal. We want **semantic** coverage: the behaviors,
invariants, and failure modes are tested, not just lines executed.

## 1. Coverage targets

| Surface | Target | Tool |
|---------|--------|------|
| Overall | ≥ 90% | `cargo llvm-cov` |
| `osproxy-spi` + `osproxy-tenancy` + routing core | ≥ 95% | `cargo llvm-cov` per-crate gate |
| `osproxy-rewrite` (bulk demux, query rewrite, strip) | ≥ 95% incl. branch | branch coverage |
| Error variants | 100% of request-path error variants constructed & asserted in a test | custom check |

Coverage is measured in CI and **gates merge**. A drop below threshold fails the
build. But every PR review also asks "what behavior is *not* covered?", line
coverage can be high while a branch's *meaning* is untested.

## 2. Test layers

1. **Unit tests**, pure logic per module: partition extraction, id construction,
   query wrapping, field stripping, epoch comparison, NDJSON line parsing.
2. **Property tests** (`proptest`), the invariants that make this proxy correct:
   - **Round-trip symmetry**: for any document + tenancy rule, `read(write(doc))`
     yields the logical document (injected fields stripped, id mapped back).
     This is the single most important property, write-inject and read-strip
     must be inverse.
     ```
     ∀ doc, rule:  strip(query_result(ingest(doc, rule)), rule) == doc
     ```
   - **Isolation**: a query for partition P never returns a doc with partition ≠ P
     (shared index).
   - **Bulk order preservation**: demux→dispatch→re-interleave preserves item
     order and per-item status for any mix of partitions/operations.
   - **Id collision freedom**: distinct (partition, natural_key) pairs never
     collide on constructed `_id` in shared mode.
3. **Migration simulation tests**, deterministic, time-controlled (mock clock,
   controllable store) verifying INV-M1..M4 (docs/06): no stale-epoch commit,
   monotonic cutover, abort safety, no split read view. Use a model-based /
   linearizability-style check over interleaved write+migrate operations.
4. **Integration tests**, real protocol in, mock/ephemeral OpenSearch out
   (testcontainers or a faithful mock cluster). Cover the endpoint matrix
   (docs/02 §5): each tenancy-aware endpoint proven symmetric end-to-end.
5. **Fault-injection / chaos**, slow upstreams, dropped connections, malformed
   bodies, partial bulk failures, pool exhaustion, backpressure. Assert: no
   panic, no stuck request, correct typed errors, graceful 429s (NFR-R7).
6. **Performance tests**, `criterion` micro-benchmarks for hot paths + a load
   harness for the NFR-P targets (added latency, alloc counts via `dhat`, pool
   reuse rates). Regressions gate merge once baselines are calibrated.
7. **Security tests**:
   - **No-value-leak**: fuzz documents/queries with canary secrets, assert they
     never appear in any emitted log/trace/`/debug/explain` at any verbosity
     (docs/05 §7).
   - **Isolation can't be bypassed**: adversarial client queries attempting to
     escape the partition filter (nested bool, `should`, script, `_sql`), must
     be filtered or rejected, never leak (docs/03 §5).
   - **Directive auth**: unsigned/forged `X-Debug-Directive` rejected.

## 3. The "blind diagnosis" test (NFR-T1 verification)

The headline traceability requirement gets its own automated test:

- Inject a representative failure (e.g. placement backend down, stale-epoch
  storm, upstream timeout, partition unresolved).
- Capture **only** the emitted telemetry + `/debug/explain/{id}`, no source, no
  logs beyond the structured trace.
- Feed it to an automated check (and, in CI, an LLM-judged rubric) asserting the
  trace alone identifies: which stage failed, why, the decision chain, whether
  it's retryable, and the remediation hint.
- If a human/LLM cannot diagnose from telemetry alone, the trace schema is
  deficient and the test fails. This operationalizes "no human takeover."

Implemented as `crates/osproxy-engine/tests/blind_diagnosis.rs`: each
representative failure (partition unresolved, placement missing, placement
backend down, upstream rejection, stale epoch) is driven deterministically
through the pipeline with injected tenancy/sink; the test captures *only* the
`/debug/explain` document and a programmatic `diagnose()` rubric asserts the
trace alone yields the failed stage (inferred from span presence), the stable
code, the decision chain, retryability, and a non-empty remediation. The
LLM-judged variant layers on top of the same captured evidence.

## 4. Test quality standards

- **Deterministic**: no sleeps, no wall-clock, no network flakiness. Inject
  clocks and stores. Flaky tests are bugs and are quarantined+fixed, not retried.
- **One behavior per test**, named for the behavior (`bulk_demux_preserves_item_order_under_mixed_ops`).
- **Arrange/Act/Assert** clarity; helpers for fixtures, no logic in test bodies
  that itself needs testing.
- **Failure messages are diagnostic**, assert with context so a failing test
  tells you what invariant broke.
- Mutation testing (`cargo-mutants`) periodically on `spi`/`tenancy`/`rewrite` to
  catch assertions that don't actually constrain behavior (the real defense
  against "high coverage, weak tests").

## 5. What "done" means for a feature

A feature is done when: unit + relevant property tests + integration coverage of
the endpoint(s) exist; error paths are tested; coverage thresholds hold; the
blind-diagnosis trace for its failure modes passes; budgets/lints green; docs
(including SPI doc examples) updated.
