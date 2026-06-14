# Architecture Decision Records (ADRs)

Each ADR is an immutable record of one decision: context, options, the decision,
and why. Superseding a decision means adding a new ADR that references the old
one — never editing history. This is the permanent, greppable rationale trail so
intent can be re-derived, not guessed (docs/10 §5).

| ADR | Decision |
|-----|----------|
| [001](001-language-rust.md) | Language: Rust (Go only if FIPS had no Rust path — it does) |
| [002](002-single-target-search.md) | No synchronous fan-out; every search is single-cluster |
| [003](003-epoch-gated-migration.md) | Partition migration via epoch-gated pointer flip, no in-path dual-write |
| [004](004-fips-aws-lc-rs.md) | FIPS via rustls + aws-lc-rs (`fips`), crypto behind a trait |
| [005](005-readonly-ai-observability.md) | Observability is read-only & shape-only; AI observes, never mutates |
| [006](006-isolation-filter-or-reject.md) | Read isolation: provably filtered or request rejected — no best-effort |
| [007](007-static-spi.md) | SPI compiled in statically; no WASM/dylib dynamic plugins |
| [008](008-sink-trait-deferred-redundancy.md) | Write `Sink` trait; queue-based redundancy deferred behind it |
| [009](009-m1-tls-ring-provider.md) | M1 TLS uses the `ring` provider; aws-lc-rs/FIPS at M6 behind the seam |
