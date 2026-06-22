# ADR-007: SPI compiled in statically; no dynamic plugins

**Status:** Accepted

## Context

The proxy must be extensible "as a library" so implementers can access request
headers/body and return routing decisions. Extensibility could be runtime
(WASM/dylib plugins) or compile-time (trait impls linked in).

## Decision

The SPI is **compiled in statically.** Implementers depend on `osproxy-spi`,
`impl` the traits, and link their logic into the binary. No WASM, no dylib, no
runtime plugin discovery. The user explicitly did not want dynamicity.

## Why

- Monomorphized trait calls → no dynamic-dispatch/sandbox overhead on the hot
  path (NFR-P).
- No plugin ABI/versioning/sandbox-escape surface → smaller security & reliability
  surface.
- Simpler build and deployment; the routing logic's types are checked at compile
  time alongside the core.

## Consequences

- Changing routing logic requires a rebuild (acceptable; matches the "library"
  model).
- The public SPI surface must be small, well-documented, and stable (docs/02,
  docs/08 §4); changes are design-review events (docs/10).
- Multi-tenant "different logic per deployment" is handled by building the
  appropriate impl in, not by loading plugins.
