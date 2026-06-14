//! Transport layer: bytes on and off the wire.
//!
//! Owns protocol framing (HTTP/1.1, HTTP/2, gRPC), TLS termination behind the
//! `CryptoProvider` seam (`docs/07`), and pooled upstream connections with TLS
//! session reuse (`docs/04` §7). It knows nothing about routing decisions or
//! tenancy semantics. Implementation lands in milestones M1 and M4 (`docs/11`).
