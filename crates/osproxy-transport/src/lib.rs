//! Transport layer: bytes on and off the wire.
//!
//! Owns protocol framing and, in a later slice, TLS termination behind the
//! `CryptoProvider` seam (`docs/07`) and pooled upstream connections (`docs/04`
//! §7). It knows nothing about routing decisions or tenancy semantics.
//!
//! M1 implements the HTTP/1.1 cleartext **ingress**: [`serve`] accepts
//! connections, parses each request into an owned [`IngressRequest`] (with its
//! [`EndpointKind`](osproxy_core::EndpointKind) classified by [`classify()`]),
//! invokes an [`IngressHandler`], and writes the [`IngressResponse`]. The handler
//! — implemented by the binary — is where the request meets the engine pipeline.
#![deny(missing_docs)]

mod admission;
mod classify;
mod grpc;
mod handler;
mod request;
mod server;
mod tls;

pub use admission::IngressLimits;
pub use classify::{classify, Classified};
pub use grpc::{serve_grpc, serve_grpc_tls};
pub use handler::IngressHandler;
pub use request::{IngressRequest, IngressResponse};
pub use server::{serve, serve_tls, serve_tls_with_limits, serve_with_limits};
pub use tls::{CryptoProvider, RingProvider, TlsError};
