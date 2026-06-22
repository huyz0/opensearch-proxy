//! Transport layer: bytes on and off the wire.
//!
//! Owns protocol framing and, in a later slice, TLS termination behind the
//! `CryptoProvider` seam (`docs/07`) and pooled upstream connections (`docs/04`
//! §7). It knows nothing about routing decisions or tenancy semantics.
//!
//! M1 implements the HTTP/1.1 cleartext **ingress**: [`serve`] accepts
//! connections, parses each request into an owned [`IngressRequest`] (with its
//! [`EndpointKind`](osproxy_core::EndpointKind) classified by [`classify()`]),
//! invokes an [`IngressHandler`], and writes the [`IngressResponse`]. The handler,
//! implemented by the binary, is where the request meets the engine pipeline.
#![deny(missing_docs)]

// Exactly one crypto provider is compiled in, chosen at build time (ADR-009).
// `non-fips` is the default; a FIPS release builds `--no-default-features
// --features fips`. Catch a mis-invocation at compile time rather than silently
// linking both (or neither) crypto module.
#[cfg(all(feature = "fips", feature = "non-fips"))]
compile_error!(
    "features `fips` and `non-fips` are mutually exclusive; a FIPS artifact must \
     not link a non-validated crypto module; build with `--no-default-features \
     --features fips`"
);
#[cfg(not(any(feature = "fips", feature = "non-fips")))]
compile_error!("enable exactly one crypto provider feature: `fips` or `non-fips`");

mod admission;
mod classify;
mod grpc;
mod handler;
mod http_io;
mod request;
mod server;
mod tls;

pub use admission::IngressLimits;
pub use classify::{classify, Classified};
pub use grpc::{serve_grpc, serve_grpc_tls};
pub use handler::IngressHandler;
/// The streamed request body type for [`IngressHandler::handle_forward`],
/// re-exported so handlers can name it without depending on `hyper` directly.
pub use hyper::body::Incoming;
pub use request::{
    buffered_response, IngressRequest, IngressResponse, ResponseBody, StreamingResponse,
};
pub use server::{
    serve, serve_tls, serve_tls_with_limits, serve_tls_with_shutdown, serve_with_limits,
    serve_with_shutdown, DRAIN_DEADLINE,
};
pub use tls::{CryptoProvider, TlsError, FIPS_APPROVED_SUITES};

#[cfg(feature = "fips")]
pub use tls::AwsLcFipsProvider;
#[cfg(feature = "non-fips")]
pub use tls::RingProvider;

/// The crypto provider the active build selected: `RingProvider` under
/// `non-fips`, `AwsLcFipsProvider` under `fips`. Server/wiring code names this
/// alias so it never hard-codes a concrete provider or branches on the feature.
#[cfg(feature = "non-fips")]
pub type DefaultCryptoProvider = tls::RingProvider;
/// The crypto provider the active build selected (see the `non-fips` variant).
#[cfg(feature = "fips")]
pub type DefaultCryptoProvider = tls::AwsLcFipsProvider;
