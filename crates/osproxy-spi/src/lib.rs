//! Public SPI traits for osproxy.
//!
//! This is the contract implementers compile against (`docs/02`). It depends
//! only on [`osproxy_core`] (plus `serde_json` for body values) so the surface
//! stays tiny and fast.
//!
//! Two layers:
//!
//! - [`RoutingSpi`], low-level, full control over the [`RouteDecision`].
//! - [`TenancySpi`], high-level, declarative tenancy rules; `osproxy-tenancy`
//!   adapts it into a [`RoutingSpi`].
//!
//! Supporting vocabulary is grouped by concern: [`Principal`] identity,
//! [`RequestCtx`] inputs, [`RouteDecision`] outputs, declarative [`rules`], and
//! [`Placement`] results. Every public item carries an example, per NFR-Q3.
#![deny(missing_docs)]

// Re-export the core vocabulary so SPI implementers get the identifier, target,
// and error types from a single dependency.
pub use osproxy_core as core;

mod auth;
mod decision;
mod error;
mod placement;
mod principal;
mod request;
mod routing;
pub mod rules;
mod tenancy;

pub use auth::{Action, AuthError, Authenticator, Authorizer, ClientCredentials};
pub use decision::{BodyTransform, HeaderOp, RouteDecision};
pub use error::SpiError;
pub use placement::{MigrationPhase, Placement, PlacementAt};
pub use principal::{Principal, PrincipalAttr};
pub use request::{BodyDoc, HeaderView, HttpMethod, Protocol, RequestCtx};
pub use routing::RoutingSpi;
pub use rules::{
    DocIdRule, IdTemplate, InjectedField, InjectedValue, JsonPath, PartitionKeySpec,
    PartitionKeySpecKind, SensitivitySpec,
};
pub use tenancy::TenancySpi;
