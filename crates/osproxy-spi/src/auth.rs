//! Authentication and authorization contracts.
//!
//! Separated so policy can evolve independently (`docs/02` §3): an
//! [`Authenticator`] turns wire credentials into a [`Principal`]; an
//! [`Authorizer`] decides whether that principal may perform an action. Both are
//! provided by the implementer. The credential material itself
//! ([`ClientCredentials`]) is consumed here and never reaches the routing SPI or
//! telemetry (NFR-S2).

use osproxy_core::{EndpointKind, ErrorCode};
use thiserror::Error;

use crate::principal::Principal;

/// The raw client credentials extracted from a request by the transport.
///
/// Holds only what the authenticator needs; it is dropped after authentication,
/// so the bearer token never flows downstream. The TLS slice populates
/// [`ClientCredentials::client_cert_subject`] on mTLS termination.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct ClientCredentials {
    /// A bearer token from the `Authorization` header, if present.
    pub bearer_token: Option<String>,
    /// The verified client-certificate subject from mTLS, if the connection was
    /// mutually authenticated.
    pub client_cert_subject: Option<String>,
}

impl ClientCredentials {
    /// Credentials carrying just a bearer token.
    #[must_use]
    pub fn bearer(token: impl Into<String>) -> Self {
        Self {
            bearer_token: Some(token.into()),
            client_cert_subject: None,
        }
    }

    /// Whether any credential is present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bearer_token.is_none() && self.client_cert_subject.is_none()
    }
}

/// The action a principal is attempting, for authorization.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Action {
    /// The endpoint class being invoked.
    pub endpoint: EndpointKind,
    /// The logical index targeted (a name, never a value).
    pub logical_index: String,
}

/// A failure to authenticate or authorize a request.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum AuthError {
    /// No credentials were presented but the deployment requires them.
    #[error("missing credentials")]
    MissingCredentials,
    /// Credentials were presented but are not valid.
    #[error("invalid credentials")]
    InvalidCredentials,
    /// The principal is authenticated but not permitted the action.
    #[error("not authorized for the requested action")]
    Unauthorized,
}

impl AuthError {
    /// The stable [`ErrorCode`] for this failure.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::MissingCredentials | Self::InvalidCredentials => ErrorCode::AuthFailed,
            Self::Unauthorized => ErrorCode::Unauthorized,
        }
    }

    /// The HTTP status this failure maps to (401 vs 403).
    #[must_use]
    pub fn http_status(&self) -> u16 {
        match self {
            Self::MissingCredentials | Self::InvalidCredentials => 401,
            Self::Unauthorized => 403,
        }
    }
}

/// Authenticates a client and returns the principal. mTLS and/or token.
///
/// Consumed through generics (no dyn on the hot path); the future must be `Send`.
///
/// # Examples
///
/// ```
/// use osproxy_core::PrincipalId;
/// use osproxy_spi::{Authenticator, AuthError, ClientCredentials, Principal};
///
/// struct AllowAnyToken;
///
/// impl Authenticator for AllowAnyToken {
///     async fn authenticate(&self, creds: &ClientCredentials) -> Result<Principal, AuthError> {
///         let token = creds.bearer_token.as_deref().ok_or(AuthError::MissingCredentials)?;
///         Ok(Principal::new(PrincipalId::from(token)))
///     }
/// }
/// ```
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticates the credentials, returning the principal.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::MissingCredentials`] or [`AuthError::InvalidCredentials`].
    fn authenticate(
        &self,
        creds: &ClientCredentials,
    ) -> impl std::future::Future<Output = Result<Principal, AuthError>> + Send;
}

/// Authorizes a resolved request. Separate from authentication so policy can
/// evolve independently.
pub trait Authorizer: Send + Sync + 'static {
    /// Decides whether `principal` may perform `action`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Unauthorized`] if the action is not permitted.
    fn authorize(
        &self,
        principal: &Principal,
        action: &Action,
    ) -> impl std::future::Future<Output = Result<(), AuthError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_errors_map_to_codes_and_statuses() {
        assert_eq!(AuthError::MissingCredentials.code(), ErrorCode::AuthFailed);
        assert_eq!(AuthError::MissingCredentials.http_status(), 401);
        assert_eq!(AuthError::Unauthorized.code(), ErrorCode::Unauthorized);
        assert_eq!(AuthError::Unauthorized.http_status(), 403);
    }

    #[test]
    fn credentials_helpers() {
        assert!(ClientCredentials::default().is_empty());
        let c = ClientCredentials::bearer("t");
        assert!(!c.is_empty());
        assert_eq!(c.bearer_token.as_deref(), Some("t"));
    }
}
