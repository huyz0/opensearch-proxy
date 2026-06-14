//! The reference authenticator the binary uses.
//!
//! A minimal token authenticator: a configured `token -> principal` map. With
//! no tokens configured it runs in **dev mode**, accepting any caller as an
//! anonymous (or token-named) principal — convenient for local runs, never for
//! production. Real consumers provide their own [`Authenticator`] (mTLS, JWT, an
//! external identity provider, …).

use std::collections::HashMap;

use osproxy_core::PrincipalId;
use osproxy_spi::{AuthError, Authenticator, ClientCredentials, Principal};

/// A bearer-token authenticator over a static `token -> principal id` map.
#[derive(Debug, Default)]
pub struct ReferenceAuthenticator {
    tokens: HashMap<String, String>,
}

impl ReferenceAuthenticator {
    /// Builds an authenticator requiring one of `tokens` (token -> principal id).
    #[must_use]
    pub fn new(tokens: HashMap<String, String>) -> Self {
        Self { tokens }
    }

    /// A dev-mode authenticator that accepts any caller (no tokens configured).
    #[must_use]
    pub fn dev() -> Self {
        Self::default()
    }

    /// Whether the authenticator is in permissive dev mode.
    fn is_dev(&self) -> bool {
        self.tokens.is_empty()
    }
}

impl Authenticator for ReferenceAuthenticator {
    async fn authenticate(&self, creds: &ClientCredentials) -> Result<Principal, AuthError> {
        if self.is_dev() {
            // Dev mode: name the principal after the presented token if any,
            // else "anonymous". Never rejects.
            let id = creds.bearer_token.as_deref().unwrap_or("anonymous");
            return Ok(Principal::new(PrincipalId::from(id)));
        }
        let token = creds
            .bearer_token
            .as_deref()
            .ok_or(AuthError::MissingCredentials)?;
        self.tokens
            .get(token)
            .map(|pid| Principal::new(PrincipalId::from(pid.as_str())))
            .ok_or(AuthError::InvalidCredentials)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dev_mode_accepts_anyone() {
        let auth = ReferenceAuthenticator::dev();
        let p = auth
            .authenticate(&ClientCredentials::default())
            .await
            .unwrap();
        assert_eq!(p.id().as_str(), "anonymous");
        let p = auth
            .authenticate(&ClientCredentials::bearer("svc-x"))
            .await
            .unwrap();
        assert_eq!(p.id().as_str(), "svc-x");
    }

    #[tokio::test]
    async fn configured_tokens_are_enforced() {
        let mut tokens = HashMap::new();
        tokens.insert("s3cr3t".to_owned(), "svc-ingest".to_owned());
        let auth = ReferenceAuthenticator::new(tokens);

        let p = auth
            .authenticate(&ClientCredentials::bearer("s3cr3t"))
            .await
            .unwrap();
        assert_eq!(p.id().as_str(), "svc-ingest");

        assert_eq!(
            auth.authenticate(&ClientCredentials::bearer("wrong")).await,
            Err(AuthError::InvalidCredentials)
        );
        assert_eq!(
            auth.authenticate(&ClientCredentials::default()).await,
            Err(AuthError::MissingCredentials)
        );
    }
}
