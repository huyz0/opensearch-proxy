//! The authenticated client identity passed to the SPI.

use osproxy_core::PrincipalId;

/// The authenticated caller, as seen by the routing/tenancy SPI.
///
/// Carries a stable [`PrincipalId`] and a small set of attributes an
/// implementer may key tenancy decisions on (e.g. a tenant id derived from the
/// client certificate). It **never** carries the raw credential (token,
/// certificate bytes): those are consumed by the authenticator and dropped, so
/// nothing secret reaches the SPI or telemetry (`docs/05` §7, NFR-S2).
///
/// # Examples
///
/// ```
/// use osproxy_core::PrincipalId;
/// use osproxy_spi::{Principal, PrincipalAttr};
///
/// let p = Principal::new(PrincipalId::from("svc-ingest"))
///     .with_attr(PrincipalAttr::new("tenant", "acme"));
/// assert_eq!(p.id().as_str(), "svc-ingest");
/// assert_eq!(p.attr("tenant"), Some("acme"));
/// assert_eq!(p.attr("missing"), None);
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Principal {
    id: PrincipalId,
    attrs: Vec<PrincipalAttr>,
}

impl Principal {
    /// Constructs a principal with no attributes.
    #[must_use]
    pub fn new(id: PrincipalId) -> Self {
        Self {
            id,
            attrs: Vec::new(),
        }
    }

    /// Adds an attribute (builder style).
    #[must_use]
    pub fn with_attr(mut self, attr: PrincipalAttr) -> Self {
        self.attrs.push(attr);
        self
    }

    /// The principal's stable id.
    #[must_use]
    pub fn id(&self) -> &PrincipalId {
        &self.id
    }

    /// Looks up an attribute value by key, if present.
    #[must_use]
    pub fn attr(&self, key: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|a| a.key == key)
            .map(|a| a.value.as_str())
    }
}

/// A single named attribute carried by a [`Principal`].
///
/// Both key and value are derived identity facts (never secrets), so they are
/// safe to use in routing and to surface as trace attributes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrincipalAttr {
    /// The attribute name (e.g. `"tenant"`).
    pub key: String,
    /// The attribute value (e.g. `"acme"`).
    pub value: String,
}

impl PrincipalAttr {
    /// Constructs an attribute from a key and value.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_are_looked_up_by_key() {
        let p = Principal::new(PrincipalId::from("u-1"))
            .with_attr(PrincipalAttr::new("tenant", "acme"))
            .with_attr(PrincipalAttr::new("region", "eu"));
        assert_eq!(p.attr("tenant"), Some("acme"));
        assert_eq!(p.attr("region"), Some("eu"));
        assert_eq!(p.attr("nope"), None);
        assert_eq!(p.id().as_str(), "u-1");
    }
}
