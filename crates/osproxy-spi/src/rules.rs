//! Declarative tenancy rules an implementer provides through [`TenancySpi`].
//!
//! These types are pure data: how to find the partition id, how to build the
//! document `_id`, which fields to inject, and which to treat as sensitive. The
//! [`crate::TenancySpi`] returns them; `osproxy-tenancy` interprets them. The
//! interpretation is symmetric — a field injected on ingest is stripped on read
//! (`docs/02` §2, `docs/03`).
//!
//! [`TenancySpi`]: crate::TenancySpi

use osproxy_core::FieldName;
use serde_json::Value as JsonValue;

/// A dotted path into a JSON document, e.g. `tenant_id` or `meta.tenant`.
///
/// A deliberately small subset of JSONPath: a sequence of object keys. It does
/// not support array indexing or wildcards in M1 — the partition key is a
/// scalar field on the document root or a nested object. The supported grammar
/// is version-tracked in `docs/specs/opensearch-endpoints.md`.
///
/// # Examples
///
/// ```
/// use osproxy_spi::JsonPath;
///
/// let p = JsonPath::new("meta.tenant");
/// assert_eq!(p.segments().collect::<Vec<_>>(), ["meta", "tenant"]);
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct JsonPath(String);

impl JsonPath {
    /// Constructs a path from a dotted string.
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    /// The dotted path as written.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Iterates the path's object-key segments in order.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }
}

/// How to find the partition id in a request.
///
/// Not `#[non_exhaustive]`: the resolver must handle every source kind, so a new
/// source should force the resolver to be updated rather than silently fail to
/// resolve.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PartitionKeySpec {
    /// A JSON path into the document body (ingest path).
    BodyField(JsonPath),
    /// A request header carries it (e.g. set by an upstream auth gateway).
    Header(String),
    /// Derived from a [`crate::Principal`] attribute of this name.
    PrincipalAttr(String),
    /// Try each in order until one resolves.
    AnyOf(Vec<PartitionKeySpec>),
}

/// The kind tag of a [`PartitionKeySpec`], without its payload.
///
/// Returned in [`crate::SpiError::PartitionUnresolved`] to report *which*
/// sources were tried, as shape-only telemetry (never the values looked for).
#[non_exhaustive]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PartitionKeySpecKind {
    /// Corresponds to [`PartitionKeySpec::BodyField`].
    BodyField,
    /// Corresponds to [`PartitionKeySpec::Header`].
    Header,
    /// Corresponds to [`PartitionKeySpec::PrincipalAttr`].
    PrincipalAttr,
}

/// Rule to construct a document `_id`.
///
/// In `SharedIndex` placement the partition id MUST appear in the template so
/// ids cannot collide across tenants sharing one physical index (`docs/03`).
/// `osproxy-tenancy` enforces this.
///
/// # Examples
///
/// ```
/// use osproxy_spi::{DocIdRule, IdTemplate};
///
/// let rule = DocIdRule::new(IdTemplate::new("{partition}:{body.order_id}"))
///     .with_routing(true);
/// assert!(rule.set_routing);
/// assert!(rule.template.references_partition());
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DocIdRule {
    /// The id template, e.g. `{partition}:{body.natural_key}`.
    pub template: IdTemplate,
    /// Also set OpenSearch `_routing` to the partition id, so the document
    /// lands on a deterministic shard for the partition.
    pub set_routing: bool,
}

impl DocIdRule {
    /// Constructs a rule from a template, with routing off.
    #[must_use]
    pub fn new(template: IdTemplate) -> Self {
        Self {
            template,
            set_routing: false,
        }
    }

    /// Sets `set_routing` (builder style).
    #[must_use]
    pub fn with_routing(mut self, set_routing: bool) -> Self {
        self.set_routing = set_routing;
        self
    }
}

/// A document-`_id` template with `{partition}` and `{body.<path>}` placeholders.
///
/// Interpretation lives in `osproxy-rewrite`; this is just the parsed-on-demand
/// source string. `{partition}` expands to the resolved partition id;
/// `{body.<path>}` expands to a scalar pulled from the document at `<path>`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IdTemplate(String);

impl IdTemplate {
    /// Constructs a template from its source string.
    pub fn new(template: impl Into<String>) -> Self {
        Self(template.into())
    }

    /// The template source.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether the template references the partition id placeholder. Used to
    /// reject a `SharedIndex` rule that would allow cross-tenant id collisions.
    #[must_use]
    pub fn references_partition(&self) -> bool {
        self.0.contains("{partition}")
    }
}

/// A field the proxy injects into every ingested document (and strips on read).
///
/// The field *name* is chosen by the implementer (per the requirement that the
/// SPI decides injected field names). The value is computed per-document from
/// [`InjectedValue`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InjectedField {
    /// The name of the field to inject.
    pub name: FieldName,
    /// How to compute the field's value.
    pub value: InjectedValue,
}

impl InjectedField {
    /// Constructs an injected field.
    #[must_use]
    pub fn new(name: FieldName, value: InjectedValue) -> Self {
        Self { name, value }
    }
}

/// How an [`InjectedField`]'s value is computed for a document.
///
/// Not `#[non_exhaustive]`: the proxy must resolve every value kind to inject a
/// concrete value, so a new kind should force the resolver to be updated.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum InjectedValue {
    /// The resolved partition id. This is the **isolation** value: the read path
    /// filters on it, so it must be deterministic (the partition), not
    /// context-derived. Exactly the fields whose value is `PartitionId` drive
    /// read isolation.
    PartitionId,
    /// A fixed JSON value, the same for every document.
    Constant(JsonValue),
    /// A named attribute of the authenticated principal, resolved per request.
    /// A *decorative* value: injected on write and stripped on read, never used
    /// as a read filter (its value can differ between the write and the read).
    FromPrincipal(String),
    /// A named request header, resolved per request. Decorative like
    /// [`InjectedValue::FromPrincipal`]: injected and stripped, never filtered.
    /// Lets injection be dynamic from request context (e.g. a `_region` field
    /// taken from an `x-region` header set by an upstream gateway).
    FromHeader(String),
}

/// Declares which document field *values* observability may capture.
///
/// Drives value-suppression so observability never leaks tenant values (NFR-S2).
/// The model is **deny-by-default (opt-out)**: every field is treated as
/// sensitive unless explicitly allow-listed as safe. A field added to your
/// documents tomorrow is protected automatically — you opt specific, known-safe
/// fields *out* of redaction rather than remembering to opt every risky one in.
///
/// Use [`SensitivitySpec::allowing`] to name the shape-only, non-tenant fields
/// that are safe to capture; [`SensitivitySpec::all_sensitive`] (the default)
/// redacts everything; [`SensitivitySpec::nothing_sensitive`] is the explicit
/// opt-out for data that carries no tenant values at all (e.g. test fixtures).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SensitivitySpec {
    /// Fields explicitly safe to capture. Consulted only in deny-by-default mode.
    safe: Vec<FieldName>,
    /// When `true` (default), every field not in `safe` is sensitive. When
    /// `false`, nothing is sensitive (the explicit opt-out).
    deny_by_default: bool,
}

impl Default for SensitivitySpec {
    fn default() -> Self {
        Self::all_sensitive()
    }
}

impl SensitivitySpec {
    /// Deny by default: every field's value is sensitive. The safe default.
    #[must_use]
    pub fn all_sensitive() -> Self {
        Self {
            safe: Vec::new(),
            deny_by_default: true,
        }
    }

    /// Deny by default, except the `safe` fields (known shape-only / non-tenant
    /// values) which observability may capture.
    #[must_use]
    pub fn allowing(safe: Vec<FieldName>) -> Self {
        Self {
            safe,
            deny_by_default: true,
        }
    }

    /// Treat nothing as sensitive. An explicit opt-out for data that carries no
    /// tenant values (e.g. test fixtures); never appropriate for tenant payloads.
    #[must_use]
    pub fn nothing_sensitive() -> Self {
        Self {
            safe: Vec::new(),
            deny_by_default: false,
        }
    }

    /// Alias for [`SensitivitySpec::nothing_sensitive`].
    #[must_use]
    pub fn none() -> Self {
        Self::nothing_sensitive()
    }

    /// Whether `field`'s value is sensitive (deny-by-default: sensitive unless
    /// explicitly allow-listed as safe).
    #[must_use]
    pub fn is_sensitive(&self, field: &FieldName) -> bool {
        if self.deny_by_default {
            !self.safe.contains(field)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_path_splits_into_segments() {
        assert_eq!(
            JsonPath::new("a.b.c").segments().collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert_eq!(
            JsonPath::new("flat").segments().collect::<Vec<_>>(),
            ["flat"]
        );
    }

    #[test]
    fn id_template_detects_partition_reference() {
        assert!(IdTemplate::new("{partition}:{body.k}").references_partition());
        assert!(!IdTemplate::new("{body.k}").references_partition());
    }

    #[test]
    fn sensitivity_is_deny_by_default_with_an_allow_list() {
        // Deny-by-default: an unknown field is sensitive, even one never named.
        let spec = SensitivitySpec::allowing(vec![FieldName::from("status")]);
        assert!(
            spec.is_sensitive(&FieldName::from("ssn")),
            "unknown ⇒ sensitive"
        );
        assert!(spec.is_sensitive(&FieldName::from("brand_new_field")));
        assert!(
            !spec.is_sensitive(&FieldName::from("status")),
            "explicitly allow-listed ⇒ safe"
        );
        // `all_sensitive` (the default) redacts everything.
        assert!(SensitivitySpec::all_sensitive().is_sensitive(&FieldName::from("anything")));
        assert_eq!(SensitivitySpec::default(), SensitivitySpec::all_sensitive());
        // The explicit opt-out treats nothing as sensitive.
        assert!(!SensitivitySpec::nothing_sensitive().is_sensitive(&FieldName::from("ssn")));
        assert!(!SensitivitySpec::none().is_sensitive(&FieldName::from("ssn")));
    }
}
