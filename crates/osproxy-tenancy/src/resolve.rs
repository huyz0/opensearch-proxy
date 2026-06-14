//! Resolving the partition id from a request per a [`PartitionKeySpec`].

use osproxy_core::PartitionId;
use osproxy_rewrite::extract_scalar;
use osproxy_spi::{PartitionKeySpec, PartitionKeySpecKind, RequestCtx, SpiError};
use serde_json::Value;

/// Resolves the partition id by trying `spec`'s sources in order.
///
/// `doc` is the request body parsed as JSON, or `None` if the body was absent
/// or not valid JSON (in which case [`PartitionKeySpec::BodyField`] simply does
/// not resolve).
///
/// # Errors
///
/// Returns [`SpiError::PartitionUnresolved`] listing the source kinds tried if
/// none resolved.
pub(crate) fn resolve_partition(
    spec: &PartitionKeySpec,
    ctx: &RequestCtx<'_>,
    doc: Option<&Value>,
) -> Result<PartitionId, SpiError> {
    let mut tried = Vec::new();
    match try_spec(spec, ctx, doc, &mut tried) {
        Some(id) => Ok(PartitionId::from(id.as_str())),
        None => Err(SpiError::PartitionUnresolved { tried }),
    }
}

/// Tries a single spec (recursing through [`PartitionKeySpec::AnyOf`]),
/// recording each leaf kind it attempts into `tried`.
fn try_spec(
    spec: &PartitionKeySpec,
    ctx: &RequestCtx<'_>,
    doc: Option<&Value>,
    tried: &mut Vec<PartitionKeySpecKind>,
) -> Option<String> {
    match spec {
        PartitionKeySpec::BodyField(path) => {
            tried.push(PartitionKeySpecKind::BodyField);
            doc.and_then(|d| extract_scalar(d, path.segments()).ok())
        }
        PartitionKeySpec::Header(name) => {
            tried.push(PartitionKeySpecKind::Header);
            ctx.headers().get(name).map(str::to_owned)
        }
        PartitionKeySpec::PrincipalAttr(name) => {
            tried.push(PartitionKeySpecKind::PrincipalAttr);
            ctx.principal().attr(name).map(str::to_owned)
        }
        PartitionKeySpec::AnyOf(specs) => specs
            .iter()
            .find_map(|inner| try_spec(inner, ctx, doc, tried)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{EndpointKind, PrincipalId, RequestId};
    use osproxy_spi::{
        HeaderView, HttpMethod, JsonPath, Principal, PrincipalAttr, Protocol, RequestCtx,
    };
    use serde_json::json;

    fn ctx<'a>(
        principal: &'a Principal,
        rid: &'a RequestId,
        headers: &'a [(String, String)],
        body: &'a [u8],
    ) -> RequestCtx<'a> {
        RequestCtx::new(
            principal,
            rid,
            HttpMethod::Put,
            EndpointKind::IngestDoc,
            Protocol::Http1,
            "logical",
            HeaderView::new(headers),
            body,
        )
    }

    #[test]
    fn body_field_resolves_from_document() {
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("r");
        let headers = vec![];
        let c = ctx(&principal, &rid, &headers, b"");
        let doc = json!({ "tenant_id": "acme" });
        let spec = PartitionKeySpec::BodyField(JsonPath::new("tenant_id"));
        assert_eq!(
            resolve_partition(&spec, &c, Some(&doc)).unwrap(),
            PartitionId::from("acme")
        );
    }

    #[test]
    fn any_of_falls_through_to_principal_and_records_tries() {
        let principal =
            Principal::new(PrincipalId::from("svc")).with_attr(PrincipalAttr::new("tenant", "p9"));
        let rid = RequestId::from("r");
        let headers = vec![];
        let c = ctx(&principal, &rid, &headers, b"");
        let doc = json!({ "other": 1 });
        let spec = PartitionKeySpec::AnyOf(vec![
            PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            PartitionKeySpec::Header("x-tenant".to_owned()),
            PartitionKeySpec::PrincipalAttr("tenant".to_owned()),
        ]);
        assert_eq!(
            resolve_partition(&spec, &c, Some(&doc)).unwrap(),
            PartitionId::from("p9")
        );
    }

    #[test]
    fn unresolved_reports_each_kind_tried() {
        let principal = Principal::new(PrincipalId::from("svc"));
        let rid = RequestId::from("r");
        let headers = vec![];
        let c = ctx(&principal, &rid, &headers, b"");
        let spec = PartitionKeySpec::AnyOf(vec![
            PartitionKeySpec::Header("x-tenant".to_owned()),
            PartitionKeySpec::PrincipalAttr("tenant".to_owned()),
        ]);
        let err = resolve_partition(&spec, &c, None).unwrap_err();
        assert!(
            matches!(&err, SpiError::PartitionUnresolved { tried }
            if *tried == vec![
                PartitionKeySpecKind::Header,
                PartitionKeySpecKind::PrincipalAttr,
            ]),
            "unexpected: {err:?}"
        );
    }
}
