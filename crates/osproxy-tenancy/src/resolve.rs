//! Resolving the partition id from a request per a [`PartitionKeySpec`].

use osproxy_core::PartitionId;
use osproxy_spi::{BodyDoc, PartitionKeySpec, PartitionKeySpecKind, RequestCtx, SpiError};

/// Resolves the partition id by trying `spec`'s sources in order, the
/// declarative resolver most [`osproxy_spi::TenancySpi::resolve_partition`]
/// implementations defer to.
///
/// `body` is a [`BodyDoc`] view over the document; a [`PartitionKeySpec::BodyField`]
/// reads its scalar straight from the bytes (no JSON tree, ADR-014), and an
/// absent/non-object body simply does not resolve.
///
/// # Errors
///
/// Returns [`SpiError::PartitionUnresolved`] listing the source kinds tried if
/// none resolved.
pub fn resolve_partition_spec(
    spec: &PartitionKeySpec,
    ctx: &RequestCtx<'_>,
    body: BodyDoc<'_>,
) -> Result<PartitionId, SpiError> {
    let mut tried = Vec::new();
    match try_spec(spec, ctx, body, &mut tried) {
        Some(id) => Ok(PartitionId::from(id.as_str())),
        None => Err(SpiError::PartitionUnresolved { tried }),
    }
}

/// Tries a single spec (recursing through [`PartitionKeySpec::AnyOf`]),
/// recording each leaf kind it attempts into `tried`.
fn try_spec(
    spec: &PartitionKeySpec,
    ctx: &RequestCtx<'_>,
    body: BodyDoc<'_>,
    tried: &mut Vec<PartitionKeySpecKind>,
) -> Option<String> {
    match spec {
        PartitionKeySpec::BodyField(path) => {
            tried.push(PartitionKeySpecKind::BodyField);
            body.scalar(path.as_str())
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
            .find_map(|inner| try_spec(inner, ctx, body, tried)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::{EndpointKind, PrincipalId, RequestId};
    use osproxy_spi::{
        HeaderView, HttpMethod, JsonPath, Principal, PrincipalAttr, Protocol, RequestCtx,
    };

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
        let c = ctx(&principal, &rid, &headers, br#"{ "tenant_id": "acme" }"#);
        let spec = PartitionKeySpec::BodyField(JsonPath::new("tenant_id"));
        assert_eq!(
            resolve_partition_spec(&spec, &c, BodyDoc::new(c.body())).unwrap(),
            PartitionId::from("acme")
        );
    }

    #[test]
    fn any_of_falls_through_to_principal_and_records_tries() {
        let principal =
            Principal::new(PrincipalId::from("svc")).with_attr(PrincipalAttr::new("tenant", "p9"));
        let rid = RequestId::from("r");
        let headers = vec![];
        let c = ctx(&principal, &rid, &headers, br#"{ "other": 1 }"#);
        let spec = PartitionKeySpec::AnyOf(vec![
            PartitionKeySpec::BodyField(JsonPath::new("tenant_id")),
            PartitionKeySpec::Header("x-tenant".to_owned()),
            PartitionKeySpec::PrincipalAttr("tenant".to_owned()),
        ]);
        assert_eq!(
            resolve_partition_spec(&spec, &c, BodyDoc::new(c.body())).unwrap(),
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
        let err = resolve_partition_spec(&spec, &c, BodyDoc::new(c.body())).unwrap_err();
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
