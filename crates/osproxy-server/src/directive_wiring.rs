//! Wiring for the fleet directive store and its admin publish endpoint.
//!
//! Split out of `main` so the binary's lifecycle code stays within the
//! file-length budget. Decides, from config, whether the pipeline reads an
//! in-memory store (fed by `POST /admin/directives`) or, under the `etcd`
//! feature, a distributed, watch-fed `EtcdDirectiveStore` (the etcd key is then
//! the fleet control plane and the local publish endpoint stays disabled).

use std::sync::Arc;

use osproxy_config::Config;
use osproxy_core::SystemClock;
use osproxy_observe::{DirectiveStore, InMemoryDirectiveStore};
use osproxy_server::handler::AppHandler;

/// Enables the `POST /admin/directives` channel when an admin `token` and an
/// in-memory `store` are both present; otherwise the endpoint stays disabled
/// (reports `not_enabled`).
///
/// The admin publish path is meaningful only with an in-memory store to publish
/// into; under etcd the etcd key is the control plane, so `store` is `None` and
/// the endpoint stays disabled (no publish to a store the pipeline ignores).
pub(crate) fn with_directive_admin<A: osproxy_spi::Authenticator>(
    handler: AppHandler<A>,
    store: Option<Arc<InMemoryDirectiveStore>>,
    token: Option<&str>,
) -> AppHandler<A> {
    let (Some(store), Some(token)) = (store, token) else {
        return handler;
    };
    println!("osproxy fleet directive admin: on (POST /admin/directives)");
    handler.with_directive_admin(store, token.to_owned(), Arc::new(SystemClock))
}

/// Builds the fleet directive store: the read side handed to the pipeline, plus
/// an optional in-memory store for the admin publish endpoint.
///
/// With `cfg.etcd` set, the pipeline reads an `osproxy_etcd::EtcdDirectiveStore`
/// kept fresh by an etcd watch (fleet-wide, no restart) and there is no local
/// admin store, operators publish to the etcd key. Without it, an in-memory store
/// is both read by the pipeline and written by the admin endpoint (single
/// instance). A configured etcd on a binary built without the `etcd` feature is a
/// loud startup error, never a silent fallback.
#[allow(clippy::unused_async)] // async only when the `etcd` feature is enabled
pub(crate) async fn directive_store(
    cfg: &Config,
) -> Result<(Arc<dyn DirectiveStore>, Option<Arc<InMemoryDirectiveStore>>), String> {
    let Some(etcd) = &cfg.etcd else {
        let store = Arc::new(InMemoryDirectiveStore::new());
        return Ok((store.clone(), Some(store)));
    };
    #[cfg(feature = "etcd")]
    {
        let store = osproxy_etcd::EtcdDirectiveStore::connect(
            &etcd.endpoints,
            etcd.directives_key.clone(),
            Arc::new(SystemClock),
        )
        .await
        .map_err(|e| format!("etcd directive store: {e}"))?;
        println!(
            "osproxy fleet directive store: etcd ({}, key '{}')",
            etcd.endpoints.join(","),
            etcd.directives_key
        );
        let read: Arc<dyn DirectiveStore> = Arc::new(store);
        Ok((read, None))
    }
    #[cfg(not(feature = "etcd"))]
    {
        let _ = etcd; // referenced only when the `etcd` feature is enabled
        Err(
            "etcd directive store configured but this binary was built without \
             the `etcd` feature"
                .to_owned(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_core::EndpointKind;
    use osproxy_core::{ClusterId, IndexName};
    use osproxy_server::auth::ReferenceAuthenticator;
    use osproxy_server::tenancy::ReferenceTenancy;
    use osproxy_spi::HttpMethod;
    use osproxy_tenancy::TenancyRouter;
    use osproxy_transport::{IngressHandler as _, IngressRequest};

    fn handler() -> AppHandler<ReferenceAuthenticator> {
        let tenancy = ReferenceTenancy::new(
            ClusterId::from("eu-1"),
            IndexName::from("shared"),
            "http://localhost:9200",
        );
        let pipeline = osproxy_engine::Pipeline::new(
            TenancyRouter::new(tenancy),
            osproxy_sink::OpenSearchSink::new(),
        );
        AppHandler::new(pipeline, ReferenceAuthenticator::dev())
    }

    /// A `POST /admin/directives` request; `bearer` sets (or omits) the
    /// `authorization` header so tests can drive the real token-gate branch.
    fn publish_request(bearer: Option<&str>) -> IngressRequest {
        IngressRequest {
            method: HttpMethod::Post,
            protocol: osproxy_spi::Protocol::Http1,
            path: "/admin/directives".to_owned(),
            endpoint: EndpointKind::IngestDoc,
            logical_index: String::new(),
            doc_id: None,
            headers: bearer
                .map(|t| vec![("authorization".to_owned(), format!("Bearer {t}"))])
                .unwrap_or_default(),
            body: br#"{"directives":[]}"#.to_vec(),
            query: None,
            client_cert_subject: None,
            secure: true,
        }
    }

    #[tokio::test]
    async fn without_a_store_and_token_the_admin_channel_stays_disabled() {
        // Neither present: the real endpoint reports not_enabled, not a 404 route
        // miss, distinguishing "turned off" from "no such route".
        let h = with_directive_admin(handler(), None, None);
        let resp = h.handle(publish_request(Some("whatever"))).await;
        assert_eq!(resp.status, 404);
        assert!(String::from_utf8_lossy(&resp.body).contains("not_enabled"));
    }

    #[tokio::test]
    async fn a_store_without_a_token_leaves_the_channel_disabled() {
        let store = Arc::new(InMemoryDirectiveStore::new());
        let h = with_directive_admin(handler(), Some(store), None);
        let resp = h.handle(publish_request(Some("whatever"))).await;
        assert_eq!(resp.status, 404);
        assert!(String::from_utf8_lossy(&resp.body).contains("not_enabled"));
    }

    #[tokio::test]
    async fn a_store_and_token_enable_the_admin_channel() {
        let store = Arc::new(InMemoryDirectiveStore::new());
        let h = with_directive_admin(handler(), Some(store), Some("s3cret"));
        // Wrong token: channel is enabled (not not_enabled) but unauthorized.
        let unauthorized = h.handle(publish_request(Some("nope"))).await;
        assert_eq!(unauthorized.status, 401);
        // Right token: publish succeeds.
        let published = h.handle(publish_request(Some("s3cret"))).await;
        assert_eq!(published.status, 200);
        assert!(String::from_utf8_lossy(&published.body).contains("published"));
    }

    #[tokio::test]
    async fn directive_store_defaults_to_an_in_memory_store_without_etcd_config() {
        let cfg = Config::resolve_for_test(&[]).unwrap();
        assert!(cfg.etcd.is_none());
        let (_read, admin) = directive_store(&cfg).await.unwrap();
        assert!(
            admin.is_some(),
            "in-memory store doubles as the admin store"
        );
    }
}
