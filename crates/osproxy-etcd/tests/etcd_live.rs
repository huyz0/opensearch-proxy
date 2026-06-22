//! Live round-trip of the etcd-backed directive store against a real etcd v3.
//!
//! Needs a Docker daemon, so it is `#[ignore]`'d, run with `--ignored`. It
//! proves the watch-and-cache contract end to end: an initial publish is read at
//! connect, a later publish propagates to `load()` without a restart, and a key
//! delete flips the fleet back to "no directives".

use std::sync::Arc;
use std::time::Duration;

use etcd_client::Client;
use osproxy_core::SystemClock;
use osproxy_etcd::EtcdDirectiveStore;
use osproxy_observe::DirectiveStore;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const KEY: &str = "osproxy/directives";
const ONE: &str = r#"{"directives":[{"id":"a","level":"Shape","ttl_secs":600}]}"#;
const TWO: &str = r#"{"directives":[
    {"id":"a","level":"Shape","ttl_secs":600},
    {"id":"b","level":"ShapeTiming","ttl_secs":600,"tenant":"acme"}
]}"#;

/// Polls `load()` until `want` directives are live, or fails after a bounded wait
/// (watch propagation is async). Returns the observed length on success.
async fn await_len(store: &EtcdDirectiveStore, want: usize) -> usize {
    for _ in 0..50 {
        if store.load().len() == want {
            return want;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    store.load().len()
}

#[tokio::test]
#[ignore = "requires Docker; run with --ignored"]
async fn directives_published_to_etcd_propagate_to_the_store() {
    let container = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
        .with_exposed_port(ContainerPort::Tcp(2379))
        .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
        .with_cmd([
            "etcd",
            "--advertise-client-urls",
            "http://0.0.0.0:2379",
            "--listen-client-urls",
            "http://0.0.0.0:2379",
        ])
        .start()
        .await
        .expect("etcd container starts");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(2379).await.unwrap();
    let endpoints = vec![format!("{host}:{port}")];

    // Seed an initial set, then connect: the initial read must see it.
    let mut client = Client::connect(&endpoints, None).await.unwrap();
    client.put(KEY, ONE, None).await.unwrap();

    let store = EtcdDirectiveStore::connect(&endpoints, KEY, Arc::new(SystemClock))
        .await
        .expect("store connects and reads the initial set");
    assert_eq!(store.load().len(), 1, "initial set read at connect");

    // Publish a larger set, the watch must propagate it with no restart.
    client.put(KEY, TWO, None).await.unwrap();
    assert_eq!(await_len(&store, 2).await, 2, "the update propagated live");

    // A malformed publish must NOT blank the live set (fail-safe, last-good kept).
    client.put(KEY, "not json", None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        store.load().len(),
        2,
        "a bad publish kept the last good set"
    );

    // Deleting the key flips the fleet to "no directives".
    client.delete(KEY, None).await.unwrap();
    assert_eq!(
        await_len(&store, 0).await,
        0,
        "a deleted key clears the set"
    );
}
