//! A minimal, single-connection write loop, sized for an external profiler
//! (valgrind/callgrind) to read the **per-request CPU breakdown** of the proxy's
//! write path: ingress read → byte-splice rewrite → upstream dispatch → response.
//! One connection, sequential requests, so there is no concurrency noise —
//! callgrind ranks functions by instruction count, which is what sets the
//! throughput ceiling the connection-scaling tail queues behind
//! (`docs/guide/11-performance`).
//!
//! Two targets — a 64 KB and a 256 B loop — so a profiler can **diff** them: the
//! 256 B run is the fixed per-request cost (hyper parse, the byte-splice rewrite,
//! tokio, loopback syscalls); the 64 KB run adds the body-size-dependent cost
//! (`memcpy` of the document) on top. The difference isolates how much of the
//! large-payload work is just moving bytes.
//!
//! Not a timing benchmark and not asserted; `#[ignore]`. Profile with, e.g.:
//!
//! ```text
//! CARGO_PROFILE_RELEASE_DEBUG=true \
//!   cargo test -p osproxy-server --test profile_64k --release --no-run
//! valgrind --tool=callgrind --callgrind-out-file=/tmp/osproxy-64k.callgrind \
//!   <built-binary> --ignored --test-threads=1 profile_64k_single_connection
//! valgrind --tool=callgrind --callgrind-out-file=/tmp/osproxy-256b.callgrind \
//!   <built-binary> --ignored --test-threads=1 profile_256b_single_connection
//! callgrind_annotate /tmp/osproxy-64k.callgrind  | head -40
//! callgrind_annotate /tmp/osproxy-256b.callgrind | head -40
//! ```
#![allow(clippy::unwrap_used)]

mod common;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use common::{build_handler, payload, serve, start_upstream};
use osproxy_server::tenancy::PlacementMode;

/// Requests in the profiled loop. Enough samples for callgrind, few enough that a
/// ~30× valgrind slowdown stays quick.
const REQUESTS: usize = 200;

/// Drives `REQUESTS` sequential POSTs of a `size`-byte body over one connection
/// through a `SharedIndex` (body-rewrite) proxy.
async fn drive(size: usize) {
    let upstream = start_upstream().await;
    let proxy = serve(build_handler(&upstream, Some(PlacementMode::SharedIndex))).await;
    let body = payload(size);
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let uri: hyper::Uri = format!("http://{proxy}/orders/_doc").parse().unwrap();

    for _ in 0..REQUESTS {
        let req = Request::builder()
            .method(Method::POST)
            .uri(uri.clone())
            .header("content-type", "application/json")
            .body(Full::new(body.clone()))
            .unwrap();
        let resp = client.request(req).await.unwrap();
        assert!(resp.status().is_success());
        let _ = resp.into_body().collect().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "profiling target; run under callgrind, see module docs"]
async fn profile_64k_single_connection() {
    drive(65536).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "profiling target; run under callgrind, see module docs"]
async fn profile_256b_single_connection() {
    drive(256).await;
}
