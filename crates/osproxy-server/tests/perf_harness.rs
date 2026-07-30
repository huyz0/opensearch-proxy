//! The NFR-P load runner (`docs/01` §NFR-P, `docs/11` M4 calibration track): the
//! thing that *fills in* an [`NfrProfile`]. It drives the same write workload two
//! ways against one real OpenSearch, **direct to the cluster** (the baseline)
//! and **through the proxy**, measures per-request latency on each side, reads
//! the proxy's upstream connection-reuse counters, and emits the machine-readable
//! profile + [`judge`](osproxy_bench::judge) verdict an operator (or an LLM) reads.
//!
//! This is the artifact half of the perf story: `osproxy-bench` is the
//! deterministic vocabulary (percentiles, derived added-latency, the threshold
//! judge); this runner produces a real instance of it. It needs Docker, so it is
//! `#[ignore]`'d like the other testcontainer gates and never runs in the
//! Docker-less CI lane:
//!   `cargo test -p osproxy-server --test perf_harness -- --ignored --nocapture`
//!
//! Latency is read through `osproxy_core::SystemClock` (the one sanctioned
//! wall-clock seam), not `Instant::now`, so the determinism lint stays satisfied.

// Test scaffolding (helpers + a spawned proxy/container, not `#[test]` fns).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_precision_loss)]
// JUSTIFY(file-length): one cohesive load runner, container + proxy scaffold,
// the concurrent driver, latency collection, and profile assembly belong
// together; splitting them would duplicate the ~60-line scaffold and the shared
// request shapes across files for no gain.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use osproxy_bench::{
    judge, judge_scalability, profile_brief, scalability_brief, LatencySummary, NfrProfile,
    NfrThresholds, ScalabilityCurve, ScalabilityPoint, ScalabilityThresholds,
};
use osproxy_core::time::{Clock, SystemClock};
use osproxy_core::{ClusterId, IndexName};
use osproxy_engine::Pipeline;
use osproxy_server::auth::ReferenceAuthenticator;
use osproxy_server::handler::AppHandler;
use osproxy_server::tenancy::ReferenceTenancy;
use osproxy_sink::OpenSearchSink;
use osproxy_tenancy::TenancyRouter;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::net::TcpListener;

const INDEX: &str = "osproxy-shared";
const CLUSTER: &str = "default";
/// Requests issued per side for the [`BodyShape::Tiny`]/[`BodyShape::ManyFields`]
/// shapes. Large enough that the pool warms and percentiles are stable; small
/// enough to finish in seconds against a local container.
const TOTAL: u64 = 2_000;
/// Requests issued per side for [`BodyShape::BigField`]: fewer, since each one
/// moves `BIG_FIELD_BYTES` — this still totals tens of MB per side, plenty to
/// warm the pool and stabilize percentiles without a slow run.
const TOTAL_BIG_FIELD: u64 = 300;
/// Worker count, the configured (nominal) in-flight request count the profile
/// records; the achieved mean in-flight depends on how fast workers drain.
const CONCURRENCY: u32 = 16;
/// Size of [`BodyShape::BigField`]'s single string field: the shape that
/// motivated `osproxy-core::json`'s vectorized string scan (a large text blob
/// or base64 attachment), large enough to spend real time in that scan.
const BIG_FIELD_BYTES: usize = 64 * 1024;

/// The shape-specific JSON fields merged into [`BodyShape::ManyFields`]'s
/// envelope (`tenant_id`/`_tenant` + `id` are added by `Side::request`): a
/// typical small OpenSearch-ish document — mixed types, an array, two small
/// nested objects — not dominated by any single field, unlike [`BodyShape::BigField`].
const MANY_FIELDS_JSON: &str = concat!(
    r#""status":"shipped","total":129.99,"currency":"USD","items":3,"#,
    r#""created_at":"2026-07-30T08:00:00Z","tags":["priority","gift-wrap"],"#,
    r#""shipping":{"carrier":"ups","tracking":"1Z999AA10123456784","eta_days":2},"#,
    r#""flags":{"fraud_checked":true,"refunded":false},"#,
    r#""notes":"customer requested signature on delivery""#,
);

/// Request-body shapes the profile run varies across, so the same proxy-vs-
/// baseline comparison exercises both the tiny-envelope hot path and the
/// large-string / many-field paths through `osproxy-core::json`'s zero-
/// materialization scanner, end to end against a real cluster.
#[derive(Clone, Copy, Debug)]
enum BodyShape {
    /// The original fixed 3-field envelope: a tenant id, an integer id, one
    /// constant one-character string.
    Tiny,
    /// One large string field ([`BIG_FIELD_BYTES`]).
    BigField,
    /// Many small fields, mixed types, two small nested objects, an array.
    ManyFields,
}

impl BodyShape {
    /// Short, filename/label-safe name for artifacts and printed summaries.
    fn label(self) -> &'static str {
        match self {
            BodyShape::Tiny => "tiny",
            BodyShape::BigField => "big-field",
            BodyShape::ManyFields => "many-fields",
        }
    }

    /// Requests to drive for this shape (see [`TOTAL_BIG_FIELD`]'s doc for why
    /// [`BodyShape::BigField`] differs).
    fn total(self) -> u64 {
        match self {
            BodyShape::Tiny | BodyShape::ManyFields => TOTAL,
            BodyShape::BigField => TOTAL_BIG_FIELD,
        }
    }

    /// This shape's JSON fields (no enclosing braces, no `tenant_id`/`_tenant`/
    /// `id`), merged into each side's envelope by [`Side::request`].
    fn fields_json(self) -> String {
        match self {
            BodyShape::Tiny => r#""msg":"x""#.to_owned(),
            BodyShape::BigField => format!(r#""payload":"{}""#, "x".repeat(BIG_FIELD_BYTES)),
            BodyShape::ManyFields => MANY_FIELDS_JSON.to_owned(),
        }
    }
}

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;
type Handler = AppHandler<ReferenceAuthenticator>;

/// Starts a single-node OpenSearch (security disabled) and returns its base URL.
async fn start_opensearch() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("opensearchproject/opensearch", "2.11.1")
        .with_exposed_port(ContainerPort::Tcp(9200))
        .with_wait_for(WaitFor::message_on_stdout("] started"))
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .with_env_var("bootstrap.memory_lock", "false")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(9200).await.unwrap();
    (container, format!("http://{host}:{port}"))
}

/// Polls cluster health until OpenSearch answers; returns readiness.
async fn wait_ready(client: &HttpClient, base: &str) -> bool {
    for _ in 0..60 {
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("{base}/_cluster/health"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        if let Ok(resp) = client.request(req).await {
            if resp.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    false
}

/// Spawns the proxy (real [`OpenSearchSink`] to `upstream`) and returns its base
/// URL plus a handle to its handler, so the run can read upstream `pool_stats`.
async fn spawn_proxy(upstream: String) -> (String, Arc<Handler>) {
    let cluster = ClusterId::from(CLUSTER);
    let sink = OpenSearchSink::new();
    let tenancy = ReferenceTenancy::new(cluster, IndexName::from(INDEX), upstream);
    let handler = Arc::new(
        AppHandler::new(
            Pipeline::new(TenancyRouter::new(tenancy), sink),
            ReferenceAuthenticator::dev(),
        )
        .with_require_tls_for_mutation(false),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = handler.clone();
    tokio::spawn(async move {
        let _ = osproxy_transport::serve(listener, serving).await;
    });
    (format!("http://{addr}"), handler)
}

/// Which side of the comparison a request targets.
///
/// Fairness rests on the two sides issuing the **same upstream operation** to
/// OpenSearch, so the only difference is the proxy hop. The reference tenancy
/// constructs the doc id and routing, so the proxy's *upstream* call for
/// `POST /orders/_doc {tenant_id,id}` is a `PUT /{INDEX}/_doc/{partition}:{id}
/// ?routing={partition}` with the injected `_tenant` field, exactly the shape
/// [`Side::Direct`] sends straight to the cluster. Each side uses a distinct
/// partition only to avoid colliding on ids; both re-write their own warmed ids,
/// so both runs are version-updates (symmetric), not create-vs-update.
#[derive(Clone)]
enum Side {
    /// Straight to OpenSearch: the exact `PUT`-by-physical-id-with-routing the
    /// proxy emits upstream, the no-proxy baseline NFR-P1/P2 measure against.
    Direct(String),
    /// Through the proxy: the logical `POST /orders/_doc` a client sends; the
    /// proxy classifies, resolves, rewrites, and dispatches the upstream `PUT`.
    Proxy(String),
}

impl Side {
    fn request(&self, i: u64, shape: BodyShape) -> Request<Full<Bytes>> {
        let fields = shape.fields_json();
        let (method, url, body) = match self {
            Side::Direct(os) => (
                Method::PUT,
                format!("{os}/{INDEX}/_doc/base:{i}?routing=base"),
                format!(r#"{{"_tenant":"base","id":{i},{fields}}}"#),
            ),
            Side::Proxy(proxy) => (
                Method::POST,
                format!("{proxy}/orders/_doc"),
                format!(r#"{{"tenant_id":"prox","id":{i},{fields}}}"#),
            ),
        };
        Request::builder()
            .method(method)
            .uri(url)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap()
    }
}

/// Drives `total` requests of `shape` against `side` at `concurrency`,
/// returning every request's latency in nanoseconds (measured via
/// [`SystemClock`]) and the wall-clock the whole run took (for throughput).
async fn drive(
    client: &HttpClient,
    side: Side,
    concurrency: u32,
    total: u64,
    shape: BodyShape,
    clock: &Arc<dyn Clock>,
) -> (Vec<u64>, Duration) {
    let next = Arc::new(AtomicU64::new(0));
    let run_start = clock.now();
    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let client = client.clone();
        let side = side.clone();
        let next = next.clone();
        let clock = clock.clone();
        workers.push(tokio::spawn(async move {
            let mut samples = Vec::new();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }
                let t0 = clock.now();
                let ok = match client.request(side.request(i, shape)).await {
                    Ok(resp) => drain(resp).await,
                    Err(_) => false,
                };
                let dt = clock.now().saturating_duration_since(t0);
                if ok {
                    samples.push(u64::try_from(dt.as_nanos()).unwrap_or(u64::MAX));
                }
            }
            samples
        }));
    }
    let mut all = Vec::new();
    for w in workers {
        all.extend(w.await.unwrap());
    }
    let elapsed = clock.now().saturating_duration_since(run_start);
    (all, elapsed)
}

/// Reads and discards a response body, reporting whether the status was 2xx.
async fn drain(resp: Response<hyper::body::Incoming>) -> bool {
    let ok = resp.status().is_success();
    let _ = resp.into_body().collect().await;
    ok
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with --ignored --nocapture"]
async fn nfr_p_profile_against_real_opensearch() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    assert!(wait_ready(&client, &os_base).await, "opensearch not ready");
    let (proxy_base, handler) = spawn_proxy(os_base.clone()).await;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    // Same proxy-vs-baseline comparison, three body shapes: the tiny fixed
    // envelope (the original profile), a single large string field (the shape
    // that motivated osproxy-core::json's vectorized scan), and a typical
    // multi-field document. Each gets its own artifact + verdict, so a
    // regression confined to one shape isn't averaged away by the others.
    for shape in [BodyShape::Tiny, BodyShape::BigField, BodyShape::ManyFields] {
        let profile =
            measure_profile(&client, &handler, &os_base, &proxy_base, shape, &clock).await;

        // Emit the artifact (the thing an LLM judges) + the verdict.
        let verdict = judge(&profile, &NfrThresholds::provisional());
        report_profile(shape, &profile, &verdict);

        // Host-independent invariant worth gating even on a noisy box (completeness
        // is already asserted above): the proxy keeps its upstream connections warm
        // (NFR-P5 / NFR-P4) rather than churning one per request. The *latency*
        // numbers are recorded for calibration, not asserted (they are host-bound
        // and the thresholds are still provisional).
        assert!(
            profile.pool_reuse_rate >= 0.90,
            "[{}] upstream pool should reuse connections under load, got {:.4}",
            shape.label(),
            profile.pool_reuse_rate
        );
    }
}

/// Concurrency levels the scalability sweep drives the proxy at, ascending.
const SWEEP: &[u32] = &[1, 8, 32, 64];
/// Requests per sweep point, smaller than the single-point profile so a
/// four-point sweep still finishes in seconds.
const SWEEP_TOTAL: u64 = 800;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires Docker; run with --ignored --nocapture"]
async fn nfr_p_scalability_curve_against_real_opensearch() {
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (_container, os_base) = start_opensearch().await;
    assert!(wait_ready(&client, &os_base).await, "opensearch not ready");
    let (proxy_base, _handler) = spawn_proxy(os_base).await;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    // Warm the pool once so the lightest-load point isn't paying cold-handshake
    // and index-creation cost (which would understate amplification). Body shape
    // is fixed at Tiny here: this test's axis is concurrency, not body shape (see
    // `nfr_p_profile_against_real_opensearch` for the shape comparison).
    let _ = drive(
        &client,
        Side::Proxy(proxy_base.clone()),
        8,
        SWEEP_TOTAL,
        BodyShape::Tiny,
        &clock,
    )
    .await;

    let curve = sweep_curve(&client, &proxy_base, &clock).await;
    let verdict = judge_scalability(&curve, &ScalabilityThresholds::provisional());
    report_curve(&curve, &verdict);

    // Host-independent invariant: serving more concurrency must buy more
    // throughput, the proxy isn't serializing requests behind its pool. The
    // exact tail-amplification factor is host-bound, so it is recorded (and judged
    // against provisional bounds) but not hard-asserted here.
    assert!(
        curve.throughput_scaling() > 1.0,
        "added concurrency should raise throughput, scaling = {:.2}x",
        curve.throughput_scaling()
    );
}

/// Warms both paths, runs the timed baseline + proxy passes, and assembles the
/// single-operating-point profile (with steady-state reuse from `pool_stats`
/// snapshots diffed around the timed proxy run), for one [`BodyShape`].
async fn measure_profile(
    client: &HttpClient,
    handler: &Arc<Handler>,
    os_base: &str,
    proxy_base: &str,
    shape: BodyShape,
    clock: &Arc<dyn Clock>,
) -> NfrProfile {
    let total = shape.total();
    // Warm both paths so the timed runs see a steady-state pool (and the index
    // exists), not first-request handshake + index-creation cost.
    let direct = || Side::Direct(os_base.to_owned());
    let proxy = || Side::Proxy(proxy_base.to_owned());
    let _ = drive(client, direct(), CONCURRENCY, total, shape, clock).await;
    let _ = drive(client, proxy(), CONCURRENCY, total, shape, clock).await;

    // The pool's reuse counters are cumulative and not resettable, so we snapshot
    // them before and after the timed proxy run and diff, warmup opens then fall
    // outside the window and don't skew the steady-state reuse rate.
    let cluster = ClusterId::from(CLUSTER);
    let before = handler.pipeline().sink().pool_stats(&cluster);
    let (base_ns, _) = drive(client, direct(), CONCURRENCY, total, shape, clock).await;
    let (proxy_ns, proxy_elapsed) = drive(client, proxy(), CONCURRENCY, total, shape, clock).await;
    let reuse_rate = steady_reuse_rate(before, handler.pipeline().sink().pool_stats(&cluster));

    // Both summaries must be complete before they're compared: a dropped request
    // contributes no sample, which would *shrink* a side and flatter its
    // percentiles. The baseline is the subtrahend in added-latency.
    assert_eq!(base_ns.len() as u64, total, "every baseline write succeeds");
    assert_eq!(proxy_ns.len() as u64, total, "every proxy write succeeds");
    let baseline = LatencySummary::from_nanos(&base_ns).expect("baseline samples");
    let proxy = LatencySummary::from_nanos(&proxy_ns).expect("proxy samples");
    // Proxy-side sustained rate only (count / wall-clock of the proxy run), a
    // steady-state smoke number, not a proxy-vs-baseline ratio; `judge` leaves it
    // ungated until a target is calibrated.
    let throughput_rps = proxy.count as f64 / proxy_elapsed.as_secs_f64();
    NfrProfile {
        samples: proxy.count,
        concurrency: CONCURRENCY,
        baseline,
        proxy,
        pool_reuse_rate: reuse_rate,
        throughput_rps,
    }
}

/// Writes the profile + verdict JSON to the scratch dir (one file per
/// [`BodyShape`], via its [`BodyShape::label`]) and prints the added-latency /
/// reuse / throughput summary line.
fn report_profile(shape: BodyShape, profile: &NfrProfile, verdict: &osproxy_bench::Verdict) {
    let dir = env!("CARGO_TARGET_TMPDIR");
    let label = shape.label();
    std::fs::write(format!("{dir}/nfr-profile-{label}.json"), profile.to_json()).unwrap();
    std::fs::write(format!("{dir}/nfr-verdict-{label}.json"), verdict.to_json()).unwrap();
    std::fs::write(
        format!("{dir}/nfr-profile-{label}.md"),
        profile_brief(profile, verdict),
    )
    .unwrap();
    println!("NFR-P profile [{label}]:\n{}", profile.to_json());
    println!(
        "[{label}] added p50 = {:.3} ms, added p99 = {:.3} ms, reuse = {:.4}, throughput = {:.0} rps",
        ms(profile.added_p50_ns()),
        ms(profile.added_p99_ns()),
        profile.pool_reuse_rate,
        profile.throughput_rps,
    );
    println!(
        "verdict (provisional thresholds) [{label}]:\n{}",
        verdict.to_json()
    );
}

/// Drives the proxy at each [`SWEEP`] concurrency level and assembles the curve.
async fn sweep_curve(
    client: &HttpClient,
    proxy_base: &str,
    clock: &Arc<dyn Clock>,
) -> ScalabilityCurve {
    let mut points = Vec::new();
    for &c in SWEEP {
        let side = Side::Proxy(proxy_base.to_owned());
        let (ns, elapsed) = drive(client, side, c, SWEEP_TOTAL, BodyShape::Tiny, clock).await;
        assert_eq!(ns.len() as u64, SWEEP_TOTAL, "all writes at c={c} succeed");
        let latency = LatencySummary::from_nanos(&ns).expect("samples");
        let throughput_rps = latency.count as f64 / elapsed.as_secs_f64();
        points.push(ScalabilityPoint {
            concurrency: c,
            latency,
            throughput_rps,
        });
    }
    ScalabilityCurve::new(points).expect("non-empty sweep")
}

/// Writes the curve + verdict JSON to the scratch dir and prints the per-point
/// trend and the amplification/scaling summary.
fn report_curve(curve: &ScalabilityCurve, verdict: &osproxy_bench::Verdict) {
    let dir = env!("CARGO_TARGET_TMPDIR");
    let curve_json = serde_json::to_string_pretty(curve).unwrap();
    std::fs::write(format!("{dir}/nfr-scalability.json"), &curve_json).unwrap();
    std::fs::write(
        format!("{dir}/nfr-scalability-verdict.json"),
        verdict.to_json(),
    )
    .unwrap();
    std::fs::write(
        format!("{dir}/nfr-scalability.md"),
        scalability_brief(curve, verdict),
    )
    .unwrap();
    println!("scalability curve:\n{curve_json}");
    for p in &curve.points {
        println!(
            "c={:>3}: p50 {:.3} ms, p99 {:.3} ms, {:.0} rps",
            p.concurrency,
            ms(p.latency.p50_ns),
            ms(p.latency.p99_ns),
            p.throughput_rps,
        );
    }
    println!(
        "tail amplification = {:.2}x, throughput scaling = {:.2}x\nverdict (provisional):\n{}",
        curve.tail_amplification(),
        curve.throughput_scaling(),
        verdict.to_json(),
    );
}

/// Steady-state reuse rate from two `pool_stats` snapshots around the timed run:
/// reused dispatches over total dispatches *in that window*. Missing stats (no
/// dispatch yet) reads as zero reuse.
fn steady_reuse_rate(
    before: Option<osproxy_sink::PoolStats>,
    after: Option<osproxy_sink::PoolStats>,
) -> f64 {
    let (Some(b), Some(a)) = (before, after) else {
        return 0.0;
    };
    let dispatched = a.dispatched.saturating_sub(b.dispatched);
    let opened = a.opened.saturating_sub(b.opened);
    if dispatched == 0 {
        return 0.0;
    }
    let reused = dispatched.saturating_sub(opened);
    reused as f64 / dispatched as f64
}

/// Nanoseconds as milliseconds, for the human-readable summary line.
fn ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `BodyShape`, merged into both sides' envelopes, must be valid
    /// JSON: unlike the rest of this file, this doesn't need Docker, so it
    /// runs in the normal (non-`--ignored`) gate — a future shape (e.g. a
    /// string field with an unescaped `"`) would otherwise only be caught by
    /// someone remembering to run the `--ignored` e2e suite.
    #[tokio::test]
    async fn every_body_shape_produces_valid_json_for_both_sides() {
        for shape in [BodyShape::Tiny, BodyShape::BigField, BodyShape::ManyFields] {
            for side in [
                Side::Direct("http://os".to_owned()),
                Side::Proxy("http://proxy".to_owned()),
            ] {
                let req = side.request(0, shape);
                let body = req
                    .into_body()
                    .collect()
                    .await
                    .expect("Full<Bytes> collects synchronously")
                    .to_bytes();
                let result = serde_json::from_slice::<serde_json::Value>(&body);
                assert!(
                    result.is_ok(),
                    "{}/{shape:?}: invalid JSON: {:?}\nbody={body:?}",
                    side_label(&side),
                    result.err()
                );
            }
        }
    }

    fn side_label(side: &Side) -> &'static str {
        match side {
            Side::Direct(_) => "Direct",
            Side::Proxy(_) => "Proxy",
        }
    }
}
