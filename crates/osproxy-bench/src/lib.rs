//! NFR-P performance-profile vocabulary (`docs/01` §NFR-P, `docs/11` M4).
//!
//! The performance NFRs that are not allocation- or instruction-counts,
//! *added latency over direct-to-cluster*, *steady-state throughput*, *pool
//! reuse rate*, cannot be judged from a microbenchmark of a transform function.
//! They need a load run of the **whole proxy against a real cluster**, compared
//! to talking to that cluster **directly** (the baseline). This crate is the
//! deterministic spine of that track:
//!
//! - [`LatencySummary`], percentiles (nearest-rank) over a set of latency
//!   samples. Pure arithmetic; the same samples always yield the same summary.
//! - [`NfrProfile`], a machine-readable proxy-vs-baseline profile: the artifact
//!   a load run *produces* and an operator (or an LLM) *reads*. The proxy's
//!   added latency is **derived** here, not measured, so the comparison is
//!   defined in exactly one place.
//! - [`judge()`], scores a profile against [`NfrThresholds`], emitting a per-NFR
//!   [`Verdict`]. This is the automated gate: a load run that exceeds the added-
//!   latency or reuse-rate budget fails it, with a finding that names the NFR.
//! - [`ScalabilityCurve`] + [`judge_scalability`], the *shape* under rising
//!   concurrency: a sweep of [`LatencySummary`] whose tail-amplification and
//!   throughput-scaling are judged against NFR-P2 ("no tail amplification from
//!   pooling"). Where [`NfrProfile`] is one operating point, this is the trend.
//! - [`FootprintProfile`] + [`judge_footprint`], the proxy's resident set when
//!   idle and after a soak, judged against NFR-P6 (bounded idle footprint; no
//!   unbounded buffers/queues, the growth ratio is the leak guard).
//! - [`profile_brief`] / [`scalability_brief`] / [`footprint_brief`], render the
//!   same numbers as a Markdown brief: the consolidated view an operator reads in
//!   a CI summary, or an LLM judges in prose (the JSON is for a programmatic gate).
//!
//! **Shape-only, like the rest of osproxy's observability:** a profile carries
//! timings and counts, never request bodies, tenant values, or cluster
//! identities. It is safe to log, ship to a collector, or hand to a judge.
//!
//! The Docker-backed load runner that fills a profile in (spawning the proxy and
//! a direct baseline against one testcontainer OpenSearch) is a separate,
//! environment-gated slice that sits *on top of* this vocabulary, kept out so
//! these pure types gate in CI with no Docker.
#![deny(missing_docs)]

mod footprint;
mod judge;
mod profile;
mod report;
mod scale;
mod summary;

pub use footprint::{judge_footprint, FootprintProfile, FootprintThresholds};
pub use judge::{judge, Finding, NfrThresholds, Verdict};
pub use profile::NfrProfile;
pub use report::{brief_header, footprint_brief, profile_brief, scalability_brief};
pub use scale::{judge_scalability, ScalabilityCurve, ScalabilityPoint, ScalabilityThresholds};
pub use summary::LatencySummary;
