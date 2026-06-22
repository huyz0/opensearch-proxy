// Tests for the streaming-search body glue ([`super::shape_hits_stream`] and the
// `unfold`/`StreamBody` frame pump). The pure transform is covered exhaustively
// by `search_scan_tests`; these pin the *streaming* layer — multi-frame
// reassembly across arbitrary frame boundaries, the empty-output-then-continue
// case, the end-of-stream tail, and upstream error propagation.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::read::{shape_hits, ReadShape};
use osproxy_core::FieldName;
use osproxy_sink::{BodyError, ByteBody};
use osproxy_spi::{DocIdRule, IdTemplate};
use serde_json::Value;

fn make_shape() -> ReadShape {
    ReadShape {
        inject_names: vec![FieldName::from("_tenant")],
        id_rule: Some(DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true)),
    }
}

fn shaper() -> HitShaper {
    HitShaper {
        logical_index: "orders".to_owned(),
        partition: "acme".to_owned(),
        shape: make_shape(),
    }
}

/// A response with two hits and a sibling `aggregations`, used across the cases.
const BODY: &[u8] = br#"{"took":5,"hits":{"total":{"value":2},"hits":[
    {"_index":"shared","_id":"acme:7","_routing":"acme","_source":{"_tenant":"acme","msg":"hi"}},
    {"_index":"shared","_id":"acme:8","_routing":"acme","_source":{"_tenant":"acme","msg":"yo"}}
]},"aggregations":{"by_day":{"buckets":[{"key":1,"doc_count":9}]}}}"#;

/// Builds a [`ByteBody`] that yields `chunks` as separate data frames — the
/// realistic multi-frame upstream the engine glue must reassemble.
fn body_from_chunks(chunks: Vec<Vec<u8>>) -> ByteBody {
    let frames = chunks
        .into_iter()
        .map(|c| Ok::<_, BodyError>(Frame::data(Bytes::from(c))));
    StreamBody::new(stream::iter(frames)).boxed_unsync()
}

/// Splits `body` into `n` near-equal chunks (frame boundaries fall mid-hit,
/// mid-string, etc., depending on `n`).
fn split_into(body: &[u8], n: usize) -> Vec<Vec<u8>> {
    let n = n.max(1);
    let step = body.len().div_ceil(n);
    body.chunks(step.max(1)).map(<[u8]>::to_vec).collect()
}

async fn drive(body: ByteBody) -> Result<Vec<u8>, BodyError> {
    Ok(shape_hits_stream(body, shaper())
        .collect()
        .await?
        .to_bytes()
        .to_vec())
}

#[tokio::test]
async fn reassembles_across_arbitrary_frame_boundaries() {
    // The buffered oracle is the source of truth; the streamed output must match it
    // semantically no matter how the upstream chunks the body — including
    // single-byte frames, which exercise every mid-token boundary and force many
    // empty-output-then-continue iterations of the frame pump.
    let oracle = shape_hits(BODY, "orders", "acme", &make_shape()).expect("oracle ok");
    let oracle_val: Value = serde_json::from_slice(&oracle).unwrap();

    for n in [1, 2, 3, 7, 16, BODY.len()] {
        let out = drive(body_from_chunks(split_into(BODY, n))).await.unwrap();
        let out_val: Value =
            serde_json::from_slice(&out).unwrap_or_else(|e| panic!("not json for n={n}: {e}"));
        assert_eq!(out_val, oracle_val, "streamed != oracle for n={n} frames");
        assert!(
            !out.windows(7).any(|w| w == b"_tenant"),
            "injected field leaked (n={n})"
        );
    }
}

#[tokio::test]
async fn single_byte_frames_emit_each_hit_only_once() {
    // Byte-at-a-time framing is the worst case for the "emit nothing until a hit
    // closes" pump; the result must still contain each shaped hit exactly once.
    let out = drive(body_from_chunks(split_into(BODY, BODY.len())))
        .await
        .unwrap();
    let s = String::from_utf8(out).unwrap();
    assert_eq!(s.matches(r#""msg":"hi""#).count(), 1);
    assert_eq!(s.matches(r#""msg":"yo""#).count(), 1);
    // Logical view: physical id mapped back, logical index presented.
    let v: Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["hits"]["hits"][0]["_id"], "7");
    assert_eq!(v["hits"]["hits"][0]["_index"], "orders");
}

#[tokio::test]
async fn empty_upstream_frames_are_skipped() {
    // A zero-length data frame (legal from hyper) must not terminate the stream or
    // corrupt output — the pump loops to the next frame.
    let chunks = vec![
        BODY[..20].to_vec(),
        Vec::new(),
        BODY[20..].to_vec(),
        Vec::new(),
    ];
    let oracle = shape_hits(BODY, "orders", "acme", &make_shape()).unwrap();
    let out = drive(body_from_chunks(chunks)).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&out).unwrap(),
        serde_json::from_slice::<Value>(&oracle).unwrap()
    );
}

#[tokio::test]
async fn upstream_error_propagates_as_a_stream_error() {
    // An upstream error frame mid-body must surface as a body error (hyper then
    // resets the response) rather than be silently swallowed or panicked on — and
    // it must not yield truncated-but-OK bytes.
    let frames: Vec<Result<Frame<Bytes>, BodyError>> = vec![
        Ok(Frame::data(Bytes::from(BODY[..30].to_vec()))),
        Err("upstream reset".into()),
    ];
    let body = StreamBody::new(stream::iter(frames)).boxed_unsync();
    let result = drive(body).await;
    assert!(result.is_err(), "upstream error must propagate, got Ok");
}
