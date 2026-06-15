//! Injecting W3C trace-context headers onto an upstream request — the single
//! propagation choke point shared by the write, read, and query paths.

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{HeaderName, HeaderValue};
use hyper::Request;
use osproxy_core::TraceContext;

/// Adds the proxy's `traceparent` and, when the request carried one, the caller's
/// `tracestate` (verbatim — the proxy adds no entry) to `req`. A `None` trace or a
/// header value that is not valid ASCII is silently skipped (propagation is
/// best-effort and never fails a request).
pub(crate) fn inject_trace(req: &mut Request<Full<Bytes>>, trace: Option<&TraceContext>) {
    let Some(t) = trace else { return };
    for (name, value) in [
        ("traceparent", Some(t.to_traceparent())),
        ("tracestate", t.to_tracestate().map(str::to_owned)),
    ] {
        if let Some(v) = value.and_then(|s| HeaderValue::from_str(&s).ok()) {
            req.headers_mut().insert(HeaderName::from_static(name), v);
        }
    }
}
