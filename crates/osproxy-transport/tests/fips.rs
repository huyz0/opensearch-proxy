//! FIPS-build assertions for the aws-lc-rs provider (`docs/07`). These run only
//! under `--features fips` (where the CMVP-validated module is linked); the
//! `non-fips` build compiles this file to nothing. They prove the two things the
//! suite-pinning tests on the `ring` build cannot: that the linked module really
//! reports FIPS mode, and that *every* approved suite is actually present in the
//! FIPS module (not silently dropped to a shorter list).

#![allow(clippy::unwrap_used)]
#![cfg(feature = "fips")]

use osproxy_transport::{AwsLcFipsProvider, CryptoProvider, FIPS_APPROVED_SUITES};

/// A self-signed cert for `localhost` as PEM (cert + key).
fn test_cert() -> (String, String) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    (cert.cert.pem(), cert.key_pair.serialize_pem())
}

#[test]
fn the_fips_build_reports_fips_mode() {
    let (cert, key) = test_cert();
    let provider = AwsLcFipsProvider::from_pem(cert.as_bytes(), key.as_bytes()).unwrap();
    assert!(
        provider.fips_mode(),
        "a `fips` build must link a module operating in FIPS mode"
    );
}

#[test]
fn the_fips_provider_offers_exactly_the_approved_suites() {
    let (cert, key) = test_cert();
    let provider = AwsLcFipsProvider::from_pem(cert.as_bytes(), key.as_bytes()).unwrap();
    let config = provider.server_config();

    let offered: Vec<_> = config
        .crypto_provider()
        .cipher_suites
        .iter()
        .map(tokio_rustls::rustls::SupportedCipherSuite::suite)
        .collect();

    // Every approved suite is actually present in the FIPS module (count match
    // catches a silent shrink), and nothing beyond the approved set is offered.
    assert_eq!(
        offered.len(),
        FIPS_APPROVED_SUITES.len(),
        "the FIPS module must expose every approved suite, no more no less: {offered:?}"
    );
    for suite in &offered {
        assert!(
            FIPS_APPROVED_SUITES.contains(suite),
            "non-approved suite offered by the FIPS build: {suite:?}"
        );
    }
}
