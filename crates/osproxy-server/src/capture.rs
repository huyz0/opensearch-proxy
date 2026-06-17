//! Wiring for full-fidelity Kafka traffic capture.
//!
//! Capture is opt-in twice over: it links no broker client unless the binary is
//! built with the `capture-kafka` feature, and even then it stays off until
//! `capture_kafka_brokers`/`capture_topic` are configured. The captured stream
//! carries bodies and values verbatim, so it is privileged: the `Authorization`
//! header is redacted unless explicitly kept. See `docs/guide/07-configuration.md`.

use osproxy_config::Config;
use osproxy_server::handler::AppHandler;
use osproxy_spi::Authenticator;

/// Attaches Kafka capture when configured. Builds the portable krafka producer
/// (over TLS/mTLS when `capture_kafka_ca` is set) behind the `Capture` seam,
/// wrapping it in `RedactingCapture` unless `capture_redact=false`.
#[cfg(feature = "capture-kafka")]
pub(crate) async fn attach<A: Authenticator>(
    handler: AppHandler<A>,
    cfg: &Config,
) -> Result<AppHandler<A>, String> {
    use osproxy_capture::{Capture, RedactingCapture};
    use osproxy_kafka::KafkaCapture;
    use osproxy_kafka_krafka::{AuthConfig, KrafkaProducer, TlsConfig as KafkaTlsConfig};

    let Some(cap) = &cfg.capture else {
        return Ok(handler);
    };
    let auth = cap.tls.as_ref().map(|tls| {
        let mut t = KafkaTlsConfig::new()
            .with_ca_cert(&tls.ca_path)
            .with_kafka_alpn();
        if let (Some(cert), Some(key)) = (&tls.client_cert_path, &tls.client_key_path) {
            t = t.with_client_cert(cert, key);
        }
        AuthConfig::ssl(t)
    });
    let producer = KrafkaProducer::connect(cap.brokers.clone(), "osproxy-capture", auth)
        .await
        .map_err(|e| format!("connecting capture producer: {}", e.reason))?;
    let kafka = KafkaCapture::new(producer, cap.topic.clone());
    let capture: Box<dyn Capture> = if cap.redact {
        Box::new(RedactingCapture::new(kafka))
    } else {
        Box::new(kafka)
    };
    println!(
        "osproxy capture: on (kafka topic={}, brokers={}, tls={}, redact={})",
        cap.topic,
        cap.brokers.len(),
        cap.tls.is_some(),
        cap.redact
    );
    Ok(handler.with_capture(capture))
}

/// Without the `capture-kafka` feature no broker client is linked, so a
/// configured capture is a loud startup error rather than a silent no-op.
#[cfg(not(feature = "capture-kafka"))]
#[allow(clippy::unused_async)]
pub(crate) async fn attach<A: Authenticator>(
    handler: AppHandler<A>,
    cfg: &Config,
) -> Result<AppHandler<A>, String> {
    if cfg.capture.is_some() {
        return Err(
            "capture is configured (capture_kafka_brokers/capture_topic) but this binary \
                    was built without the `capture-kafka` feature; rebuild with \
                    `--features capture-kafka`"
                .to_owned(),
        );
    }
    Ok(handler)
}
