//! Wiring for full-fidelity Kafka traffic capture.
//!
//! Capture is opt-in twice over: it links no broker client unless the binary is
//! built with the `kafka` feature, and even then it stays off until
//! `capture_kafka_brokers`/`capture_topic` are configured. The captured stream
//! carries bodies and values verbatim, so it is privileged: the `Authorization`
//! header is redacted unless explicitly kept. See `docs/guide/07-configuration.md`.

use osproxy_config::Config;
use osproxy_server::handler::AppHandler;
use osproxy_spi::Authenticator;

/// Attaches Kafka capture when configured. Builds the portable krafka producer
/// (over TLS/mTLS when `capture_kafka_ca` is set) behind the `Capture` seam,
/// wrapping it in `RedactingCapture` unless `capture_redact=false`.
#[cfg(feature = "kafka")]
pub(crate) async fn attach<A: Authenticator>(
    handler: AppHandler<A>,
    cfg: &Config,
) -> Result<AppHandler<A>, String> {
    use std::time::Duration;

    use osproxy_kafka_krafka::{
        AuthConfig, DeliveryConfig, KrafkaProducer, TlsConfig as KafkaTlsConfig,
    };
    use osproxy_kafka_wal::{DurableProducer, WalConfig};

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
    let krafka = KrafkaProducer::connect(cap.brokers.clone(), "osproxy-capture", auth)
        .await
        .map_err(|e| format!("connecting capture producer: {}", e.reason))?;

    // With a WAL directory, deliver durably (at-least-once, survives restart): the
    // disk buffer owns retry, so the in-memory delivery wrapper is bypassed.
    // Otherwise, bounded in-memory best-effort.
    let capture = if let Some(dir) = &cap.wal_dir {
        let wal = WalConfig {
            max_bytes: cap.wal_max_bytes,
            base_backoff: Duration::from_millis(cap.backoff_ms),
            ..WalConfig::default()
        };
        let durable = DurableProducer::spawn(dir, krafka, wal)
            .map_err(|e| format!("opening capture WAL at {dir}: {e}"))?;
        wrap_capture(durable, cap)
    } else {
        let delivery = DeliveryConfig {
            max_inflight: cap.max_inflight,
            max_attempts: cap.max_attempts,
            base_backoff: Duration::from_millis(cap.backoff_ms),
        };
        wrap_capture(krafka.with_delivery(delivery), cap)
    };
    println!(
        "osproxy capture: on (kafka topic={}, brokers={}, tls={}, redact={}, durable={})",
        cap.topic,
        cap.brokers.len(),
        cap.tls.is_some(),
        cap.redact,
        cap.wal_dir.is_some()
    );
    Ok(handler.with_capture(capture))
}

/// Wraps a producer in a `KafkaCapture`, plus `RedactingCapture` unless capture
/// redaction is opted out. Generic so either the durable or in-memory producer
/// composes through the same path.
#[cfg(feature = "kafka")]
fn wrap_capture<P: osproxy_kafka::Producer + 'static>(
    producer: P,
    cap: &osproxy_config::CaptureConfig,
) -> Box<dyn osproxy_capture::Capture> {
    let kafka = osproxy_kafka::KafkaCapture::new(producer, cap.topic.clone());
    if cap.redact {
        Box::new(osproxy_capture::RedactingCapture::new(kafka))
    } else {
        Box::new(kafka)
    }
}

/// Without the `kafka` feature no broker client is linked, so a
/// configured capture is a loud startup error rather than a silent no-op.
#[cfg(not(feature = "kafka"))]
#[allow(clippy::unused_async)]
pub(crate) async fn attach<A: Authenticator>(
    handler: AppHandler<A>,
    cfg: &Config,
) -> Result<AppHandler<A>, String> {
    if cfg.capture.is_some() {
        return Err(
            "capture is configured (capture_kafka_brokers/capture_topic) but this binary \
                    was built without the `kafka` feature; rebuild with \
                    `--features kafka`"
                .to_owned(),
        );
    }
    Ok(handler)
}
