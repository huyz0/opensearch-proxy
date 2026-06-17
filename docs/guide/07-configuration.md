# 7. Configuration

Configuration is typed and fully validated at startup, before any socket opens. An
invalid value is a typed error naming the exact field, so an operator (or an LLM) can
fix it immediately. Defaults are applied once, centrally.

## Sources and precedence

Settings are merged from three layers, **lowest to highest**:

```
config file   <   environment   <   command-line flags
```

| Source | Form | Example |
|--------|------|---------|
| File | `key = value` lines; path from `OSPROXY_CONFIG` | `bind = 0.0.0.0:8080` |
| Environment | `OSPROXY_<KEY>` (key upper-cased) | `OSPROXY_BIND=0.0.0.0:8080` |
| Flag | `--key value` | `--bind 0.0.0.0:8080` |

The canonical key is `snake_case`; the env var is `OSPROXY_` + the upper-cased key
(e.g. `bind` → `OSPROXY_BIND`).

## Settings reference

### Networking

| Key (`OSPROXY_…`) | Default | Description |
|-------------------|---------|-------------|
| `bind` | `127.0.0.1:8080` | The `host:port` the HTTP (h1/h2) ingress listens on. |
| `grpc_bind` | *(unset)* | If set, also serve **gRPC** ingress on this `host:port` (same handler). |
| `upstream` | `http://127.0.0.1:9200` | Base URL of the OpenSearch cluster the reference wiring routes to. |
| `index` | `osproxy-shared` | The physical shared index the reference tenancy targets. |

### Authentication & TLS

| Key (`OSPROXY_…`) | Default | Description |
|-------------------|---------|-------------|
| `tokens` | *(empty → dev open)* | `token=principal` entries (comma/whitespace separated). **Empty means dev mode: any caller is accepted**, never for production. |
| `allow_cleartext_mutation` | `false` | When `false` (default), body-mutating requests over cleartext are **refused** (NFR-S1). Set `true` only on a trusted network. |
| `tls_cert` | *(unset)* | Path to the server certificate PEM. Set together with `tls_key` to enable TLS. |
| `tls_key` | *(unset)* | Path to the server private-key PEM. Both-or-neither with `tls_cert`. |
| `tls_client_ca` | *(unset)* | Path to a client-CA PEM. Setting it requires **mutual TLS**: clients must present a cert chaining to this CA. Only valid alongside `tls_cert`/`tls_key`. |

> TLS is on when `tls_cert` + `tls_key` are configured; cleartext otherwise. The same
> provider terminates the HTTP and gRPC listeners. The crypto module (ring vs.
> FIPS aws-lc-rs) is chosen at **build time**, not here. See [FIPS & Crypto](../07-fips-and-crypto.md).

### Observability & diagnostics

| Key (`OSPROXY_…`) | Default | Description |
|-------------------|---------|-------------|
| `log_requests` | `false` | Emit one structured JSON log line per request (the shape-only explain doc, carrying `trace_id`). |
| `otlp_endpoint` | *(unset → export off)* | OTLP collector base URL (e.g. `http://otel-collector:4318`). When set, shape-only spans are exported; when unset, export costs nothing. |
| `service_name` | `osproxy` | The `service.name` reported on exported spans. |
| `diag_baseline` | `shape` | Baseline diagnostics verbosity before any directive: `off` \| `shape` \| `shape-timing` \| `shape-rewrite-diff`. Pair `off` with the directive channels for targeted, sampled diagnostics. |
| `debug_directive_key` | *(unset)* | Shared HMAC key that verifies signed `X-Debug-Directive` headers. Unset ⇒ that channel rejects everything. |
| `directive_admin_token` | *(unset → disabled)* | Bearer token gating `POST`/`GET /admin/directives`. Unset ⇒ the endpoint reports `not_enabled`. |
| `debug_endpoints` | `true` | Whether the pre-auth `/debug/explain` and `/debug/breakglass` surfaces are served. **Set `false` in production** so operational metadata is not exposed unauthenticated. `/metrics` stays on regardless. |

### Control plane & routing

| Key (`OSPROXY_…`) | Default | Description |
|-------------------|---------|-------------|
| `admin_passthrough_cluster` | *(unset → admin rejected)* | The cluster that answers allow-listed admin (`_cat`/`_cluster`/`_nodes`) requests. Unset ⇒ all admin requests are rejected (fail-closed). |
| `admin_passthrough_prefixes` | `/_cat/,/_cluster/,/_nodes/` | Comma-separated allow-list of admin path prefixes (only meaningful with `admin_passthrough_cluster`). |
| `admin_passthrough_endpoint` | *(unset → tenancy lookup)* | Base URL of the admin cluster. The admin cluster is operator infrastructure, not a tenancy placement, so its endpoint is set here; unset falls back to the tenancy's `cluster_endpoint` for that cluster id. |
| `cursor_affinity_key` | *(unset → affinity off)* | Shared HMAC key that signs the cluster-in-cursor envelope so a continued scroll/PIT routes to its pinned cluster across the fleet with no shared store. **The same key must be set on every instance.** Unset ⇒ cursor requests fail closed. |
| `passthrough_cluster` | *(unset → tenancy mode)* | Tenant-agnostic mode: forward every request verbatim to this cluster id with no tenancy rewrite (a transparent / capture proxy). Requires `passthrough_endpoint`. |
| `passthrough_endpoint` | *(unset)* | The passthrough cluster's base URL. Both-or-neither with `passthrough_cluster`. |

### Traffic capture (Kafka)

Full-fidelity capture tees every request and response to a Kafka topic for replay
or audit. The captured stream carries bodies and values verbatim, so it is
privileged: it stays off until configured, and the `Authorization` header is
stripped unless you opt out. These keys need a binary built with the
`capture-kafka` feature (`cargo build -p osproxy-server --features capture-kafka`);
setting them on a binary built without it is a loud startup error, not a silent
no-op.

The sink (where captured traffic goes) and the switch (when to capture) are
separate. The keys below wire the **sink**; capture stays off until the switch is
on, which is either `capture_default = true` or a published `capture` directive
(see [Observability](08-observability.md) — capture is on demand through the same
control store as diagnostics, so you flip it fleet-wide with no restart).

| Key (`OSPROXY_…`) | Default | Description |
|-------------------|---------|-------------|
| `capture_default` | `false` | The capture switch's baseline. `false` = on demand (nothing is teed until a `capture` directive selects requests). `true` = always-capture, for a dedicated capture/migration proxy. Independent of the sink keys below. |
| `capture_kafka_brokers` | *(unset → no sink)* | Comma-separated Kafka bootstrap brokers (`host:port`). Both-or-neither with `capture_topic`. |
| `capture_topic` | *(unset)* | The topic each captured exchange envelope is produced to. |
| `capture_redact` | `true` | Strip the `Authorization` header from the captured stream. Set `false` only when the stream consumer needs credentials and is itself secured. |
| `capture_kafka_ca` | *(unset → plaintext)* | Path to the CA PEM the broker certificate must chain to. Present ⇒ TLS to the brokers with that CA pinned; absent ⇒ a plaintext broker connection. |
| `capture_kafka_client_cert` | *(unset)* | Client certificate chain PEM for broker mTLS. Both-or-neither with `capture_kafka_client_key`, and requires `capture_kafka_ca`. |
| `capture_kafka_client_key` | *(unset)* | Client private key PEM for broker mTLS. |
| `capture_max_inflight` | `1024` | The reliability/latency dial: most records buffered + retrying at once before a produce is dropped, bounding memory. Higher = fewer drops under load, more memory. |
| `capture_max_attempts` | `4` | Send attempts per record before giving up. Higher = better delivery odds across a transient broker blip. Delivery is bounded in-memory best-effort, not durable across a restart. |
| `capture_backoff_ms` | `50` | First retry backoff in milliseconds; doubles after each failure. |
| `capture_wal_dir` | *(unset → in-memory)* | Directory for the durable on-disk spill buffer. Set it for **at-least-once** capture that survives a restart: records persist to a write-ahead log and replay until the broker acknowledges. Unset = bounded in-memory best-effort (the `max_inflight`/`max_attempts` knobs above). |
| `capture_wal_max_bytes` | `268435456` (256 MiB) | Cap on undelivered bytes in the spill buffer before new records are dropped (only with `capture_wal_dir`). Bounds disk like `capture_max_inflight` bounds memory. |

Two delivery tiers: without `capture_wal_dir`, delivery is bounded in-memory
best-effort (a broker outage past the buffer drops records, and a restart loses
the buffer). With it, delivery is durable at-least-once — records survive a
restart and replay until acknowledged, so the consumer dedupes on the request id
the envelope carries. Durability is group-commit: a hard power loss can lose the
last sub-second of appended-but-undelivered records; a graceful restart loses
nothing.

## Worked examples

### Local development (cleartext, open auth, full debug)

```bash
OSPROXY_BIND=127.0.0.1:8080 \
OSPROXY_UPSTREAM=http://127.0.0.1:9200 \
OSPROXY_ALLOW_CLEARTEXT_MUTATION=true \
osproxy
```

### Production (mTLS, token auth, diagnostics off until targeted)

```bash
OSPROXY_BIND=0.0.0.0:8443 \
OSPROXY_TLS_CERT=/etc/osproxy/server.crt \
OSPROXY_TLS_KEY=/etc/osproxy/server.key \
OSPROXY_TLS_CLIENT_CA=/etc/osproxy/client-ca.crt \
OSPROXY_TOKENS='svc-ingest=ingest,svc-read=reader' \
OSPROXY_DIAG_BASELINE=off \
OSPROXY_DEBUG_ENDPOINTS=false \
OSPROXY_DEBUG_DIRECTIVE_KEY="$DIRECTIVE_HMAC_KEY" \
OSPROXY_DIRECTIVE_ADMIN_TOKEN="$ADMIN_TOKEN" \
OSPROXY_OTLP_ENDPOINT=http://otel-collector:4318 \
OSPROXY_CURSOR_AFFINITY_KEY="$FLEET_CURSOR_KEY" \
osproxy
```

### Config file

```ini
# /etc/osproxy/osproxy.conf  (referenced via OSPROXY_CONFIG)
bind = 0.0.0.0:8443
upstream = https://opensearch.internal:9200
diag_baseline = off
admin_passthrough_cluster = ops-1
```

```bash
OSPROXY_CONFIG=/etc/osproxy/osproxy.conf osproxy
```

## What is *not* configured here

Anything that changes per request at runtime (the live placement table and the
diagnostics directives) flows through the control plane, not the config file. See
[Observability & Control Plane](08-observability.md).

→ [Observability & Control Plane](08-observability.md)
