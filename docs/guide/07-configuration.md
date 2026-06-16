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
| `cursor_affinity_key` | *(unset → affinity off)* | Shared HMAC key that signs the cluster-in-cursor envelope so a continued scroll/PIT routes to its pinned cluster across the fleet with no shared store. **The same key must be set on every instance.** Unset ⇒ cursor requests fail closed. |

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
