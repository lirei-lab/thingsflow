# Operations

This page collects the runtime material that does not need a standalone guide:
public access, logging, backup/restore, observability, and pilot checks. Use
[INSTALL.md](INSTALL.md) for the full install path and [SECURITY.md](SECURITY.md)
for trust boundaries and key custody.

## Operator Secrets

Pilots and production installs should reference Kubernetes Secrets instead of
inline private Helm values. The chart validates this when `production=true`.

Required baseline Secrets:

| Secret | Keys | Purpose |
|---|---|---|
| `thingsflow-platform-keys` | `jwt-token-signing-key` | Flow Core API/session JWT signing key. |
| `thingsflow-device-jwt` | `device-jwt-es256-private-key-pem-b64`, `previous-public-jwks-b64` | Device JWT signer and rotation overlap keys. |
| `thingsflow-postgres` | `password` | Postgres metadata password. |
| `thingsflow-nats-auth` | `username`, `password` | Internal NATS data-plane credential. |
| `thingsflow-oidc` | `client-secret`, `state-signing-key` | OIDC client and CSRF/state protection when OIDC is enabled. |
| `thingsflow-backup-s3` | `endpoint`, `bucket`, `region`, `access-key`, `secret-key` | Backup target credentials when backups are enabled. |

Generate a pilot Secret manifest with:

```bash
NAMESPACE=thingsflow OUT=/tmp/thingsflow-pilot-secrets.yaml \
  tools/generate-pilot-secrets.sh
kubectl -n thingsflow apply -f /tmp/thingsflow-pilot-secrets.yaml
```

The helper does not create backup credentials unless all `BACKUP_*` variables
are supplied. For a pilot without object storage, set `backup.enabled=false` in
the private overlay until backups are configured.

## Public Access

ThingsFlow is private by default. The chart keeps Flow Core, NATS, GreptimeDB,
Postgres, Bento processors, and RMQTT internal unless an operator explicitly
publishes a surface.

The default posture is:

```yaml
ingress:
  enabled: false
rmqttEdge:
  serviceType: ClusterIP
httpIngest:
  serviceType: ClusterIP
```

The Flow Core Service is always `ClusterIP` — it is fixed in the template and has no
value to override; reach it through the ingress or a port-forward.

In Helm shorthand, the private default is `ingress.enabled=false`; publishing
the browser/API surface requires `ingress.enabled=true`.

Minimal browser/API ingress:

```bash
helm upgrade --install thingsflow ./k8s/helm/thingsflow \
  --namespace thingsflow --create-namespace \
  --set ingress.enabled=true \
  --set ingress.host=thingsflow.example.com \
  --set flowCore.allowedOrigin=https://thingsflow.example.com
```

MQTT exposure is separate. `rmqttEdge.serviceType=LoadBalancer` publishes the
**plaintext** `1883` listener, which is only appropriate on a trusted network. For a
public edge, enable the TLS listener and publish `8883` instead:

```yaml
rmqttEdge:
  tls:
    enabled: true          # adds a TLS listener on 8883
    port: 8883
    secretName: rmqtt-mqtt-tls   # cert-manager Certificate
```

Then expose `8883` with a Service of your choice (the reference cluster uses a
dedicated MetalLB LoadBalancer, `k8s/helm/thingsflow/extra-manifests/rmqtt-lb-service.yaml`),
keeping `1883` internal. Clients assume TLS on 8883, so publishing plaintext there
fails the handshake rather than falling back. Device-side details are in
[MQTT Device Auth](MQTT_DEVICE_AUTH.md).

HTTP telemetry exposure is also separate and must be protected by
JWT-bearing traffic, TLS, and the Envoy JWT verifier. Public UI/API ingress
does not imply public MQTT or HTTP telemetry exposure.

## Logging Policy

Postgres `audit_log` stores platform audit events:

- login/authentication events;
- entity changes;
- provisioning and credential changes;
- device security state changes;
- alarm lifecycle actions;
- administrative and security operations.

Routine telemetry messages, successful broker auth checks, and health checks
are operational metrics/logs, not audit events.

At `LOG_LEVEL=info`, Flow Core emits one access-log line for normal control-plane
requests and keeps high-frequency successful probes/discovery calls quiet:
`/health`, `/ready`, `/api/noauth/device-jwks`,
`/.well-known/thingsflow-device-jwks.json`, and `/api/noauth/oauth2Clients`.
Those paths still log at `WARN` when they fail or exceed the slow-request
threshold, and they reappear at `DEBUG` for a focused investigation.

Operational logs must not include:

- access tokens or refresh tokens;
- Device JWTs or MQTT passwords;
- OIDC client secrets or state signing keys;
- backup credentials;
- full raw telemetry payloads at INFO.

Use metrics, counters, heartbeat summaries, and sampled debug logs for
data-plane diagnosis. Demo and edge generators should emit aggregate publish
rates at INFO; per-device payload examples belong at DEBUG.

## Audit Log

Schema: `audit_log` partitioned `BY RANGE (created_time)`, one child
per month named `audit_log_YYYY_MM`. Created on demand at boot for
current+next 2 months by `audit.EnsurePartitions()`. Daily ticker
extends + drops.

Coverage:

- **Auth events**: `LOGIN` SUCCESS/FAILURE (disabled account, bad
  password), `LOGOUT` SUCCESS — at
  [internal/user/auth_handler.go](https://github.com/lirei-lab/thingsflow/blob/main/flow-core/internal/user/auth_handler.go)
  and [internal/user/crud_user.go](https://github.com/lirei-lab/thingsflow/blob/main/flow-core/internal/user/crud_user.go).
- **Entity CRUD** (ADDED / UPDATED / DELETED): `DEVICE`, `DEVICE_PROFILE`,
  `USER`, `DASHBOARD`, `ASSET`, `ASSET_PROFILE`, `CUSTOMER`,
  `ENTITY_VIEW`. Each handler calls `audit.EntityChange(claims, type, id, name, action)`.

To inspect:

```sql
SELECT created_time, user_name, action_type, action_status, entity_type, entity_name
FROM audit_log
WHERE tenant_id = '<tenant-uuid>'
ORDER BY created_time DESC LIMIT 50;

SELECT inhrelid::regclass FROM pg_inherits
WHERE inhparent = 'audit_log'::regclass;
```

The audit writer is async with a 1000-event ring buffer; overflow drops
events and increments `flow_audit_dropped_total` (see [Observability](#observability)).
The `audit` table never extends without retention. This rule is intentional:
audit is operational evidence, not an unbounded event store.

## Observability

Minimum runtime signals for a pilot:

- Flow Core `/ready` and `/metrics`;
- failed `greptimedb-freshness-guard` Jobs — the CronJob runs every 5 minutes and
  fails when no telemetry has landed in `monitoring.freshnessGuard.staleMinutes`
  (15 by default). A failed Job **is** the alert: it is the direct control for a
  silent ingest halt, where pods stay Running and ingest keeps returning 200 while
  nothing reaches the store;
- failed `greptimedb-ttl-guard` Jobs (TTL drift) and `postgres-alarm-retention` Jobs;
- RMQTT connection/auth failures and publish failures;
- NATS JetStream consumer lag;
- NATS KV latest-state freshness;
- GreptimeDB write latency and storage growth;
- Bento processor errors and DLQ counts;
- alarm materializer lag and write failures;
- Postgres connection pool saturation;
- UI dashboard hydration latency.

### Flow Core endpoints and metrics

| Endpoint | Purpose |
|---|---|
| `/health`  | Liveness probe (200 once HTTP server is up). |
| `/ready`   | Readiness probe — pings Postgres pool. 503 during boot or database outage. |
| `/metrics` | Prometheus text format — see counters below. |

Counters / gauges exposed at `/metrics`:

```
flow_postgres_pool_open               ← currently open connections
flow_postgres_pool_in_use             ← connections actively running a query
flow_postgres_pool_wait_count         ← cumulative waits for a free conn (saturation signal)
flow_audit_events_total               ← audit events written
flow_audit_dropped_total              ← audit events dropped (queue full)
flow_transport_allowed_total          ← transport messages accepted under rate cap
```

Logs are slog JSON by default. `LOG_FORMAT=text` for dev,
`LOG_LEVEL=debug|info|warn|error` to gate verbosity. Every HTTP request
logs an `rid=<uuid>` field that matches the response's `X-Request-ID`
header — grab it from the browser's network tab to grep the bridge log
for a specific request.

Which paths stay quiet at INFO, and what must never appear in logs, is defined in
[Logging Policy](OPERATIONS.md#logging-policy).

## Pilot Acceptance Gate

Run the acceptance gate after every chart, image, OIDC, security, data-plane,
or storage change:

```bash
KUBECONFIG=<your-kubeconfig> \
NAMESPACE=thingsflow \
RELEASE=thingsflow \
EXPECTED_OIDC_PROVIDER=zitadel \
tools/verify-pilot-acceptance.sh
```

For a fast configuration and health check without launching a benchmark:

```bash
KUBECONFIG=<your-kubeconfig> \
RUN_BENCHMARK=false \
EXPECTED_LATEST_ROWS=1 \
EXPECTED_HISTORY_ROWS=1 \
tools/verify-pilot-acceptance.sh
```

The gate is intentionally stricter than a smoke test. It validates:

| Area | Check |
|---|---|
| Chart | Controlled pilot values render with Helm. |
| Cleanup | No removed-runtime leftovers are deployed. |
| Secrets | Platform JWT, Device JWT, Postgres, NATS auth, and OIDC Secrets exist. |
| Workloads | RMQTT, NATS, GreptimeDB, Postgres, Bento processors, alarms, Flow Core, and UI roll out. |
| Runtime | Non-job pods are Running with zero unexpected restarts. |
| Posture | `EVENT_BROKER=nats`, Flow Core ingest disabled, data-plane consumer disabled, GreptimeDB history, explicit CORS origin, and `FLOW_ENV=production`. |
| Device security | Device JWKS and public PEM endpoints are reachable. |
| OIDC | `/api/noauth/oauth2Clients` advertises the broker and UserInfo-backed providers are configured. |
| Load | Benchmark publishes the configured message count with bounded errors. |
| Latest state | NATS KV `twin_state` has the expected `DEVICE.*` latest/twin keys. |
| History | GreptimeDB contains historical telemetry rows after the drain window. |

Important variables:

| Variable | Default | Use |
|---|---:|---|
| `RUN_BENCHMARK` | `true` | Set `false` for a quick health/config check. |
| `EXPECTED_DEVICE_COUNT` | `2000` | Device count for the benchmark job. |
| `EXPECTED_PUBLISHED_MIN` | `120000` | Minimum successful publish count. |
| `EXPECTED_ERRORS_MAX` | `0` | Maximum load-generator errors. |
| `EXPECTED_LATEST_ROWS` | `6000` | Minimum NATS KV latest keys. |
| `EXPECTED_HISTORY_ROWS` | `6000` | Minimum GreptimeDB telemetry rows. |
| `POST_BENCHMARK_DRAIN_SECONDS` | `90` | Wait for Bento/GreptimeDB materialization. |
| `OIDC_REQUIRED` | `true` | Require OIDC discovery and `thingsflow-oidc` Secret. |
| `EXPECTED_OIDC_PROVIDER` | empty | Optional exact provider id, for example `zitadel`. |
| `NATS_AUTH_REQUIRED` | `true` | Require NATS credentials from `thingsflow-nats-auth`. |
| `CHECK_HISTORY_STORE` | `true` | Require GreptimeDB row visibility. |

The script does not print secret values. Temporary NATS checks read the NATS
username/password through Kubernetes Secret references inside an ephemeral
`nats-box` pod.

### Ephemeral Storage Smoke Mode

When the Kubernetes storage backend is unhealthy, for example Rook/Ceph reports
`OSD_FULL` or `POOL_FULL`, PVC provisioning and deletion can block unrelated
platform checks. Use the ephemeral smoke profile only to validate application
flow while the storage layer is being repaired:

```bash
helm --kubeconfig <your-kubeconfig> upgrade thingsflow k8s/helm/thingsflow \
  --namespace thingsflow --reuse-values \
  --set nats.persistence.enabled=false \
  --set postgres.persistence.enabled=false \
  --set greptimedb.persistence.enabled=false
```

This profile stores NATS JetStream/KV, Postgres, and GreptimeDB data on pod
local `emptyDir`. It is intentionally disposable: restart or reschedule loses
state. After switching NATS to ephemeral storage, delete and recreate
`thingsflow-nats-bootstrap`, then restart RMQTT, Bento materializers, and the demo
or benchmark publisher so streams, KV buckets, latest state, and history are
freshly materialized.

The normal pilot/production profile must restore durable storage before
acceptance: Postgres persistence, NATS JetStream/KV persistence when restart
survival is required, and GreptimeDB object-storage-backed history.

## Device SDK And JWT Lifecycle Smokes

Run these after every Flow Core image change, ingress change, RMQTT change, or
Device JWT policy change:

```bash
python tools/verify-device-sdk-live.py
kubectl --kubeconfig <your-kubeconfig> -n thingsflow port-forward svc/thingsflow-rmqtt-edge 18883:1883
uv run --with paho-mqtt python tools/verify-device-sdk-mqtt-live.py
python tools/verify-device-jwt-lifecycle.py
```

Expected result:

- SDK provisioning succeeds;
- native HTTP telemetry reaches NATS/latest state through `http-ingest`;
- native MQTT telemetry reaches NATS/latest state through RMQTT;
- active devices can receive Device JWTs;
- suspended devices cannot receive fresh Device JWTs.

If the lifecycle smoke reports HTTP 200 for suspended-device JWT renewal, the
running Flow Core image is not pilot-ready.

## Capacity Planning

The default public chart is sized for local smoke tests and controlled pilots,
not as a universal production sizing claim. It is useful because every runtime
component has explicit requests and limits that can be compared against device
count and message rate.

Default steady-state requests, excluding optional Dex test auth and demo
simulator:

| Component | CPU request | Memory request | Memory limit | Scaling signal |
|---|---:|---:|---:|---|
| Flow Core | 100m | 128Mi | 512Mi | API latency, WebSocket count, dashboard hydration latency. |
| RMQTT | 100m | 128Mi | 512Mi | Connected clients, publish admission, broker CPU. |
| NATS | 50m | 128Mi | 768Mi | Stream bytes, consumer lag, KV freshness. |
| HTTP ingest Envoy | 50m | 64Mi | 256Mi | HTTP 4xx/5xx, JWT verification latency. |
| HTTP ingest Bento | 50m | 96Mi | 256Mi | HTTP-to-NATS publish latency and errors. |
| Bento latest KV | 100m | 128Mi | 512Mi | NATS KV freshness and consumer lag. |
| Bento GreptimeDB | 100m | 128Mi | 512Mi | GreptimeDB write latency and consumer lag. |
| Bento alarms | 100m | 128Mi | 512Mi | Alarm intent lag and detector errors. |
| Alarm materializer | 50m | 96Mi | 256Mi | Alarm lifecycle write lag and Postgres errors. |
| Postgres | not set | 256Mi | 1Gi | Connections, write latency, vacuum, metadata growth. |
| GreptimeDB | 500m | 512Mi | 4Gi | Ingest latency, compaction, object-storage latency, disk cache growth. |
| QuestDB (optional) | not set | 1Gi | 4Gi | Ingest latency, O3 partition work, disk growth. |
| ThingsBoard UI adapter | not set | 64Mi | 256Mi | Static UI serving and browser/API proxy load. |

Approximate default total:

| Class | CPU request | Memory request | Memory limit |
|---|---:|---:|---:|
| ThingsFlow control + data plane | ~700m | ~2.3Gi | ~9.25Gi |

Storage defaults:

| Store | Default PVC | Production note |
|---|---:|---|
| Postgres | 10Gi | Operational metadata, dashboards, alarms, audit. |
| GreptimeDB | 20Gi | Historical telemetry cache/local store; production should prefer object storage. |
| QuestDB (optional) | 20Gi | Historical telemetry; size by retention and key count. |
| NATS | 20Gi when persistence is enabled | Default smoke profile uses memory storage; pilots should enable persistence. |

### Message-Rate Model

Use messages per minute as the first sizing unit:

```text
messages_per_minute = device_count * messages_per_device_per_minute
messages_per_second = messages_per_minute / 60
```

At one message per device per second:

| Devices | Messages/min | Messages/sec | Expected first pressure point |
|---:|---:|---:|---|
| 100 | 6,000 | 100 | Functional smoke and latest-state visibility. |
| 1,000 | 60,000 | 1,000 | RMQTT/NATS/Bento CPU, GreptimeDB writes, KV freshness. |
| 2,000 | 120,000 | 2,000 | Pilot acceptance level; watch materializer lag and storage growth. |
| 5,000 | 300,000 | 5,000 | Stress level; requires evidence before treating as production capacity. |

The benchmark matrix already includes this ladder. The 2026-05-19 pilot
evidence reached the `rmqtt-5000-stress` gate with 299,998 observed publishes,
no load-generator error lines, no runtime restarts, and low broker CPU/memory
after the run. That result is evidence for the RMQTT/NATS-first direction, not
a blanket production guarantee for every payload shape, retention policy, or
dashboard workload.

### ThingsFlow vs ThingsBoard Classic Benchmarking

The benchmark package compares:

| Target | Runtime shape |
|---|---|
| `thingsflow` | Flow Core + ThingsFlow data plane + RMQTT + HTTP ingest + NATS + Bento + GreptimeDB + Postgres. |
| `thingsboard-classic` | Official `thingsboard/tb-node` with PostgreSQL/TimescaleDB. |

Required scenarios:

| Scenario | Protocol | Devices | Interval | Duration |
|---|---|---:|---:|---:|
| `mqtt-100` | MQTT | 100 | `PUBLISH_INTERVAL_SECONDS=5` | `RUN_DURATION_SECONDS=900` (15 min) |
| `mqtt-1000` | MQTT | 1,000 | `PUBLISH_INTERVAL_SECONDS=10` | `RUN_DURATION_SECONDS=1800` (30 min) |
| `http-1000` | HTTP | 1,000 | `PUBLISH_INTERVAL_SECONDS=10` | `RUN_DURATION_SECONDS=1800` (30 min) |

Values match the committed scenario files in `benchmarks/scenarios/*.env`.

Recommended extensions:

| Scenario | Protocol | Devices | Interval | Duration |
|---|---|---:|---:|---:|
| `mqtt-burst-1000` | MQTT | 1,000 | 100 ms burst windows | 5 min |
| `cold-start` | MQTT | 100 | 1 s | 5 min |

Fair comparison rules:

- same Kubernetes cluster and node pool;
- same load generator image and scenario file;
- same device count, payload shape, interval, and duration;
- fresh namespaces and persistent volumes before cold runs;
- pinned image tags and chart versions;
- equivalent CPU/memory requests and limits where possible;
- same observation window for CPU, memory, readiness, errors, and latency.

Minimum metrics to publish:

- publish attempts and success rate;
- client-side latency when available;
- ingest-to-latest latency;
- pod CPU and memory;
- restarts;
- time to readiness;
- storage growth;
- NATS consumer lag and KV freshness for ThingsFlow;
- ThingsBoard rule-engine queue health for ThingsBoard Classic;
- dashboard/latest-value visibility after load.

The comparison should be interpreted by component boundary:

| Question | ThingsFlow expectation | ThingsBoard Classic expectation |
|---|---|---|
| Does hot latest state hit Postgres? | No. NATS KV is authoritative hot state. | Latest/telemetry persistence follows the ThingsBoard database/rule-engine path. |
| Is the UI in the ingest path? | No. It is a compatibility client. | No, but the backend runtime is the ThingsBoard core/rule-engine stack. |
| Can processors scale independently? | Yes, by NATS subject/consumer and Bento replicas. | Rule Engine and queues scale through ThingsBoard architecture. |
| Where should ML/analytics attach? | NATS consumers and GreptimeDB historical reads. | Rule Engine, integrations, external analytics, or DB reads. |

Benchmark methodology and result templates live in `benchmarks/README.md` and
`benchmarks/MATRIX.md`.

## Backup And Restore

Production deployments have two primary stateful stores, plus the NATS event
state that protects replay/latest-state durability:

- **Postgres**: users, tenants, devices, dashboards, topology, credentials,
  audit, alarm lifecycle, and operational metadata.
- **GreptimeDB**: default historical telemetry time series.
- **NATS JetStream/KV**: raw telemetry stream and `twin_state` latest/hot twin
  state; enable persistence for pilots that must survive restarts.

QuestDB remains an optional historical telemetry backend only when an operator
selects `timeseries.store=questdb`.

Postgres and GreptimeDB remain the required stores for metadata and historical
time series. In production, GreptimeDB should use object storage for historical
telemetry durability. The chart applies GreptimeDB native TTL through
`retention.greptimedb.ttl`; the default is `90d`. The chart's default
standalone GreptimeDB PVC is acceptable for local development and controlled
pilot benchmarking; a production overlay should provide the GreptimeDB storage
configuration, credentials, lifecycle policy, and restore drill for the chosen
object store.

Enable chart backups with an operator-owned Secret:

```yaml
backup:
  enabled: true
  schedule: "0 3 * * *"
  s3:
    existingSecret:
      name: thingsflow-backup-s3
      endpointKey: endpoint
      bucketKey: bucket
      regionKey: region
      accessKeyKey: access-key
      secretKeyKey: secret-key
  postgres:
    enabled: true
  greptimedb:
    enabled: true
  questdb:
    enabled: false
```

The QuestDB filesystem backup job is only relevant when
`timeseries.store=questdb`. It requires a storage class that supports read-only
multi-attach. With block storage, use CSI volume snapshots or an export job
through QuestDB pg-wire instead.

Postgres restore flow:

1. provision a fresh database;
2. download the latest custom-format dump;
3. recreate the target database;
4. restore with `pg_restore --clean --if-exists --no-owner --no-privileges`;
5. verify row counts for `tb_user`, `device`, and `dashboard`;
6. run `helm upgrade` with the restored connection settings.

The GreptimeDB backup job is a server-side export: the CronJob issues
`COPY DATABASE <db> TO 's3://<bucket>/<prefix>/<timestamp>/'` over the HTTP SQL
API and GreptimeDB streams parquet files directly to the bucket. No PVC mount,
no quiescing, no sidecar. Because the database is TTL-bounded by the retention
policy, the daily full export is bounded too. The job fails loudly when the
export reports zero rows — an empty "backup" of a live telemetry store is a
bug, not a success. Prune old prefixes with a bucket lifecycle rule (the
timestamped prefix layout makes age-based expiry safe).

GreptimeDB restore flow (from the parquet export):

1. stop writers that target GreptimeDB (scale the Bento materializers to 0);
2. on the fresh instance, recreate the tables (first ingest auto-creates the
   schema, or take `SHOW CREATE TABLE` output from the old instance);
3. import with `COPY DATABASE <db> FROM 's3://<bucket>/<prefix>/<timestamp>/'
   WITH (FORMAT='parquet') CONNECTION (...)` — same syntax as the export, with
   `FROM` instead of `TO`;
4. verify table counts and latest timestamps through SQL;
5. restart data-plane processors.

QuestDB restore flow, only when `timeseries.store=questdb`:

1. stop writers that target QuestDB;
2. stop the QuestDB statefulset;
3. restore the filesystem snapshot or exported tables into a fresh PVC/store;
4. start QuestDB;
5. verify table counts and latest timestamps;
6. restart data-plane processors.

## Controlled Pilot Runbook

Use `k8s/helm/thingsflow/values-pilot.example.yaml` as the public starting point
for a serious pilot. It is intentionally safe to commit: all private material
is referenced through existing Kubernetes Secrets and all hostnames are
placeholders.

Required operator-owned Secrets:

| Secret | Keys | Purpose |
|---|---|---|
| `thingsflow-platform-keys` | `jwt-token-signing-key` | Flow Core user/API JWT signing. |
| `thingsflow-device-jwt` | `device-jwt-es256-private-key-pem-b64`, `previous-public-jwks-b64` | Device JWT signing and rotation overlap. |
| `thingsflow-postgres` | `password` | Postgres password. |
| `thingsflow-nats-auth` | `username`, `password` | Internal NATS client auth. |
| `thingsflow-oidc` | `client-secret`, `state-signing-key` | External OIDC broker. |
| `thingsflow-backup-s3` | `endpoint`, `bucket`, `region`, `access-key`, `secret-key` | Postgres backup upload target. |

Use URL-safe NATS passwords for `thingsflow-nats-auth`. Bento and Flow Core consume
the credential through `nats://user:password@host`, while RMQTT receives the
same Secret as `auth.username` and `auth.password` in its rendered bridge
config. A hex secret is the simplest safe choice:

```bash
printf '%s' "$(openssl rand -hex 32)" > nats-password
```

Install or upgrade:

```bash
helm upgrade --install thingsflow ./k8s/helm/thingsflow \
  -n thingsflow --create-namespace \
  -f k8s/helm/thingsflow/values-pilot.example.yaml
```

Before exposing real devices, verify:

```bash
tools/verify-platform-health.sh
VALUES_FILE=k8s/helm/thingsflow/values-pilot.example.yaml \
  tools/verify-pilot-acceptance.sh
```

Then run the benchmark ladder with unique device prefixes:

```bash
WAIT_FOR_COMPLETION=true \
  benchmarks/scripts/run-benchmark.sh thingsflow benchmarks/scenarios/mqtt-100.env

WAIT_FOR_COMPLETION=true \
  benchmarks/scripts/run-benchmark.sh thingsflow benchmarks/scenarios/mqtt-1000.env

RUN_BENCHMARK=true EXPECTED_DEVICE_COUNT=2000 EXPECTED_PUBLISHED_MIN=120000 \
  VALUES_FILE=k8s/helm/thingsflow/values-pilot.example.yaml \
  tools/verify-pilot-acceptance.sh
```

Go/no-go criteria:

- no runtime pod restarts before or after the benchmark;
- publish success rate is at least `99.5%`;
- NATS KV latest state is fresh after the drain window;
- GreptimeDB contains historical samples for the benchmark window;
- ThingsBoard-compatible dashboards hydrate latest values;
- alarm intents create and clear dashboard-visible alarms;
- OIDC login succeeds and local demo auth remains disabled;
- backup CronJob exists and one manual backup/restore drill has been tested;
- logs contain no bearer tokens, MQTT passwords, OIDC secrets, or raw payload
  dumps.

## Upgrading: never `--reuse-values`

`--reuse-values` reuses the **computed** values of the previous release, not the
user-supplied ones. Every default the chart has changed since that release is
silently shadowed by the old computed value, so an upgrade that exists precisely
to ship a new default ships the old one and reports success.

This is not hypothetical. `natsDataPlane.latestKv.maxAckPending` was changed
from `1024` to `1` because it became the doc-merge consumer's serialization
mechanism — with `--reuse-values`, a production dry-run still rendered `1024`,
which NATS would have rejected at bind time (`configuration requests max ack
pending to be 1024, but consumer's value is 1`), leaving the consumer unable to
attach at all.

Upgrade by passing your overlay explicitly instead (start from one of the
tracked `values-*.example.yaml` templates):

```bash
helm upgrade thingsflow ./k8s/helm/thingsflow -n thingsflow -f my-overlay.yaml
```

If the release carries overrides that live only in the cluster, capture them
first rather than reusing them blindly — this keeps the operator's intent and
still picks up new chart defaults:

```bash
helm -n thingsflow get values thingsflow | tail -n +2 > /tmp/live-values.yaml
helm upgrade thingsflow ./k8s/helm/thingsflow -n thingsflow -f /tmp/live-values.yaml
```

Always confirm what actually rendered before trusting the upgrade:

```bash
helm -n thingsflow get values thingsflow --all | grep -A3 latestKv
kubectl -n thingsflow get deploy thingsflow-nats-latest-kv \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="LATEST_KV_MAX_ACK_PENDING")].value}{"\n"}'
```

## Routine Checks

Daily pilot checks:

- no unexpected pod restarts;
- NATS consumers are not falling behind;
- latest KV updates are fresh for active devices;
- GreptimeDB disk/object-store growth matches retention expectations;
- Postgres backups completed and restore has been tested recently;
- dashboards hydrate without blank widgets;
- logs remain free of tokens and raw payload dumps;
- no failed guard Jobs and no suspended guard CronJobs — see the
  [Guard playbook](#guard-playbook) for what each guard means and how to
  diagnose it.

## Guard playbook

Every safety property of the platform — bounded retention, live ingest, stream
configuration — is verified by a Kubernetes Job, not by a Prometheus scrape
stack. None exists, and that is intentional (see
[Retention observability](#retention-observability)). All guards follow one
convention: a healthy tick exits 0; an unhealthy tick exits non-zero and leaves
a **persistent failed Job object — that failed Job IS the alert**. Where a
guard has a numeric signal, it is a Prometheus-style exposition line on the
Job's stdout, read from the Job logs — never scraped.

Start every investigation the same way:

```bash
# The alert surface: any guard Job with COMPLETIONS 0/1 has fired.
kubectl -n thingsflow get jobs --sort-by=.metadata.creationTimestamp

# What the latest run said (every guard labels its pods app=<guard-name>):
kubectl -n thingsflow logs -l app=greptimedb-ttl-guard --tail=30
```

To re-run a CronJob-based guard immediately instead of waiting for the next
tick:

```bash
kubectl -n thingsflow create job \
  --from=cronjob/thingsflow-greptimedb-ttl-guard \
  ttl-guard-manual-$(date +%s)
```

All commands in this playbook assume the canonical install (`helm upgrade
--install thingsflow ... -n thingsflow`); with a different release name or a
`fullnameOverride`, prefix object names accordingly.

| Guard | Watches | Default schedule | Cadence value | Signal |
|---|---|---|---|---|
| `greptimedb-ttl-guard` | GreptimeDB DB/table TTL present and healthy | `0 * * * *` (hourly) | `retention.greptimedb.schedule` | `thingsflow_greptimedb_ttl_ok`, `thingsflow_greptimedb_ttl_drift_repaired` |
| `greptimedb-freshness-guard` | telemetry still landing in GreptimeDB | `*/5 * * * *` | `monitoring.freshnessGuard.schedule` | `thingsflow_greptimedb_write_stale` |
| `nats-consumer-guard` | every JetStream durable is bound and draining | `*/5 * * * *` | `monitoring.consumerGuard.schedule` | `thingsflow_nats_consumer_progress_ok` |
| `device-silence-guard` | individual devices that stopped reporting | `*/5 * * * *` (guard off by default) | `alarms.deviceSilence.schedule` | none — emits `DeviceSilent` alarm intents; a failed Job means the guard itself broke |
| NATS stream drift-verify | live JetStream stream/consumer config vs chart intent, plus the PVC budget | every `helm install`/`upgrade` (hook, not a CronJob) | n/a — runs with each upgrade | `thingsflow_nats_stream_ok` |
| `postgres-alarm-retention` | sweep of cleared+acked alarms past the window | `0 3 * * 0` (Sun 03:00) | `retention.alarm.schedule` | `thingsflow_postgres_alarm_deleted_total` |

### A guard suspended by hand stays off

Every guard CronJob declares `suspend: false` explicitly. Without the field in
the manifest, a guard stopped by hand with kubectl is never re-enabled by any
`helm upgrade` — Helm only reasserts the fields it manages. It really
happened: the freshness guard sat suspended for 47 hours, invisible to the
deployment, while the chart said `enabled: true`.

The explicit field alone does not recover a guard already suspended by hand:
`kubectl patch` makes kubectl the owner of `.spec.suspend`, and Helm's
server-side apply refuses to change it
(`Apply failed with 1 conflict: conflict with "kubectl-patch": .spec.suspend`).
Reclaim ownership once with `helm upgrade --force-conflicts` (not `--force`,
which is incompatible with server-side apply); after that any normal upgrade
keeps the guard active. Routine check:

```bash
kubectl -n thingsflow get cronjobs \
  -o custom-columns=NAME:.metadata.name,SUSPEND:.spec.suspend
# every guard must show SUSPEND=false
```

### greptimedb-ttl-guard

**What it watches.** Re-runs the shared `guard.sh` (the
`greptimedb-retention-scripts` ConfigMap — the single source of the
apply/verify/signal logic, shared with the day-0 install hook) to verify and
self-heal the GreptimeDB database-level and telemetry-table TTL. The window
itself is `retention.greptimedb.ttl` (default `90d`) — the control that keeps
telemetry history bounded.

**Schedule.** Hourly (`0 * * * *`); change with
`retention.greptimedb.schedule`. Guard and hook are enabled together by
`retention.greptimedb.enabled` (default `true`).

**Signal lines** (Job stdout):

```text
thingsflow_greptimedb_ttl_ok 1|0             1 = TTL healthy after any self-heal; 0 = un-healable
thingsflow_greptimedb_ttl_drift_repaired 1|0 1 = drift was found AND repaired this run
```

**What a failed Job means.** The CronJob runs with
`GUARD_ALERT_ON_DRIFT=true`, so it fails in two distinct situations:

- `ttl_ok 0` — un-healable: an `ALTER` failed, a trap TTL value (`instant`,
  `0s`, `0`, `forever`, or an absent TTL) is still present after the heal, or
  GreptimeDB was unreachable after ~2 minutes of retries.
- `ttl_ok 1` with `drift_repaired 1` — drift was found and already repaired;
  the Job still fails so the drift event stays visible instead of healing
  silently.

**Diagnosis.**

```bash
kubectl -n thingsflow get jobs -l app=greptimedb-ttl-guard \
  --sort-by=.metadata.creationTimestamp
kubectl -n thingsflow logs -l app=greptimedb-ttl-guard --tail=30
```

Look for the two signal lines, plus `drift: ...` (what drifted, detected
before the heal) and `... still missing/trap after heal` (what could not be
healed).

**Remediation.** `drift_repaired 1` with `ok 1` needs no repair — the TTL is
already back; find what removed it (a manual `ALTER`, a restore, a new table).
For `ok 0`: verify GreptimeDB is up and reachable on its pg-wire port, then
re-apply the intent with a no-op `helm upgrade`. The window is governed only by
`retention.greptimedb.ttl` — see
[Retention observability](#retention-observability).

### greptimedb-freshness-guard

**What it watches.** Whether telemetry is still landing in
`device_telemetry_kv` — the direct control for a silent ingest halt, where
every pod stays Running and HTTP ingest keeps returning 200 while nothing
reaches the store (the original incident went undetected ~14h). An init
container (`backlog-probe`) first reads the history consumer's backlog
(`num_pending + num_ack_pending` on the TF_RAW durable) so the guard can tell
a halted writer from an idle platform.

**Schedule.** Every 5 minutes (`*/5 * * * *`);
`monitoring.freshnessGuard.schedule`. Staleness threshold:
`monitoring.freshnessGuard.staleMinutes` (default 15). Enabled by
`monitoring.freshnessGuard.enabled` (default `true`).

**Signal line.** `thingsflow_greptimedb_write_stale 0|1` — note this is a
*staleness* flag, not an `_ok` pattern: `0` is healthy, `1` alerts.

**What a failed Job means** (exit non-zero, `write_stale 1`):

- GreptimeDB is unreachable; or
- rows exist but none within `staleMinutes` **and** messages are waiting on
  the history consumer — data-plane ingest is halted; or
- same, but the backlog could not be read — a halt cannot be ruled out, so the
  guard deliberately alerts.

Two zero-row situations deliberately do **not** alert: an empty table (fresh
install — nothing has ever arrived) and an idle platform (no recent rows but
backlog 0 — nothing was published, nothing is stuck).

**Diagnosis.**

```bash
kubectl -n thingsflow get jobs -l app=greptimedb-freshness-guard \
  --sort-by=.metadata.creationTimestamp
kubectl -n thingsflow logs -l app=greptimedb-freshness-guard --tail=20
```

The `[freshness-guard] ALERT: ...` line states which case fired, including the
total row count and the waiting-message count; `[backlog-probe] ...` shows the
consumer backlog reading.

**Remediation.** A genuine halt (rows waiting on the consumer) means the
NATS→GreptimeDB writer stopped draining — restart it and watch the backlog
drain:

```bash
kubectl -n thingsflow rollout restart deploy/thingsflow-nats-greptimedb
```

(`thingsflow-nats-entity-greptimedb` is the TF_ENTITY counterpart — restart
it too if entity rows are also stale.) If NATS itself lost its streams, follow
[Recovering a NATS that lost its streams](#recovering-a-nats-that-lost-its-streams).
If GreptimeDB is unreachable, fix that first — the guard cannot distinguish
further until it can query the store.

### nats-consumer-guard

**The failure it exists for.** A rolling node maintenance restarted NATS and
left three Bento consumers (`latest-kv`, `alarms`, `entity-greptimedb`) holding
dead subscriptions. All three reported `Running 1/1` for hours. Kubernetes could
not see it: every data-plane Deployment sets `livenessProbe: /ping` and
`readinessProbe: /ready`, and both stay green while the pod spins on
`nats: connection closed` — `/ready` is satisfied by a `nats.Conn` handle that is
dead at the *subscription* level, and `/ping` only pings Bento's HTTP server.

This is also the failure `greptimedb-freshness-guard` structurally cannot catch:
that guard asks whether the *store* is receiving anything, and it stays green
while `latest-kv` is dead because a different consumer keeps writing rows.

**What it checks, and why not backlog.** Queue depth is the wrong signal.
`latest-kv` runs `max_ack_pending: 1` as a deliberate single-writer
serialization mechanism, so under load it holds a large and often *growing*
backlog while working perfectly — measured at a sustained backlog of 6,729 while
its ack floor advanced 61,179 → 100,526. A depth threshold alerts there. The
guard uses two signals instead:

1. `push_bound` — the server-side boolean saying whether anything is subscribed
   to the durable's deliver subject right now. This is what the nats CLI renders
   as `Active Interest: No interest`. Necessary but not sufficient: with more
   than one replica in a deliver group, one dead pod still leaves it `true`.
2. `ack_floor.consumer_seq` movement across two samples — what separates
   "serialized but progressing" from "dead". (`consumer_seq`, not `stream_seq`:
   TF_RAW is discard-old, so retention deleting messages under a stalled
   consumer drags the stream-side floor forward and reads as false progress.)

An alert requires the fault in **both** samples, so a `helm upgrade` rolling the
Bento pods does not trip it.

**Reading a failure.**

```bash
kubectl -n thingsflow logs -l app=nats-consumer-guard --tail=40
```

| Line | Meaning | Remedy |
|---|---|---|
| `no subscriber bound to deliver subject` | the classic silent stall — pod alive, subscription dead | `kubectl -n thingsflow rollout restart deploy/thingsflow-<consumer>` |
| `ack floor is frozen ... bound but not draining` | subscribed but not acking: a wedged pipeline, a poison message against `maxDeliver`, or one dead pod in a multi-replica deliver group | check that consumer's pod logs before restarting |
| `consumer could not be read (absent, or NATS refused)` | a durable the chart expects does not exist | re-run `helm upgrade` so the nats-bootstrap hook recreates it |
| `NATS unreachable` | not evidence of health, deliberately alerts | fix NATS first |

**Scope.** The guard covers every JetStream durable, including the
`alarm-materializer` one (created by the Go client, and the only consumer with
no Kubernetes probes at all). It cannot see the WebSocket journal fan-out
(`flow-core/internal/ws/journal.go`) — that is a *core* NATS subscription with
its own resubscribe loop and creates no JetStream consumer.

An orphaned durable (one whose component was disabled but whose consumer was
left behind) will alert forever, correctly: on a capped, discard-old stream an
unattended consumer is not inert. Remove it at the source rather than adding it
to `monitoring.consumerGuard.ignore`.

### device-silence-guard

**Off by default** (`alarms.deviceSilence.enabled: false`): on a fleet still
being onboarded, every device not wired up yet would alarm. Turn it on once
the fleet is stable.

**What it watches.** The one failure the message-triggered pipeline
structurally cannot catch: a single device that stops sending, while the store
keeps receiving from everyone else — which keeps the freshness guard green.
Each tick queries GreptimeDB (HTTP SQL API) for the newest sample of every
device seen within `alarms.deviceSilence.lookbackHours` (default 24 — devices
outside the window count as out of the fleet, not silent, so a decommissioned
meter does not alarm forever) and publishes alarm intents on the
`natsDataPlane.alarmIntentPrefix` subject (default `tf.alarm.intent`):
`create_or_update` for devices silent at least
`alarms.deviceSilence.silenceMinutes` (default 15), `clear` for the rest —
`alarmType: DeviceSilent`, severity `alarms.deviceSilence.severity` (default
`MAJOR`). The existing alarm materializer handles lifecycle, dedup, and
clearing; the guard itself is stateless, so a missed run cannot leave an alarm
stuck active after the meter came back.

**Signal.** This guard emits **no `thingsflow_*` metric line.** Its product is
the `DeviceSilent` alarm itself, visible through the alarm UI/API; the healthy
Job log trace is `checked N device(s); silence threshold 15m` (or
`no devices reported in the last 24h — nothing to check`). **A failed Job does
not mean a device is silent — it means the guard itself broke**: the
GreptimeDB query failed or returned an error payload.

**Schedule.** Every 5 minutes (`*/5 * * * *`);
`alarms.deviceSilence.schedule`. Unlike the other guards it runs with
`backoffLimit: 1`, so one automatic retry happens before the Job surfaces as
failed.

**Diagnosis.**

```bash
kubectl -n thingsflow get jobs -l app=device-silence-guard \
  --sort-by=.metadata.creationTimestamp
kubectl -n thingsflow logs -l app=device-silence-guard --tail=20
```

`greptimedb query failed` or `greptimedb error: ...` → the HTTP SQL endpoint
(port 4000) failed or rejected the query. `failed to publish intent for
<device>` → a NATS publish problem (the run continues past it): check the
`thingsflow-nats-auth` Secret and NATS availability, and
[Recovering a NATS that lost its streams](#recovering-a-nats-that-lost-its-streams)
if streams are gone.

**Remediation.** Fix GreptimeDB/NATS availability as above and re-run the
guard (`kubectl create job --from=cronjob/thingsflow-device-silence-guard ...`).
A silent device itself is a field problem, not a guard problem — the alarm
clears automatically on the first pass after the meter reports again.

### NATS stream drift-verify

**Not a CronJob.** The drift guard (`verify.sh`, in the
`nats-stream-guard-scripts` ConfigMap) runs as the trailing step of the
`nats-bootstrap` Job, a `post-install,post-upgrade` Helm hook — it runs on
every `helm install`/`helm upgrade`, and only then. A non-zero `verify.sh`
exit fails the whole hook Job, which is the alert surface.

**What it checks.**

- TF_RAW, TF_ENTITY, and TF_TWIN_EVENTS live config against chart intent, per
  field: storage class, `max_age`, `max_bytes`, `discard=old`, subjects.
- The durable GreptimeDB-writer consumers on both streams: `deliver_policy`,
  `max_deliver`, `ack_wait`, `max_ack_pending`, and push mode
  (`deliver_group` + `deliver_subject` — a mis-created *pull* consumer would
  silently break the Bento `bind: true` attach and stall all writes).
- PVC fit: the sum of live `max_bytes` of every file-backed stream (TF_RAW,
  TF_ENTITY, TF_TWIN_EVENTS, TF_LATEST, TF_ALARMS) plus the `twin_state` KV
  stored bytes must
  stay at or under **70% of the NATS data PVC**. An uncapped file-backed
  stream (`max_bytes = -1`/absent) is treated as unbounded and fails the
  check — it is *not* counted as zero.

**Signal line.** `thingsflow_nats_stream_ok 1|0` — `0` on drift, PVC
overcommit, or NATS unreachable after ~2 minutes of retries.

**What a failed hook Job means.** The verify runs *after* the hook has
re-applied the stream configuration, so a reported drift is one that survived
the re-apply — typically a creation-time-only consumer field
(`deliver`/`filter`/`ack`), which an in-place edit silently no-ops — or a PVC
overcommit, or NATS being unreachable.

**Diagnosis.**

```bash
kubectl -n thingsflow get jobs -l app=nats-bootstrap
kubectl -n thingsflow logs -l app=nats-bootstrap --tail=60
```

Look for `drift: ...` (stream fields), `consumer drift: ...` /
`consumer MISSING: ...`, `PVC-fit: OVERCOMMIT ...`, and the final signal line.

**Remediation.** Re-running the guard is always a no-op `helm upgrade` — the
same command that recovers lost streams; see
[Recovering a NATS that lost its streams](#recovering-a-nats-that-lost-its-streams).
For consumer drift on creation-time-only fields, remove the drifted durable
and `helm upgrade` again so the hook recreates it with the intended policy.
The chart deploys no standing nats-box pod — start a throwaway one (NATS
credentials live in the `thingsflow-nats-auth` Secret, see Operator Secrets):

```bash
kubectl -n thingsflow run nats-box --rm -it --image=natsio/nats-box:0.16.0 -- sh
# inside: nats -s nats://<user>:<pass>@thingsflow-nats:4222 consumer rm <stream> <durable>
```

For PVC overcommit, shrink the stream caps or grow the PVC before anything
fills — the 70% budget exists precisely so the file-backed streams can never
overflow the shared volume.

### TF_TWIN_EVENTS (twin event journal)

**What it is.** The durable JetStream stream backing the twin event journal
(`tf.twin.events.>`) — every control-plane twin/attribute/relation/alarm write
emits a best-effort event onto it (R4), and each flow-core replica's WS
consumer (`flow-core/internal/ws/journal.go`) fans events out to its own
subscribers, giving the WS plane multi-réplica propagation.

**Retention (declarative).** Declared in `values.yaml` (`nats.twinEvents`):
file storage, `max_age: 24h`, `max_bytes: 1Gi`, `replicas: 1`. The journal is
bounded like every other store — it cannot grow unbounded (the 400GB QuestDB
disk-fill rule applies to it too).

**How it is guarded.** TF_TWIN_EVENTS is field-drift-checked by the same
`verify.sh` that guards TF_RAW/TF_ENTITY (see [NATS stream
drift-verify](#nats-stream-drift-verify)): storage class, `max_age`,
`max_bytes`, `discard=old`, and subjects are compared against chart intent on
every `helm install`/`upgrade`, and the stream's live `max_bytes` is summed
into the PVC-fit budget. A drift, PVC overcommit, or unreachable NATS emits
`thingsflow_nats_stream_ok 0` and fails the `nats-bootstrap` hook Job.

**Diagnosis.**

```bash
kubectl -n thingsflow logs -l app=nats-bootstrap --tail=60
```

Look for `drift: TF_TWIN_EVENTS ...` lines (which field drifted) and the final
signal line.

**Remediation.** The bootstrap hook re-converges the stream on every `helm
upgrade`, so re-running the guard is a no-op `helm upgrade` (the same command
that recovers a lost stream). The guard does **not** mutate — a failed Job
**is** the alert; `verify.sh` runs *after* the hook re-applied the stream, so a
reported drift is one that survived re-apply. To grow the stream, raise
`nats.twinEvents.maxBytes` in `values.yaml` **only after re-measuring** the twin
event rate against the 70% PVC budget — the journal is bounded by design, and
the budget exists so file-backed streams can never overflow the shared volume.

### Desired-state delivery, reported convergence & HTTP poll (R5)

**What they are.** Desired state (model-validated `desiredProperties`, persisted
as `feature.<name>.desired.<property>` SERVER_SCOPE keys) is delivered to
devices over MQTT retained on `thingsflow/devices/<mqttId>/desired`
(`internal/desiredstate` `DeliverDesired`, rmqtt HTTP API `retain: true`,
clientid `flow-core-desired`), so a device receives the current desired state on
connect/reconnect. Device-reported attributes converge back into the twin as
CLIENT_SCOPE via a **read-only** NATS consumer on the raw reported subject; the
twin GET returns a per-feature desired-vs-reported `delta`. Non-MQTT devices
poll desired state via `GET /api/v1/{token}/attributes?desiredKeys=`.

**Off the hot path.** Desired/reported handling is control plane: it never
modifies the rmqtt NATS egress bridge, the Bento materializers, or the ingest
pipeline. Delivery is fire-and-forget — a broker publish failure is logged
(`desired_delivery_failed`), never a write failure. The reported consumer is
read-only on the shared raw subject and only processes the attributes topic
(telemetry messages are ignored).

**Diagnosis.**

```bash
# Delivery was attempted but the broker rejected it:
kubectl -n thingsflow logs -l app=flow-core --tail=100 | grep desired_delivery_failed
# Reported messages are not converging:
kubectl -n thingsflow logs -l app=flow-core --tail=100 | grep -E 'reported (merge|consumer)'
```

Look for `desired_delivery_failed` (broker publish returned non-200 or the
device's mqttId could not be resolved — a non-MQTT device is expected to poll
instead), `reported consumer retry` (NATS unreachable), or `reported merge
failed` (postgres write error).

**Remediation.** A `desired_delivery_failed` for an MQTT device usually means
the broker API URL (`MQTT_BROKER_API_URL`) or the device's mqttId resolution is
wrong — verify the device JWT `clientid` matches the mqttId the ACL expects.
Non-MQTT devices should use the HTTP poll (`desiredKeys`), not MQTT delivery.
The desired topic ACL lives in `docker/rmqtt/rmqtt-acl.toml` + the helm
`rmqtt-edge.yaml` (`thingsflow/devices/%c/desired` subscribe) — keep the two in
sync.

### postgres-alarm-retention

**What it does.** The declarative replacement for the old in-process Go alarm
sweep: a psql transaction that deletes alarms that are both **cleared and
acked** (`clear_ts > 0 AND ack_ts > 0`) and older than `retention.alarm.days`
(default 180). Active or unacked alarms are never touched — that predicate is
the safety fence; never widen it. The delete is one atomic transaction: on any
error nothing is partially deleted and the Job fails.

**Schedule.** Weekly, Sunday 03:00 (`0 3 * * 0`); `retention.alarm.schedule`.
Window: `retention.alarm.days`. Enabled by `retention.alarm.enabled`
(default `true`).

**Signal line.** `thingsflow_postgres_alarm_deleted_total <n>` — rows deleted
this run.

**What a failed Job means.** The sweep did not complete: Postgres
unreachable/auth failure, a SQL error (the transaction rolled back — no
partial delete), or an invalid `ALARM_RETENTION_DAYS`. No data is at risk, but
Postgres alarm growth stays unbounded while the sweep keeps failing.

**Diagnosis.**

```bash
kubectl -n thingsflow get jobs -l app=postgres-alarm-retention \
  --sort-by=.metadata.creationTimestamp
kubectl -n thingsflow logs -l app=postgres-alarm-retention --tail=20
```

The log states the window (`sweeping cleared+acked alarms older than 180
days`), the deleted count, and psql's error text on failure.

**Remediation.** Check Postgres availability and the `thingsflow-postgres`
Secret, then re-run without waiting a week:

```bash
kubectl -n thingsflow create job \
  --from=cronjob/thingsflow-postgres-alarm-retention \
  alarm-retention-manual-$(date +%s)
```

The window and cadence are governed only by `retention.alarm.*` — see
[Retention observability](#retention-observability).

## Retention observability

Retention is a core safety property of the platform: **no telemetry store may grow
unbounded.** Every live retention window is configured in ONE place and is
*verified* to actually be applied — this section is the operator contract for
that verification.

### Single source of truth

Every telemetry/alarm retention window lives under the top-level `retention.*` block
in `k8s/helm/thingsflow/values.yaml`:

| Window | Values key | Applied by |
|--------|-----------|------------|
| GreptimeDB native TTL | `retention.greptimedb.ttl` (default `90d`) | day-0 Helm hook + hourly TTL-guard CronJob |
| GreptimeDB guard cadence | `retention.greptimedb.schedule` (default `0 * * * *`) | `greptimedb-ttl-guard` CronJob |
| Alarm deletion window | `retention.alarm.days` (default `180`) | `postgres-alarm-retention` CronJob |
| Alarm sweep cadence | `retention.alarm.schedule` (default `0 3 * * 0`, Sun 03:00) | `postgres-alarm-retention` CronJob |

Change a window or cadence here and only here — there is no second copy.

**One exception:** `audit_log` partition retention is still driven from the
application tier. Flow Core's partition manager reads `AUDIT_LOG_RETENTION_DAYS`
and defaults to 90 days; the chart does not set that variable, so the window is the
Go default unless you inject the env yourself. Fold it into `retention.*` before
claiming the data plane owns every window.

Windows that are not Helm `retention.*` keys, for completeness:

| Surface | Mechanism | Control | Default |
|---|---|---|---|
| Postgres `audit_log` | monthly RANGE partitions; DROP older than N days | `AUDIT_LOG_RETENTION_DAYS` (Flow Core env) | 90 |
| NATS JetStream streams | `max_age` / `max_bytes` with `discard: old` | `nats.rawStream.*`, `nats.entityStream.*`, `nats.latestStream.*`, `nats.alarmIntentStream.*` | 24h / 1Gi on the cluster overlay |
| NATS KV `twin_state` | bucket TTL + history depth | `nats.twinKv.ttl`, `nats.twinKv.history` | 1h base, 24h cluster |
| QuestDB `device_telemetry` (optional) | `ALTER TABLE … SET TTL N DAY` | `DEVICE_TELEMETRY_TTL_DAYS` | 90 |

**Why this is non-negotiable.** Any telemetry-rate write path fills disks quickly
when retention is implicit. A store without a bounded, *verified* window is the
failure this milestone exists to prevent.

### Verification signals: CronJob logs + failed-Job exit

Retention is verified by **CronJob-emitted log metrics and failed-Job exit
status** — NOT by a Prometheus scrape stack. None exists, and that is
intentional: retention was moved out of the application tier, so the signals are
emitted by the Kubernetes Jobs themselves.

- **GreptimeDB TTL guard** (`greptimedb-ttl-guard` CronJob, running the shared
  `guard.sh` from the `greptimedb-retention-scripts` ConfigMap — see its inline
  metric contract, do not duplicate it here):
  - `thingsflow_greptimedb_ttl_ok {1|0}` — `1` = DB/table TTL healthy (after any
    self-heal); `0` = drift that could not be healed.
  - `thingsflow_greptimedb_ttl_drift_repaired {1|0}` — `1` = drift was found and
    repaired on this tick.
  - On **un-healable** drift the guard exits non-zero, leaving a **persistent
    failed Job object** — that failed Job IS the deployable alert signal.
- **Postgres alarm retention** (`postgres-alarm-retention` CronJob):
  - `thingsflow_postgres_alarm_deleted_total <n>` — rows deleted this run.
  - On error the Job exits non-zero, leaving a failed Job object as the alert.

Operationally: alert on any failed `greptimedb-ttl-guard` / `postgres-alarm-retention`
Job, and scrape the CronJob stdout (`kubectl logs job/<name>`) for the metric
lines above when you need the numeric values.

### No flow-core retention metric (by design)

flow-core exposes **no** `flow_retention_*` Prometheus counter. The Go
`flow_retention_*` counters were removed when retention left the
application tier; `grep flow_retention_ flow-core/` returns zero. Do not
re-introduce an app-tier retention metric — the CronJob signals above are the
contract.

## Retention values migration

The deprecated `greptimedb.retention.*` namespace was reconciled into
`retention.greptimedb.*`. A Helm render now **fails fast** if
the old key is still present (`_validations.tpl`, inside
`thingsflow.validateSecrets`), naming the exact key to move — by design,
so an operator who set `greptimedb.retention.ttl` cannot silently lose the TTL
override.

**Operator runbook — a release that set `greptimedb.retention.ttl` (or
`.schedule`) must move it before the next `helm upgrade`:**

```yaml
# BEFORE (deprecated — next helm upgrade ABORTS):
greptimedb:
  retention:
    ttl: "90d"
    schedule: "0 * * * *"

# AFTER (move the overrides up to the top-level block):
retention:
  greptimedb:
    ttl: "90d"
    schedule: "0 * * * *"
```

Until the override is moved, `helm upgrade` aborts with:
`greptimedb.retention.* has moved to retention.greptimedb.*. Move
greptimedb.retention.ttl -> retention.greptimedb.ttl ...`. This hard-stop is
intentional — it forces an explicit migration rather than a silent TTL loss.


## Recovering a NATS that lost its streams

**Symptom.** Flow Core logs `twin state store nats init attempt N failed: nats:
bucket not found` and never becomes ready; `/ready` returns 503; `http-ingest`,
`rmqtt-edge` and the UI adapter sit in `Init`; the alarm materializer restarts.

**Cause.** JetStream lost its state. Chart defaults are file-backed on a PVC,
so this should not follow an ordinary restart — expect it only where the PVC was
lost or `nats.persistence.enabled=false` was set for a throwaway install. The
streams and the `twin_state` bucket are created by a Helm `post-install,
post-upgrade` hook, so nothing recreates them while the release sits unchanged.

**Confirm it** before acting — the same symptoms follow from NATS simply being
unreachable:

```bash
kubectl -n thingsflow port-forward svc/thingsflow-nats 8222:8222 &
curl -s 'localhost:8222/jsz?streams=1' | python3 -c \
  'import json,sys; d=json.load(sys.stdin); print(sum(len(a.get("stream_detail",[])) for a in d.get("account_details",[])), "streams")'
```

Zero streams on a release that has been running is the confirmation.

**Recover** with a no-op upgrade, which re-runs the bootstrap hook:

```bash
helm upgrade thingsflow ./k8s/helm/thingsflow -n thingsflow
```

Consumers rebind on their own once the streams exist; components that exited
are restarted by Kubernetes. Expect convergence within about a minute.

**Stop it recurring.** Use file-backed streams and a PVC, as both committed
overlays do:

```yaml
nats:
  persistence: { enabled: true, size: 20Gi }
  rawStream:   { storage: file }
  entityStream: { storage: file }
  alarmIntentStream: { storage: file }
  twinKv:      { storage: file }
```

Telemetry already written to GreptimeDB is not affected by any of this: the
history store is separate. What is lost is the in-flight buffer and the latest
values, which repopulate as devices publish again.
