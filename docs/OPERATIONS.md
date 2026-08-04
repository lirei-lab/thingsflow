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

## Routine Checks

Daily pilot checks:

- no unexpected pod restarts;
- NATS consumers are not falling behind;
- latest KV updates are fresh for active devices;
- GreptimeDB disk/object-store growth matches retention expectations;
- Postgres backups completed and restore has been tested recently;
- dashboards hydrate without blank widgets;
- logs remain free of tokens and raw payload dumps.

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
