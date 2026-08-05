# ThingsFlow Install

The Helm chart, release, and default namespace are all `thingsflow`.

ThingsFlow runs Flow Core, the data plane, Postgres, GreptimeDB by default, RMQTT,
`http-ingest`, NATS in the NATS event plane, Bento materializers,
`alarm-materializer`, and the optional ThingsBoard UI adapter. QuestDB remains
available as an explicit optional time-series backend.
Flow Core replaces the classic Java
backend runtime; the UI adapter is a compatibility console, not the platform
core.

This doc covers installing, iterating on, and troubleshooting a deployment.
Running one — public access, logging, audit, observability, retention, backup and
the pilot gates — is [Operations](OPERATIONS.md). Source of truth is the code;
this is a map.

---

## TL;DR - Fresh Install

Public chart with public images and generic defaults:

```bash
helm install thingsflow ./k8s/helm/thingsflow \
  --namespace thingsflow --create-namespace
```

Alternatively, install straight from the OCI registry (no repo checkout
needed):

```bash
helm install thingsflow oci://ghcr.io/lirei-lab/charts/thingsflow \
  --version 2.2.0 -n thingsflow --create-namespace
```

!!! warning "Do not add `--wait` to the first install"
    JetStream streams and the `twin_state` KV bucket are created by a
    `post-install` hook, and Helm only runs post-install hooks **after** the
    release's resources report Ready. Flow Core is not Ready until that KV
    bucket exists, so `--wait` deadlocks the two against each other and the
    install times out with pods stuck in `Init`/`CrashLoopBackOff`.

    Without `--wait` the install converges on its own: Helm returns
    immediately, the hook creates the streams and bucket, and Flow Core's
    retry loop picks them up within a minute. Watch it with
    `kubectl -n thingsflow get pods -w`. `--wait` is fine on subsequent
    upgrades, once the bucket exists.

!!! info "JetStream is durable by default"
    The chart defaults to a PVC with file-backed streams and a file-backed
    `twin_state` bucket, so a NATS restart — node reboot, eviction, drain —
    keeps them. Verified by deleting the NATS pod on a default install: all four
    streams survived and the platform returned ready on its own.

    This used to be memory-backed with no PVC, which meant any NATS restart
    destroyed the streams and the bucket, and **nothing recreated them**: they
    come from a Helm hook, so a release that is merely running never rebuilds
    them. Flow Core would retry forever against a bucket that never returned.
    If you deliberately set `nats.persistence.enabled=false` for a throwaway
    install, that failure mode comes back, and recovery is a no-op
    `helm upgrade` — see [Operations](OPERATIONS.md).

That's the whole install. The public chart defaults use versioned runtime
images; `flow-core` follows the chart **version** tag (releases are cut as the
git tag `v<chart version>`, which is what the image build turns into a semver
tag) and infrastructure images are pinned by version or digest. On first boot Flow Core seeds the schema,
widgets, dashboards,
SCADA symbols, the 891 system images, and provisions per-tenant
`api_usage_state`. No post-install scripts.

**Prereqs:**

- A Kubernetes cluster with a default `StorageClass` (PVCs request
  10–20 Gi each for Postgres/GreptimeDB and any enabled broker persistence).
- If the ghcr.io package is private: a docker-registry secret with a
  PAT that has `read:packages`, referenced in
  `images.pullSecrets` in [values.yaml](https://github.com/lirei-lab/thingsflow/blob/main/k8s/helm/thingsflow/values.yaml).

!!! warning "A default install has NO known login"
    The `sysadmin@thingsboard.org` and `tenant@thingsboard.org` rows are
    seeded, but a default Helm install gives them a **per-install random
    password**: the well-known upstream hash is deliberately never shipped.
    Installing and then trying `tenant/tenant` fails with a 401.

    For an evaluation install, ask for the demo passwords explicitly:

    ```bash
    helm install thingsflow ./k8s/helm/thingsflow \
      --namespace thingsflow --create-namespace \
      --set flowCore.loadDemo=true
    ```

    That seeds `sysadmin/sysadmin` and `tenant/tenant`, and the render is
    refused when `production=true`. For a non-demo install, reset the sysadmin
    password out of band before first login — see the seeded-accounts section
    below.

    `loadDemo` only takes effect on a **first** install. The seed runs from
    Postgres's init directory, which executes only when the data directory is
    empty, so adding `--set flowCore.loadDemo=true` to an existing release
    changes nothing and the login still fails. Verified: on an existing install
    it is a no-op; on a fresh one both accounts return 200.

For a real environment, keep a small operator-owned values file outside the
public documentation surface and override only what is specific to that
installation: ingress host, TLS issuer, resource sizes, storage classes,
secrets, and image tags when you pin a release. Public documentation assumes
ThingsFlow images are available from the public repository packages and does not
require a private registry or in-cluster build pipeline.

```bash
helm upgrade --install thingsflow ./k8s/helm/thingsflow \
  --namespace thingsflow --create-namespace \
  -f values.production.yaml
```

For the production pilot baseline, start from the committed example and layer
your private secret/cluster overlay on top:

```bash
helm upgrade --install thingsflow ./k8s/helm/thingsflow \
  --namespace thingsflow --create-namespace \
  -f k8s/helm/thingsflow/values-production.example.yaml \
  -f private-values.yaml
```

The example profile enables `FLOW_ENV=production`, requires external Secrets for
Postgres, platform JWT signing, Device JWT signing, and NATS auth, and switches
the built-in NATS streams/KV bucket to file-backed persistent storage. It is
safe to commit because it contains only Secret references and placeholder host
names.

For a controlled pilot with OIDC, backup Secret references, stronger resource
requests, persistent NATS, and the demo simulator disabled, start from:

```bash
helm upgrade --install thingsflow ./k8s/helm/thingsflow \
  --namespace thingsflow --create-namespace \
  -f k8s/helm/thingsflow/values-pilot.example.yaml \
  -f private-values.yaml
```

See [Operations](OPERATIONS.md) for the full controlled pilot runbook.

The chart intentionally leaves `ingress.enabled=false` by default. A default
install creates internal `ClusterIP` services only; public UI/API access is
enabled with an operator-owned overlay. See [Operations](OPERATIONS.md) for
the browser/API ingress, MQTT LoadBalancer option, HTTP telemetry edge, and
diagnostic commands for "no public endpoint" cases.

**Login / seeded accounts** — the `sysadmin@thingsboard.org` and
`tenant@thingsboard.org` account *rows* are always seeded so the platform
boots with a SYS_ADMIN and a TENANT_ADMIN, but a default Helm install gives
them a **per-install random password** (no known login) — the well-known
upstream ThingsBoard hash is never shipped. To get the known demo passwords
(`sysadmin/sysadmin`, `tenant/tenant`) on Kubernetes, install with
`--set flowCore.loadDemo=true`; the render is refused for a `production`
install. The local `docker compose` stack always mounts the demo-password
seed, so the Quick Start login works out of the box. For a non-demo install,
reset the sysadmin password out of band before first login.

**Optional demo dataset** — mirrors TB classic's `--load-demo` install
flag. Set `flowCore.loadDemo=true` (or `THINGSFLOW_LOAD_DEMO=true`) and the
bridge seeds Customer A + a customer user + 18 demo devices with access
tokens + a Demo Building asset on first boot. Login with
`customer@thingsboard.org` / `customer`. Off by default; idempotent.

---

## Release and pilot gates

Before tagging or promoting images, run the public release gate and local
contract checks:

```bash
bash tools/check-oss-release.sh
python3 -m unittest \
  tools/python/test_ui_independent_demo_contract.py \
  tools/python/test_control_plane_api_contract.py \
  tools/python/test_oidc_contract.py \
  tools/python/test_oidc_demo_auth_contract.py
(cd flow-core && go test ./...)
bash tools/verify-local.sh
```

For fast automated UI/auth tests, prefer the lightweight Dex profile. In the
local NATS compose stack, Dex runs with a static demo user
`tenant@thingsflow.local` / `tenant` and the OIDC issuer that the browser can
reach on `localhost:5556`:

```bash
OIDC_ENABLED=true docker compose -f docker/docker-compose-nats.yml \
  --profile oidc-dex up -d dex flow-core tb-web-ui
```

For Kubernetes demo installs, `values-demo.yaml` enables the same test provider
through `testAuth.enabled=true`. If you install the base chart directly, enable
it explicitly and port-forward Dex for browser login:

```bash
helm upgrade --install thingsflow ./k8s/helm/thingsflow \
  --namespace thingsflow --create-namespace \
  --set testAuth.enabled=true

kubectl -n thingsflow port-forward svc/thingsflow-test-auth-dex 5556:5556
kubectl -n thingsflow port-forward svc/thingsflow-flow-core 8082:8080
```

For the controlled industrial pilot checklist and public artifact boundary, see
[docs/RELEASE.md](RELEASE.md).

## External auth provider

The chart exposes the OIDC broker through `oidc.*` values. It is disabled by
default. In production, `FLOW_ENV=production` requires `oidc.clientId`,
`oidc.clientSecret` or `oidc.clientSecretExistingSecret.name`,
`oidc.stateSigningKey` or `oidc.stateSigningKeyExistingSecret.name`, and either
`oidc.issuer` or the explicit authorization/token/JWKS URLs when
`oidc.enabled=true`.

Flow Core supports OIDC discovery. For providers such as ZITADEL, Keycloak, and
authentik, an issuer URL can populate the authorization, token, and JWKS
endpoints automatically when the issuer is reachable from Flow Core. If the
provider does not include `email` in the ID token, set `oidc.userInfoUrl` so
Flow Core can use the standard OIDC UserInfo endpoint as a fallback:

```yaml
oidc:
  enabled: true
  providerId: zitadel
  providerTitle: ZITADEL
  clientId: thingsflow-ui
  issuer: https://auth.example.com
  userInfoUrl: https://auth.example.com/oidc/v1/userinfo
  redirectUrl: https://thingsflow.example.com/login/oauth2/code/zitadel
  clientSecretExistingSecret:
    name: thingsflow-oidc
    key: client-secret
  stateSigningKeyExistingSecret:
    name: thingsflow-oidc
    key: state-signing-key
```

For a local ZITADEL trial, use the official ZITADEL compose stack and create a
Web application in the ZITADEL console with redirect URI
`http://localhost:8082/login/oauth2/code/zitadel`. The official local
quickstart exposes the console at
`http://localhost:8084/ui/console?login_hint=zitadel-admin@zitadel.localhost`
when started with `PROXY_HTTP_PUBLISHED_PORT=8084` and
`ZITADEL_EXTERNALPORT=8084`.

If ZITADEL runs on the host or in a separate compose project, keep the
browser-facing issuer as
`http://localhost:8084` but override the server-side token/JWKS URLs through
`host.docker.internal`:

```bash
OIDC_ENABLED=true \
OIDC_PROVIDER_ID=zitadel \
OIDC_PROVIDER_TITLE=ZITADEL \
OIDC_CLIENT_ID=<zitadel-client-id> \
OIDC_CLIENT_SECRET=<zitadel-client-secret> \
OIDC_ISSUER=http://localhost:8084 \
OIDC_AUTHORIZATION_URL=http://localhost:8084/oauth/v2/authorize \
OIDC_TOKEN_URL=http://host.docker.internal:8084/oauth/v2/token \
OIDC_JWKS_URL=http://host.docker.internal:8084/oauth/v2/keys \
OIDC_STATE_SIGNING_KEY=local-zitadel-state-signing-key-32 \
docker compose -f docker/docker-compose-nats.yml up -d flow-core tb-web-ui
```

The `host.docker.internal` mapping is included in the local compose file for
this purpose. In Kubernetes, prefer a real DNS name reachable from both the
browser and Flow Core, or provide explicit internal token/JWKS URLs.

To run ZITADEL in-cluster, use the ThingsFlow-owned chart in
`k8s/helm/zitadel`. It keeps the deployment compact and avoids the official
chart hooks that depend on extra Kubernetes helper images. The chart deploys
ZITADEL `v2.67.x` and exposes the standard discovery endpoint:

```bash
helm --kubeconfig <your-kubeconfig> upgrade --install zitadel-db bitnami/postgresql \
  --namespace thingsflow --wait --timeout 5m \
  --set auth.postgresPassword=<postgres-password> \
  --set auth.database=zitadel \
  -f <zitadel-postgres-values.yaml>

helm --kubeconfig <your-kubeconfig> upgrade --install zitadel k8s/helm/zitadel \
  --namespace thingsflow --wait --timeout 10m \
  -f k8s/helm/zitadel/values-thingsflow-example.yaml
```

With an ingress at `https://zitadel.example.com`, discovery is available at
`https://zitadel.example.com/.well-known/openid-configuration`, and the
console is available at `https://zitadel.example.com/ui/console`. The first
admin login generated by ZITADEL may include the organization domain suffix,
for example `zitadel-admin@zitadel.zitadel.example.com`.

The local chart expects an external PostgreSQL service and a stable masterkey
Secret. It creates only the ZITADEL ConfigMap/Secret, init/setup Jobs,
Deployment, Service, and Ingress. It does not create machine-key helper
sidecars; create the ThingsFlow UI OIDC Web application in the ZITADEL console and
store the generated client secret in the `thingsflow-oidc` Secret.

Keep OIDC client secrets in Kubernetes Secrets or a private values overlay. The
upstream ThingsBoard UI still discovers providers through
`/api/noauth/oauth2Clients`, so enabling an external OIDC provider does not
require a custom UI.

`testAuth.enabled=true` is the exception reserved for demos and automated tests:
the chart deploys Dex, configures Flow Core OIDC automatically, and stores only
test credentials. Production installs must keep `testAuth.enabled=false` and use
an operator-owned external provider through `oidc.*`.

## Image build pipeline

Workflow: [.github/workflows/docker-publish.yml](https://github.com/lirei-lab/thingsflow/blob/main/.github/workflows/docker-publish.yml).

Pushes one image to `ghcr.io/<owner>/`:
- `flow-core` — the Go control plane image. It also contains the
  `alarm-materializer` binary used by the NATS alarm pipeline.

Triggers and tags:

| trigger | tags applied |
|---|---|
| push to `master` / `main` | `:latest`, `:master`, `:sha-<short>` for CI/dev; release charts do not consume `:latest` |
| push to a feature branch | `:<branch>`, `:sha-<short>` |
| `git tag vX.Y.Z` | `:vX.Y.Z`, `:X.Y.Z`, `:sha-<short>`, `:latest` |
| manual run | `:sha-<short>` |

Build cache is keyed per image via `type=gha,scope=...` so cold
rebuilds touch only the layers that changed.

Override at install/upgrade time with a values file or `--set`:

```bash
helm upgrade thingsflow ./k8s/helm/thingsflow \
  --set images.flowCore=ghcr.io/lirei-lab/thingsflow/flow-core:2.2.0
```

---

## Iterating on flow-core (dev loop)

**Fresh install from scratch** — the full stack, built and started with the
exact commands the "Fresh-install smoke" CI workflow executes on PRs, and on
pushes to main/develop, that touch the stack:

```bash
docker compose -f docker/docker-compose-nats.yml up -d --build
bash tools/smoke/fresh-install-smoke.sh
```

First boot on a cold machine takes a few minutes: the images build, then
flow-core seeds the schema and widgets before `http://localhost:8082/ready`
returns 200 (the fresh-install smoke budgets up to 300 s for readiness, then
polls the read API for up to 60 s more). The compose stack exposes the
flow-core API on `:8082`, the Envoy HTTP ingest edge on `:8083`, and RMQTT on
`:1883`, and it always mounts the demo-password seed, so the fresh-install
smoke logs in as `tenant@thingsboard.org` / `tenant` out of the box.

!!! info "The fresh-install smoke is the executable install contract"
    [`tools/smoke/fresh-install-smoke.sh`](https://github.com/lirei-lab/thingsflow/blob/main/tools/smoke/fresh-install-smoke.sh)
    is the **fresh-install contract**: run against the started stack, it
    proves the path a new user walks — demo login, device creation, device
    JWT, telemetry published through both real edges (Envoy HTTP ingest and
    RMQTT, never a shortcut into the store), and the rows read back through
    BOTH storage pipelines — the GreptimeDB history read and the NATS KV
    latest-values read. CI executes it in the "Fresh-install smoke" workflow
    ([fresh-install-smoke.yml](https://github.com/lirei-lab/thingsflow/blob/main/.github/workflows/fresh-install-smoke.yml)),
    and the OSS release gate pins this section to the contract: a test fails
    CI if the two commands above stop matching what the workflow runs.

**Inner dev loop** — while iterating on flow-core itself, the fast loop is a
service subset plus `tools/smoke-local.sh`:

```bash
# one-time: bring the local stack up (postgres, greptimedb, nats, rmqtt-edge, http-ingest, ui)
docker compose -f docker/docker-compose-nats.yml up -d \
  postgres greptimedb nats nats-bootstrap flow-core rmqtt-edge \
  http-ingest-bento http-ingest nats-latest-kv nats-greptimedb \
  nats-alarms alarm-materializer

# inner loop (~30s end-to-end)
bash tools/smoke-local.sh                # build + restart + smoke
bash tools/smoke-local.sh --no-build     # restart + smoke (~30s)
bash tools/smoke-local.sh --only-smoke   # verify current image (~12s)
```

`tools/smoke-local.sh` is the **inner dev loop** smoke, complementary to the
fresh-install contract above: it covers the local stack end to end through the
device provisioning path — login -> device create -> MQTT publish via RMQTT ->
GreptimeDB/NATS KV landing -> ACL reject.

There is no equivalent `helm test` suite in the chart; `helm test <release>`
reports `TEST SUITE: None`. To verify a Kubernetes install, run the same path
by hand, or point the load generator at the release:

```bash
benchmarks/scripts/loadgen2/run.sh run --target thingsflow --protocol mqtt \
  --devices 20 --rate 100 --duration 30 --verify-landed \
  --api-base http://<flow-core>:8080 --ingest-base http://<http-ingest>:8081 \
  --mqtt-host <rmqtt-edge> --mqtt-port 1883 \
  --greptime-base http://<greptimedb>:4000
```

`--verify-landed` is the part that matters: it counts the rows that actually
reached the store instead of trusting the acknowledgements.

When the change is settled locally, publish versioned public images through the
repository CI and promote by updating the chart `version` or overriding
`images.flowCore` with an immutable release tag or digest:

```bash
helm upgrade thingsflow ./k8s/helm/thingsflow \
  --namespace thingsflow \
  --set images.flowCore=ghcr.io/lirei-lab/thingsflow/flow-core:2.2.0
```

Cluster-specific build systems are operator concerns. They are not required for
public installs and should not appear in the public getting-started path.

---

## What gets seeded at boot

Idempotent — every step is a no-op when the target table is already
populated. Source: `bootstrap.LoadSystem()` in
[internal/bootstrap/bootstrap.go](https://github.com/lirei-lab/thingsflow/blob/main/flow-core/internal/bootstrap/bootstrap.go),
plus the helpers wired into [main.go](https://github.com/lirei-lab/thingsflow/blob/main/flow-core/main.go).

| Step | Source | Idempotent on |
|---|---|---|
| Schema + seed tenant | `k8s/helm/thingsflow/files/sql/*.sql` | postgres init dir |
| Migrations (golang-migrate) | `internal/migrations/migrations/*.sql` (embed.FS) | `schema_migrations` |
| Widget types + bundles (522 + N) | `flow-core/tb-resources/widget_*/*.json` | `widget_type` count > 0 |
| System images (891 incl. SCADA) | `flow-core/tb-resources/system_images/*` + `_manifest.json` | `(tenant, type, key)` UNIQUE |
| Bundle thumbnails normalized | `rewriteInlineTbImage()` at insert | per row |
| OAuth2 templates | `flow-core/tb-resources/oauth2_templates/*.json` | `oauth2_client_registration_template` count > 0 |
| Demo dashboards | `flow-core/tb-resources/demo_dashboards/*.json` | `dashboard` rows exist for tenant |
| Demo dataset (opt-in) | `bootstrap.LoadDemo()` — Customer A + customer user + 18 demo devices + Demo Building asset | demo customer row presence; gated by `THINGSFLOW_LOAD_DEMO=true` (Helm: `flowCore.loadDemo`) |
| `audit_log` partitions + retention | `audit.StartPartitionManager()` | partition naming |
| `alarm` retention sweep | `postgres-alarm-retention` CronJob (weekly) | deletes cleared+acked alarms older than `retention.alarm.days` (180) |
| GreptimeDB history table | `bento-nats-greptimedb` writes `device_telemetry_kv` through HTTP Influx Line Protocol | row count and chart window reads |
| QuestDB `device_telemetry` TTL (optional) | `telemetry.ensureTTL()` (boot + first ILP write) | QuestDB `ALTER TABLE … SET TTL N DAY` |
| `api_usage_state` row per tenant | `usage.StartReporter()` | `(tenant_id, entity_id)` UNIQUE |

---

## Retention & TTL

Retention is an operations contract, not an install-time choice: every store that
accumulates data has a bounded window, and the windows are verified to be applied.
The values, the guards and the verification signals live in
[Retention observability](OPERATIONS.md#retention-observability).

What matters at install time: **retention and durability are separate controls.**
`retention.greptimedb.ttl` defines how long history stays queryable; GreptimeDB
persistence or object storage defines whether it survives a pod reschedule, node
failure, or storage maintenance. Enable durable GreptimeDB storage before connecting
real devices whose history matters.

---

## Environment variables

**Preflight-asserted** — with `FLOW_ENV=production`, Flow Core refuses to boot if any
of these is unset (`preflightCheck` in `flow-core/main.go`):

| Var | Purpose |
|---|---|
| `SPRING_DATASOURCE_URL` | Postgres JDBC URL (`jdbc:postgresql://host:5432/db`). |
| `SPRING_DATASOURCE_PASSWORD` | Postgres password. |
| `TSDB_HOST` | Telemetry-store host, whichever backend is selected. `TSDB_PG_DSN` satisfies the check instead. The legacy name `QUESTDB_HOST` is still accepted. |
| `JWT_TOKEN_SIGNING_KEY` | HS512 secret, base64-encoded. Boot fails in production if it is unset, not valid base64, or decodes to fewer than 32 bytes. |
| `ALLOWED_ORIGIN` | Specific UI origin (e.g. `https://app.example.com`). `*` in production triggers a CORS warning + blocks the credentials flag. |

**Required in practice but not preflight-asserted** — a wrong value degrades or breaks
a subsystem at first use rather than at boot:

| Var | Purpose |
|---|---|
| `NATS_URL` | NATS client URL for the event plane. |
| `SPRING_DATASOURCE_USERNAME` | Postgres user (defaults apply if unset). |
| `TELEMETRY_HISTORY_STORE` | `greptimedb` by default; `questdb` only for explicit optional installs. |
| `TSDB_PG_DSN` | GreptimeDB PostgreSQL-compatible reader DSN. |
| `TELEMETRY_KV_TS_COLUMN` | Timestamp column name for the selected historical backend. |
| `TSDB_PG_HOST/PORT/USER/PASS/DB` | Reader connection, used when `TSDB_PG_DSN` is unset. Backend-neutral: they point at GreptimeDB in the default profile. The legacy `QUESTDB_PG_*` names are still read as a fallback and log a deprecation warning. |

`EVENT_BROKER=nats` is set by the chart and asserted by
`tools/verify-pilot-acceptance.sh` as a posture marker, but Flow Core itself never
reads it.

Operational tuning (defaults shown):

| Var | Default | Purpose |
|---|---|---|
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error`. |
| `LOG_FORMAT` | `json` | `text` for dev. |
| `FLOW_ENV` | (unset) | Set to `production` to activate preflight. |
| `PG_MAX_OPEN_CONNS` | 30 | Postgres pool ceiling. |
| `PG_MAX_IDLE_CONNS` | 10 | Idle pool target. |
| `AUDIT_LOG_RETENTION_DAYS` | 90 | Partitions older than N days are dropped. Read by Flow Core, not set by the chart — the one retention window still driven from the application tier. |
| `DEVICE_JWT_TTL_SECONDS` (`flowCore.deviceJwt.ttlSeconds`) | 86400 | Device JWT lifetime. There is no per-message revocation check at the edge, so this **is** the revocation window; the cluster overlay uses `900` (15 min). |
| `DEVICE_TELEMETRY_TTL_DAYS` | 90 | QuestDB-only `ALTER TABLE … SET TTL` when `timeseries.store=questdb`. |
| `DEVICE_INACTIVITY_TIMEOUT_SECONDS` | 60 | Marks devices INACTIVE after this gap. |

---

## Backup & restore

Owned by [Backup And Restore](OPERATIONS.md#backup-and-restore): backup values
(Secret-referenced, not inline), the storage-class fallback for block storage, the
restore procedure for Postgres and GreptimeDB, and the monthly drill template.

Postgres is backed up with `pg_dump`; GreptimeDB history relies on its
object-storage layout. The legacy QuestDB filesystem backup stays optional for
installs that set `timeseries.store=questdb`.

---

## Maintenance — bumping upstream TB OSS

`flow-core/tb-resources/system_images/` (891 images plus `_manifest.json`,
~18 MB) is committed to the repo, alongside the other seeded catalogues
(`widget_types/`, `widget_bundles/`, `scada_symbols/`, `oauth2_templates/`,
`demo_dashboards/`). When TB OSS adds new widgets or renames image keys,
refresh the bundle locally, then commit the diff:

```bash
# Recover the previews TB baked into its widget JSONs as inline base64.
# Walks upstream git history because no single commit has them all.
python3 tools/python/extract-system-images.py

git add flow-core/tb-resources/system_images/
git diff --cached --stat
git commit -m "Bump TB OSS system images to <version>"
```

This is explicitly a *maintenance* tool — it is not part of any install
or deploy path. The build picks up whatever is committed, and
`flow-core/internal/bootstrap` seeds it into Postgres on first start.

**Known gap:** a handful of image keys (`gateway_*`, `api-usage-widget.png`,
`service_rpc_*`) are referenced from widget JSONs but exist in neither
upstream OSS source nor its prebuilt DB, so those previews render a
"no image" placeholder. Cosmetic only.

---

## Other tooling

- `tools/python/extract-system-images.py` — walks upstream git history for
  inline `tb-image:<keyB64>:…;data:…;base64,…` payloads under
  `widget_types/`, `widget_bundles/` and `demo/dashboards/`, decoding them
  into `flow-core/tb-resources/system_images/`. Maintenance only.
- `tools/verify-platform-health.sh` — end-to-end health probe against a
  running deployment.
- `tools/generate-pilot-secrets.sh` — creates the Kubernetes Secrets the
  cluster overlay expects.

---

## Troubleshooting

**Widget thumbnails show "No image preview".** Check that
`flow-core/tb-resources/system_images/` is non-empty in the running
image, and that `loadSystemImages` ran:

```
$ kubectl -n thingsflow logs deploy/thingsflow-flow-core | grep 'system images:'
```

Should report `seeded N/N entries`. If it says `manifest not present`,
the image was built without the bytes.

**Bundle thumbnails show alt-text instead of an icon.** A row in
`widgets_bundle.image` still holds the `tb-image:<keyB64>:…;data:…;base64,…`
inline form (UI only renders raw `data:` URLs or `tb-image;<url>`
refs). Fixed at insert by `rewriteInlineTbImage()`; if rows predate
that fix, run:

```sql
UPDATE widgets_bundle
   SET image = 'tb-image;/api/images/system/'
            || convert_from(decode(split_part(image, ':', 2), 'base64'), 'UTF8')
 WHERE image LIKE 'tb-image:%';
```

**`helm install` pulls fail with `unauthorized` from ghcr.io.** The
public release expects public package visibility. If your organization mirrors
or privatizes images, create an `imagePullSecret`:

```bash
kubectl -n thingsflow create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io \
  --docker-username=<gh-user> \
  --docker-password=<PAT-with-read:packages>

helm upgrade thingsflow ./k8s/helm/thingsflow \
  --set 'images.pullSecrets[0]=ghcr-pull'
```

**QuestDB `device_telemetry` is logged as "TTL deferred".** Expected only when
`timeseries.store=questdb` on a fresh cluster — QuestDB creates the table
lazily on the first ILP write. The TTL gets applied via the `sync.Once` hook
the first time a device publishes. Verify after first publish:

```bash
kubectl -n thingsflow exec thingsflow-questdb-0 -- \
  curl -sG --data-urlencode \
    "query=SELECT table_name, ttlValue, ttlUnit FROM tables() WHERE table_name='device_telemetry'" \
    'http://localhost:9000/exec'
```

**GreptimeDB dashboard shows zero tables.** The default table is
`device_telemetry_kv` and it appears only after a valid event reaches NATS and
the `bento-nats-greptimedb` materializer writes it. Check the full path:

```bash
kubectl -n thingsflow logs deploy/thingsflow-rmqtt-edge --tail=120
kubectl -n thingsflow logs deploy/thingsflow-nats-greptimedb --tail=120
kubectl -n thingsflow exec -it thingsflow-greptimedb-0 -- \
  curl -sS -X POST -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'sql=SHOW TABLES' \
    'http://localhost:4000/v1/sql?db=public'
```

If RMQTT logs `authorization violation` while NATS CLI credentials work, verify
the rendered RMQTT bridge config uses `servers = "nats://thingsflow-nats:4222"`
plus `auth.username` and `auth.password`, not URL userinfo.

**Audit log is empty.** Make sure the partition manager started
(`audit_log: dropped …` or no log at all means it ran). If you see
`relation … no partition` errors on insert, the per-month partition
for the current `created_time` is missing — `EnsureAuditPartitions()`
should have created it at boot. Re-run by restarting the pod or call
the function via a one-shot debug shell.
