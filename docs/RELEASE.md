# Release

This page consolidates project identity, publication, OSS release, and pilot
readiness rules.

## Project Identity

**ThingsFlow** is open industrial IoT middleware for event-driven telemetry,
digital twins, data-plane alarms, and optional ThingsBoard UI compatibility.

ThingsFlow is not a ThingsBoard distribution and not a JVM runtime repackaging. It
is an independent architecture:

- Flow Core provides the Go control plane and compatibility API;
- ThingsFlow data plane provides the NATS-first telemetry path;
- the twin model provides the native digital-twin vocabulary;
- the ThingsBoard UI adapter is an optional compatibility console.

Compatibility language:

- use "ThingsBoard UI compatibility" when describing supported UI/API shapes;
- do not describe ThingsFlow as "ThingsBoard";
- do not imply endorsement by ThingsBoard, Inc.;
- keep trademarks and attribution clear.

Runtime identifiers are `thingsflow` for the Helm chart/release/namespace,
`flow-core` for the Go service/image component, `thingsflow_device_sdk` for the
Python import, and `thingsflow/devices/...` for the MQTT topic namespace.

The Python device SDK (`sdk/python`, distribution `thingsflow-device-sdk`)
tracks the platform minor version: platform `2.2.x` ships the SDK as `2.2.x`,
and SDK patch releases may diverge when the SDK needs fixes between platform
releases. The runtime `thingsflow_device_sdk.__version__` and the version in
`sdk/python/pyproject.toml` must always agree; the OSS release gate asserts
this on every pull request.

## Public Artifact Boundary

The public tree must contain:

- `LICENSE`, `NOTICE`, `README.md`, `SECURITY.md`, `MAINTAINERS`;
- public docs under `docs/`;
- `docs/api/openapi.yaml`;
- the public Helm chart under `k8s/helm/thingsflow`;
- portable benchmark scenarios and scripts.

The public tree must not contain:

- private kubeconfigs or `cluster.yaml`;
- private registry, Harbor, or organization endpoints;
- generated runtime data;
- secrets, tokens, or private values overlays;
- upstream ThingsBoard-only workflow leftovers.

Run:

```bash
bash tools/check-oss-release.sh
```

The OSS release gate checks required files, private-pattern leaks, stale
security references, public chart image policy, and Helm rendering when Helm is
available.

## Image And Chart Policy

- Release charts must not depend on moving `:latest` tags.
- Public image references should be explicit and reproducible.
- Feature-branch images are acceptable for development values, not release
  values.
- Production installs should use private values overlays for secrets and
  environment-specific hostnames.

## Publication Checklist

Before publishing:

- run unit tests and chart template checks;
- run `bash tools/check-oss-release.sh`;
- verify `docs/index.md`, `README.md`, and `mkdocs.yml` point to the canonical
  docs only;
- verify no deleted markdown remains linked;
- verify GitHub Pages can build the docs site;
- tag the release only after the chart, docs, and images agree.

### Publishing the SDK to PyPI (optional)

Publishing `thingsflow-device-sdk` to PyPI is an operator decision, not a CI
step. No PyPI credentials live in the repository or in workflow secrets. To
publish a tagged release:

```bash
python -m build sdk/python
python -m twine upload sdk/python/dist/*
```

Supply upload credentials (for example a PyPI API token) from the operator
environment at upload time. Skipping PyPI publication is a valid choice:
devices and gateways can install the SDK directly from a repository checkout.

## Pilot Readiness

ThingsFlow is a release candidate for controlled industrial pilots, not a blanket
production claim. A pilot is ready only when these gates pass:

- production mode is enabled;
- development credentials are replaced;
- Postgres password comes from a private value or existing Secret;
- JWT signing keys and device JWT ES256 keys are pinned;
- NATS auth and persistence are enabled for the built-in event plane;
- public UI/API ingress uses HTTPS and explicit CORS;
- public MQTT/HTTP telemetry exposure uses TLS or a trusted private network;
- backups are configured and a restore has been tested;
- `tools/verify-pilot-acceptance.sh` passes;
- benchmark evidence captures throughput, errors, lag, restart counts, and
  storage growth.

The Pilot acceptance gate should validate at least **2,000** expected devices
and **120,000** published messages for the controlled benchmark profile unless
a smaller pilot scope is explicitly documented.

## Pilot Release Candidate Gate

Before calling an install a serious pilot release candidate, run the gates below
against the exact image tag and chart values that will be used in the pilot:

```bash
python tools/verify-device-sdk-live.py
kubectl --kubeconfig <your-kubeconfig> -n thingsflow port-forward svc/thingsflow-rmqtt-edge 18883:1883
uv run --with paho-mqtt python tools/verify-device-sdk-mqtt-live.py
python tools/verify-device-jwt-lifecycle.py
tools/verify-pilot-acceptance.sh
```

The lifecycle smoke is mandatory. A suspended device must not receive fresh
Device JWTs from control-plane renewal. Already-issued bearer JWTs remain valid
until TTL/JWKS-cache expiry unless the deployment adds online revocation, so
pilot TTLs must be sized to the exposure model.

## Pilot Hardening Plan

The production-pilot plan is deliberately centered on the current architecture:
RMQTT/HTTP ingest to NATS, Bento materializers, NATS KV for latest/twin hot
state, GreptimeDB for telemetry history, Postgres for metadata/alarms/audit,
and Flow Core as the control plane and ThingsBoard UI compatibility adapter.
Flow Core must remain outside the telemetry hot path.

### Phase 1: Acceptance Gate

Make `tools/verify-pilot-acceptance.sh` the required gate after every chart,
security, OIDC, data-plane, or image change. The gate verifies:

- the chart renders with the controlled pilot values;
- no removed-runtime leftovers are present;
- required production Secrets exist without printing secret values;
- NATS, GreptimeDB, Postgres, RMQTT, Bento processors, alarm materializer, Flow
  Core, and the UI are rolled out;
- runtime pods have no unexpected restarts;
- Flow Core is in `FLOW_ENV=production`;
- the active config is NATS-first and GreptimeDB-backed;
- OIDC discovery is published and includes the standard authorization broker;
- public verifier endpoints for Device JWTs are reachable;
- the benchmark publishes the required message count with bounded errors;
- NATS KV latest/twin hot state has the expected key volume;
- GreptimeDB contains historical telemetry rows after the drain window.

### Phase 2: Security Closure

Pilot installs must use operator-owned Kubernetes Secrets for platform JWTs,
Device JWT ES256 keys, Postgres, NATS auth, OIDC, and backups. Device JWTs
should use bounded TTLs; higher-risk pilots should add immediate revocation or
online introspection before exposing telemetry edges to untrusted networks.

OIDC remains an external identity-provider integration. Flow Core brokers OIDC
into local Flow Core sessions, verifies callback state, supports discovery, and uses
UserInfo when providers such as ZITADEL keep `email` out of the ID token.

### Phase 3: Observability And Capacity Evidence

Every pilot benchmark should capture publish count, errors, pod CPU/memory,
restarts, NATS KV freshness, NATS consumer lag, GreptimeDB row growth, and UI
latest-value visibility. Publish the result as evidence for the configured
payload shape and retention policy, not as a universal capacity claim.

The comparison ladder is:

| Devices | Messages/min | Purpose |
|---:|---:|---|
| 100 | 6,000 | Smoke and dashboard visibility. |
| 1,000 | 60,000 | Normal pilot pressure. |
| 2,000 | 120,000 | Required acceptance gate. |
| 5,000 | 300,000 | Stress evidence before production expansion. |

### Phase 4: Durability

Postgres backup/restore must be tested before a pilot handles real operational
metadata. NATS JetStream/KV must use file storage and a PVC. GreptimeDB should
move from standalone PVC-only storage to an object-storage-backed deployment
for production telemetry durability when the pilot scope requires retention
beyond controlled tests.
