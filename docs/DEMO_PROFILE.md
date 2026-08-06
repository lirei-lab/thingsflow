# Demo Profile

The default ThingsFlow chart installs a minimal platform: control plane, native
NATS data plane, ThingsBoard UI compatibility console, and storage. It does not
create sample devices or run telemetry generators.

Use the demo profile when you want the ThingsBoard-compatible UI to show populated classic dashboards immediately.

## Local demo (compose)

The same demo dataset and simulator run on the local compose stack. From the
repository root:

```bash
THINGSFLOW_LOAD_DEMO=true docker compose -f docker/docker-compose-nats.yml --profile demo up -d
```

`THINGSFLOW_LOAD_DEMO=true` mirrors `flowCore.loadDemo`: the dataset seed is
flow-core-side and idempotent — setting the flag on any boot seeds it, and it
self-heals partial seeds. A fresh install is not required: recreating only
flow-core with the flag on an already-running stack seeds the same dataset:

```bash
THINGSFLOW_LOAD_DEMO=true docker compose -f docker/docker-compose-nats.yml up -d flow-core
```

`--profile demo` adds the `demo-simulator` service, which logs in as the
tenant and publishes MQTT telemetry for all 18 seeded devices every few
seconds.

Open the UI at http://localhost:3001 and log in as `tenant@thingsboard.org` /
`tenant`, or as the seeded customer user `customer@thingsboard.org` /
`customer`. Within roughly 30–60 seconds of the simulator starting,
`Thermostats`, `SCADA Process Demo`, and `Smart Building Office Demo` show
live data (thermostat temperature/humidity/HVAC state, pump/valve/tank
process values, office IAQ and occupancy). `Firmware` and `Software` are OTA
management dashboards — they list devices and firmware/software update state
rather than streaming telemetry.

Note that the compose stack is ephemeral: Postgres has no named volume, so
`docker compose -f docker/docker-compose-nats.yml down` discards all platform
state. The next boot with the flag reseeds from scratch.

To stop the demo stack:

```bash
docker compose -f docker/docker-compose-nats.yml --profile demo down
```

## What It Enables

`k8s/helm/thingsflow/values-demo.yaml` sets:

```yaml
flowCore:
  loadDemo: true

demoSimulator:
  enabled: true
```

`flowCore.loadDemo` seeds the default tenant with the demo customer, demo asset, classic demo devices, credentials, relations, OTA examples, and demo dashboard dependencies.

`demoSimulator.enabled` runs a small MQTT simulator that publishes telemetry for
the seeded devices through RMQTT and NATS. Flow Core still remains the control
plane; telemetry flows through the independent data plane.

## Install

```bash
helm upgrade --install thingsflow k8s/helm/thingsflow \
  -n thingsflow --create-namespace \
  -f k8s/helm/thingsflow/values-demo.yaml
```

For a private pilot cluster, put registry mirrors, hosts, TLS, and load balancer settings in a separate private values file and pass it after the public demo file:

```bash
helm upgrade --install thingsflow k8s/helm/thingsflow \
  -n thingsflow --create-namespace \
  -f k8s/helm/thingsflow/values-demo.yaml \
  -f path/to/private-cluster-values.yaml
```

## Expected UI Demos

The UI should list and load these dashboards with live data:

- `Thermostats`
- `SCADA Process Demo`
- `Smart Building Office Demo`
- `Firmware`
- `Software`

The demo profile seeds 18 devices and the `Demo Building` asset. Device
telemetry is produced continuously by the simulator and materialized into
GreptimeDB plus NATS KV latest/twin hot state for dashboard compatibility.

## Verify

Run:

```bash
tools/verify-demo-profile.sh thingsflow
```

If your Helm release name is not `thingsflow`, set `RELEASE`:

```bash
RELEASE=my-release tools/verify-demo-profile.sh my-namespace
```

The verifier checks:

- demo dashboards exist;
- 18 demo devices exist;
- `Demo Building` exists;
- 18 relations from `Demo Building` exist;
- every demo device has latest telemetry keys;
- `demo-simulator` is running.

## Notes

The demo profile is for evaluation, demos, and pilot validation. Production installs should decide explicitly whether seeded data and a simulator are appropriate.
