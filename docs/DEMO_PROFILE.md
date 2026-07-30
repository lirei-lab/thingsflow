# Demo Profile

The default ThingsFlow chart installs a minimal platform: control plane, native
NATS data plane, ThingsBoard UI compatibility console, and storage. It does not
create sample devices or run telemetry generators.

Use the demo profile when you want the ThingsBoard-compatible UI to show populated classic dashboards immediately.

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
- `Rule Engine Statistics`

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
