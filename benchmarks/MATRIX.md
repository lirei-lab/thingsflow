# Benchmark Matrix

This matrix defines the first fair comparison set for ThingsFlow and ThingsBoard
Classic. It is intentionally conservative: every scenario must be reproducible
on the same Kubernetes cluster, with the same load generator, same resource
class, same device count, same payload shape, and clean namespaces.

## Targets

| Target | Stack |
|---|---|
| `thingsflow` | RMQTT, HTTP ingest, NATS, Bento, GreptimeDB, Postgres, Flow Core, optional TB UI. |
| `thingsboard-classic` | Official `thingsboard/tb-node` with PostgreSQL/TimescaleDB. |

ThingsFlow is measured as an event-driven middleware where Flow Core is the control plane and ThingsFlow data plane is the telemetry path. ThingsBoard Classic is measured as the classic monolith baseline using TimescaleDB for time-series storage.

## Scenario Set

| Scenario | Protocol | Devices | Interval | Duration | Purpose |
|---|---:|---:|---:|---:|---|
| `mqtt-100` | MQTT | 100 | `PUBLISH_INTERVAL_SECONDS=5` | `RUN_DURATION_SECONDS=900` (15 min) | Basic sustained publish and latest-value visibility. |
| `mqtt-1000` | MQTT | 1000 | `PUBLISH_INTERVAL_SECONDS=10` | `RUN_DURATION_SECONDS=1800` (30 min) | Broker and ingest pressure with many devices. |
| `http-1000` | HTTP | 1000 | `PUBLISH_INTERVAL_SECONDS=10` | `RUN_DURATION_SECONDS=1800` (30 min) | HTTP ingress path and control/data-plane separation. |
| `mqtt-burst-1000` [^matrix-only] | MQTT | 1000 | 100 ms burst windows | 5 min | Burst tolerance and backpressure behavior. |
| `cold-start` [^matrix-only] | MQTT | 100 | 1 s | 5 min | Time to readiness, first successful publish, and dashboard visibility. |

Intervals and durations for the first three scenarios are the exact values
committed in `benchmarks/scenarios/*.env` (`mqtt-100.env`, `mqtt-1000.env`,
`http-1000.env`).

Only the first three scenarios are required for the initial public benchmark
package. Burst and cold-start are recommended pilot extensions.

[^matrix-only]: matrix-only: run as overrides over mqtt-1000.env; no committed scenario file

## Metrics

| Metric | Required | Notes |
|---|---|---|
| Publish attempts | yes | From the load generator. |
| Publish success rate | yes | Failed baseline if below gate. |
| Client-side latency | yes | Median, p95, p99 when available. |
| Ingest-to-latest latency | yes | Publish known value, query latest/read dashboard. |
| Pod CPU and memory | yes | Same observation window for both targets. |
| Restarts | yes | Any restart must be explained. |
| Time to readiness | yes | Cold install and warm restart. |
| Storage growth | yes | Postgres/TimescaleDB/GreptimeDB PVC growth. |
| Log volume | yes | Especially to confirm telemetry payloads are not logged. |
| NATS stream/consumer state and KV freshness | ThingsFlow only | Expected for event-driven architecture. |
| Rule-engine queue health | ThingsBoard only | Required to avoid invalid TB baseline runs. |

For aggressive ThingsFlow MQTT tests, also record RMQTT HTTP API output for
`/api/v1/brokers`, `/api/v1/nodes`, `/api/v1/clients`, and `/api/v1/plugins`.
These endpoints are the broker-level evidence used to decide whether the broker
or a downstream materializer is the bottleneck.

## Aggressive RMQTT Ladder

| Scenario | Devices | Interval | Duration | Shards | Expected publishes |
|---|---:|---:|---:|---:|---:|
| `rmqtt-100-smoke` [^matrix-only] | 100 | 1 s | 60 s | 4 | 6,000 |
| `rmqtt-1000-aggressive` [^matrix-only] | 1000 | 1 s | 60 s | 8 | 60,000 |
| `rmqtt-2000-aggressive` [^matrix-only] | 2000 | 1 s | 60 s | 16 | 120,000 |
| `rmqtt-5000-stress` [^matrix-only] | 5000 | 1 s | 60 s | 25 | 300,000 |

RMQTT multi-node is a separate architecture profile, not a replica count. The
single-node broker remains valid while broker CPU, memory, connection state,
and publish admission remain healthy. Move to RMQTT clustering only when RMQTT
itself is the first saturated component or when availability requirements
demand broker failover.

The 2026-05-19 pilot evidence reached the `rmqtt-5000-stress` gate with
299,998 observed publishes, no load-generator error lines, no runtime restarts,
and broker CPU/memory still low after the run.

The earlier latest-state bottleneck was latest-value fan-out into Postgres, not
RMQTT. The current architecture measures NATS KV freshness for hot state and
keeps Postgres focused on operational state and eventual snapshots.

## Acceptance Gates

A run is valid only if:

- target namespace starts from a clean state;
- all pods are ready before load starts;
- no pod enters crash loop during the run;
- publish success rate is at least `99.5%`;
- known latest telemetry is queryable after the run;
- dashboard smoke opens without blank widgets for the relevant demo;
- logs do not contain raw telemetry payload dumps;
- ThingsBoard Classic does not show partition or queue initialization errors;
- ThingsFlow data-plane materializer has no unexplained consumer lag after the
  cool-down window.

If a gate fails, the result should be recorded as a failed validation, not as a
performance comparison.

## Resource Fairness

Document for every run:

- Kubernetes version;
- node count and node type;
- storage class;
- image tags;
- chart versions;
- CPU and memory requests/limits;
- database retention and time-series configuration;
- benchmark generator image and scenario file;
- start and end timestamps.

Do not commit private cluster hostnames, registry names, or kubeconfig paths in
public results.

## Result Template

Use one result file per target and scenario:

```text
benchmarks/results/YYYYMMDD-target-scenario.md
```

Recommended fields:

```markdown
# Benchmark Result

Target:
Scenario:
Date:
Cluster:
Images:
Resources:
Storage class:

## Gates

| Gate | Result | Evidence |
|---|---|---|

## Metrics

| Metric | Value |
|---|---:|

## Notes

```

Raw generated output can live outside the public repository when it contains
cluster-specific details. Public results should include enough metadata for
repeatability without exposing private infrastructure.

## First Pilot Sequence

1. Install ThingsFlow from the public chart with production-like settings.
2. Run `mqtt-100`, `mqtt-1000`, and `http-1000`.
3. Install ThingsBoard Classic with TimescaleDB in a separate namespace.
4. Run the same three scenarios.
5. Collect metrics immediately after every run.
6. Summarize with `benchmarks/scripts/summarize-results.py`.
7. Publish methodology and sanitized result summaries only after all gates pass.
