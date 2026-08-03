# ThingsFlow vs ThingsBoard Classic Benchmark

This package defines a reproducible benchmark harness for comparing ThingsFlow and
classic ThingsBoard under controlled conditions. The scenario matrix, gates,
and result template live in [MATRIX.md](MATRIX.md). The goal is not to publish a
single universal winner. The goal is to make the methodology explicit enough
that operators can repeat the test on the same cluster, with the same load
generator, the same resource constraints, and the same telemetry scenarios.

## What This Compares

| Target | Runtime shape | Purpose |
|---|---|---|
| `thingsflow` | Flow Core + ThingsFlow data plane + RMQTT + HTTP ingest + NATS + Bento + GreptimeDB + Postgres | Validate the independent control/data-plane architecture. |
| `thingsboard-classic` | Official ThingsBoard CE 4.2.0 in **cluster mode**: Kafka (KRaft) + Redis + 2x `tb-node` + separate `tb-http-transport` / `tb-mqtt-transport`, time-series in **plain PostgreSQL** | Provide a distributed baseline under the same cluster and load conditions. |

The baseline uses the official ThingsBoard CE 4.2.0 images in **cluster mode**:
Kafka (KRaft) as the queue, Redis as the cache, two `tb-node` replicas, and the
device transports (`tb-http-transport`, `tb-mqtt-transport`) as separate
deployments. This is not the monolith: both sides of the comparison are
distributed deployments, which is the only shape in which the resource question
is meaningful.

Time-series storage is **plain PostgreSQL** -- `DATABASE_TS_TYPE=sql`, writing to
`ts_kv` joined through `key_dictionary`. It is **not** TimescaleDB and **not**
Cassandra. That choice is deliberate and it is not neutral: either alternative
would change ThingsBoard's write path and therefore its numbers. Any published
result must name the backend it measured; a `sql` run does not license a general
claim about ThingsBoard. (Earlier revisions of this file said `timescale`; that
was never what was deployed.)

The baseline must pass a health gate before results are accepted:
HTTP telemetry must return 200, MQTT clients must remain connected, and logs
must not contain the TB_RULE_ENGINE partition-missing error. If that gate fails,
the run is a failed baseline validation rather than a valid platform
comparison.

## Fairness Rules

Run both targets with:

- the same cluster and node pool;
- the same load generator image and scenario file;
- the same device count, publish interval, payload shape, and duration;
- pinned images and chart versions;
- equivalent Kubernetes requests/limits where possible;
- empty namespaces and fresh persistent volumes before each cold run;
- the same observation window for CPU, memory, readiness, errors, and latency.

Do not compare a warmed ThingsFlow deployment against a cold ThingsBoard Classic
deployment, or a local-path disk against a production SSD class, and call that a
platform result.

## Metrics To Capture

Minimum metrics:

- image size and pod cold start time;
- time to readiness;
- publish success rate and client-side publish errors;
- ingest-to-query latency for latest values;
- pod CPU and memory;
- restart count;
- broker/event-bus lag when available;
- PVC growth: Postgres (ThingsBoard) and GreptimeDB + NATS (ThingsFlow);
- log volume;
- dashboard/latest-value visibility after load.

Optional metrics:

- Prometheus `rate(container_cpu_usage_seconds_total[1m])`;
- memory working set;
- NATS stream/consumer state and KV freshness;
- GreptimeDB write throughput;
- Postgres connection pool saturation.

## Quick Start

Install ThingsFlow normally from the public chart, then install the classic
ThingsBoard baseline:

```bash
KUBECONFIG=/path/to/kubeconfig helm upgrade --install tb-classic benchmarks/helm/thingsboard-classic \
  -n tb-classic-bench --create-namespace
```

Run the same MQTT scenario against each target:

```bash
KUBECONFIG=/path/to/kubeconfig WAIT_FOR_COMPLETION=true \
  benchmarks/scripts/run-benchmark.sh thingsflow benchmarks/scenarios/mqtt-100.env
KUBECONFIG=/path/to/kubeconfig WAIT_FOR_COMPLETION=true \
  benchmarks/scripts/run-benchmark.sh thingsboard-classic benchmarks/scenarios/mqtt-100.env
```

For HTTP scenarios, ThingsFlow intentionally uses separate targets so control-plane
provisioning and telemetry can evolve independently. The current public
benchmark focuses on the native MQTT path: Flow Core provisions devices and
issues device JWTs; RMQTT validates MQTT sessions and writes accepted telemetry
to NATS.

Collect cluster metrics during or immediately after the run:

```bash
KUBECONFIG=/path/to/kubeconfig benchmarks/scripts/collect-metrics.sh thingsflow thingsflow-bench
KUBECONFIG=/path/to/kubeconfig benchmarks/scripts/collect-metrics.sh thingsboard-classic tb-classic-bench
```

For aggressive ThingsFlow MQTT runs, collect the full evidence pack. Use a unique
`DEVICE_PREFIX` per run and disable background demo traffic during the
observation window if you want GreptimeDB counts to map cleanly to the benchmark:

```bash
KUBECONFIG=/path/to/kubeconfig RUN_ID=rmqtt1k-YYYYMMDD \
  DEVICE_PREFIX=rmqtt1k-YYYYMMDD DEVICE_COUNT=1000 LOADGEN_SHARDS=8 \
  PUBLISH_INTERVAL_SECONDS=1 RUN_DURATION_SECONDS=60 MQTT_AUTH_MODE=deviceJwtRaw \
  WAIT_FOR_COMPLETION=true \
  benchmarks/scripts/run-benchmark.sh thingsflow benchmarks/scenarios/mqtt-1000.env

```

Evidence collection for the current platform is built into the ramp itself
(`benchmarks/scripts/fair-ramp.sh`): per-level verdicts, kernel-measured CPU and
memory, CFS throttling checks and consumer lag. The old standalone collector was
QuestDB-specific and has been removed along with that backend.

Summarize result files:

```bash
python3 benchmarks/scripts/summarize-results.py benchmarks/results
```

Sanitized pilot summaries:

- [RESULTS_2026_05_19_RMQTT.md](RESULTS_2026_05_19_RMQTT.md): native RMQTT
  aggressive ladder.

## Interpreting Results

ThingsFlow should be evaluated on the architectural boundary it draws:

**Flow Core manages the platform; ThingsFlow data plane moves device data; ThingsBoard UI is optional.**

Useful benchmark conclusions should therefore separate:

- control-plane cost: API, provisioning, auth, topology, dashboards;
- data-plane cost: MQTT/HTTP ingestion, event streaming, materialization;
- compatibility cost: UI dashboards, latest-value reads, WebSocket updates;
- operational cost: number of pods, logs, restarts, and storage pressure.

Results belong in `benchmarks/results/` or an external evidence store. Commit
methodology and scenario definitions; commit raw results only when they are
clearly labelled with cluster, date, versions, and resource limits.
