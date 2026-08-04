# Roadmap

What ThingsFlow supports today, and which backends are planned next. Anything not
listed as available should be treated as unimplemented, not as a soft commitment.

## Available today

| Concern | Implementation | Status |
|---|---|---|
| Time-series history | **GreptimeDB** | Default. Written over HTTP InfluxDB Line Protocol, read over the Postgres wire protocol. |
| Time-series history | **QuestDB** | Optional, implemented. Set `timeseries.store=questdb`. Kept working where already configured, but GreptimeDB is where new work goes. |
| Event bus / replay | **NATS JetStream** | The only bus. Carries accepted raw telemetry from the MQTT and HTTP edges. |
| Latest / twin state | **NATS KV** | Authoritative hot-state store. See its measured limit in `benchmarks/FINDING-twin-state.md`. |
| Operational state | **PostgreSQL 16** | Users, devices, credentials, topology, alarm lifecycle, audit. Not a telemetry sink. |
| Scheduled work | **Kubernetes CronJob** | Retention sweeps, freshness and TTL guards, backup, device-silence. No orchestrator, deliberately -- see below. |

## Planned

These are ordered by how much new code they need, which is not the same as by
priority. The point of stating it this way is that most of the work is
configuration and validation rather than new integrations written from scratch:
the components already in the data plane speak these protocols.

### Kafka and Redpanda as an alternative event bus

The processor layer (Bento) already ships `kafka` and `kafka_franz` outputs, and
the MQTT edge (RMQTT) already ships `rmqtt-bridge-egress-kafka`. The work is
therefore not "add Kafka support" but:

- deciding what the durability and replay contract is when the bus is Kafka
  rather than JetStream, since consumer-group semantics differ from durable
  push consumers with `AckWait`;
- porting the retention guards, which today assert stream `max_age`/`max_bytes`
  and consumer lag against JetStream specifically;
- reproducing the twin-state and history materializers against topics instead
  of subjects.

Redpanda is Kafka-API compatible, so it is expected to fall out of the same
work rather than being a separate integration.

### TimescaleDB as a time-series backend

Bento already has a `sql_insert` output and PostgreSQL is already deployed for
operational state, so the write path is short. The open questions are the
schema (hypertable layout, chunk interval, compression policy) and how
retention is declared, because the project's rule is that **every store must
have a retention policy that is declarative and verified** — a Timescale backend
needs its own guard, not just a working insert.

### InfluxDB as a time-series backend

Closer than it looks: the ingest path already emits InfluxDB Line Protocol,
which is how it writes to GreptimeDB today. What is missing is the read side —
the query translation that `flow-core/internal/telemetry/reader.go` implements
per backend — plus retention expressed as InfluxDB retention policies and a
guard that verifies they are applied.

### Machine learning over time series and twins

Two workloads get conflated here, and they want different tools.

**Online scoring, per message.** This already has a home and needs no new
infrastructure: a NATS consumer reads `tf.ingest.>`, scores, and writes back a
derived subject or a twin key. It is the same shape as the existing alarm
detector. `docs/DATA_PLANE.md` and `docs/OPERATIONS.md` already name NATS
consumers and GreptimeDB reads as the attachment point. What is missing is not a
runtime but conventions: where model artifacts live, how a model version is
pinned to a deployment, and how a scoring consumer declares its input contract.

**Batch and scheduled work** — training, backfills, feature aggregation over
history, periodic twin recomputation. This is what an orchestrator is for, and
today the platform has none: scheduled work runs as five plain Kubernetes
CronJobs (retention sweeps, freshness and TTL guards, backup, device-silence).

### Choosing an orchestrator: the criterion first

CronJob is the incumbent and it is not a placeholder — it is declarative, has no
control plane to operate, and already carries the retention guards. An
orchestrator earns its place only once the workload needs something CronJob
genuinely cannot express:

- dependencies between jobs, so a feature build waits on an ingest window;
- backfills over a historical range with per-partition retries;
- run state and lineage, so a failed training run can be resumed and audited;
- dynamic fan-out, where the number of tasks depends on the data.

Until at least two of those are real, adding an orchestrator adds a control
plane to operate for no gain. That matters here specifically: this project's
stated direction is to minimise custom runtime services, and every component in
the hot path was chosen to be replaceable and standard.

### Candidates under review

**Prefect** is the current front-runner and the reason is fit, not popularity:
it is Python-native, which matches the device SDK and edge tooling already in
Python, and it is lighter to operate than Airflow. Two things must be verified
before committing, and neither is a detail:

- **How much works self-hosted.** This platform is self-hostable by design.
  Some orchestration products keep automations, RBAC or event triggers as
  hosted-only features. Whatever is adopted has to be fully operable without a
  vendor account, and that has to be checked against the current release rather
  than assumed.
- **What it adds operationally.** An orchestrator needs its own database and
  its own upgrade path. Postgres is already deployed, so that part is cheap, but
  the control plane is not free.

**Dagster** deserves a serious look for a reason specific to this platform: it
models *data assets* rather than tasks, and twin state and telemetry history are
exactly that. Asset lineage would answer "which model version produced this twin
feature" natively instead of by convention.

**Argo Workflows** stays Kubernetes-native with no extra Python control plane,
which fits the deployment model, at the cost of a worse experience for the
people who would actually write the pipelines.

The choice is not made. What is decided is the boundary: **whatever runs, runs
off the hot path**. Nothing scheduled may sit between a device publish and its
landing in the store, and no ML component may become a dependency of ingest.

## What "planned" does and does not mean here

It means the design admits the backend and the components already speak the
protocol. It does not mean a date, and it does not mean the work is small: for
every store, the hard part is not writing rows, it is proving that retention is
applied and that nothing grows unbounded. That requirement is the reason this
project exists, and a new backend is not integrated until it has a guard like
the ones the existing stores have.

## Backend selection is not neutral

Whichever store is configured changes the write path and therefore the
performance profile. Benchmarks published from this repository name the backend
they measured, and a result obtained with one backend does not carry over to
another. See `benchmarks/METHODOLOGY.md`.
