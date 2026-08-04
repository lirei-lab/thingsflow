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
