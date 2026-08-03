# Scenario matrix

ThingsFlow measurement scenarios and what is observed in each one. The validity rule and
the method defects are in [METHODOLOGY.md](METHODOLOGY.md).

## Subject

| Target | Stack |
|---|---|
| `thingsflow` | RMQTT (3-node raft cluster), HTTP ingest (Envoy + Bento), NATS JetStream, Bento materializers, GreptimeDB, Postgres, Flow Core |

ThingsFlow is measured as event-oriented middleware: Flow Core is the control plane and
the data plane is the telemetry path. Flow Core is **not on the hot path**, and that is
why its single replica does not bound the throughput.

## Capacity ramp

The main scenario. Increasing load is offered until a level stops being valid.

| Parameter | Value |
|---|---|
| Rates | 1,000 → 2,000 → 4,000 → 8,000 → 16,000 msg/s |
| Devices | 2,000 |
| Duration per level | 180 s of steady load + 45 s of discarded warm-up |
| Payload | 3 telemetry keys per message |
| Protocols | MQTT and HTTP, measured separately |
| Settling | 30 s before counting landed rows |

Between levels the **streams and the KV bucket are drained**: without that, one level's
backlog is charged to the next and the measurement measures the previous experiment.

The duration is not arbitrary: 180 s plus the warm-up exceed Envoy's 300 s JWKS cache, so
the ramp crosses at least one refresh. A 60 s run would pass green without exercising that
path.

## Footprint scenarios

| Scenario | Devices | Purpose |
|---|---:|---|
| `idle` | 0 | Idle cost of the complete platform |
| `light` | 50 | Footprint under sustained low load |

They measure consumption, not capacity. The distinction matters: without landing
verification, a high-load scenario **is not a capacity claim** (see
[results-instrumented/SCOPE.md](results-instrumented/SCOPE.md)).

## What is observed

| Metric | How |
|---|---|
| Accepted and errors by type | from the generator |
| **Rows landed in the store** | `count(*)` with the run prefix; must match exactly |
| **Consumer lag** | pending messages per consumer at the level's close |
| Client latency | p50 / p95 / p99 |
| CPU and memory per container | read from the kernel (cgroup v2), not sampled |
| **CFS throttling** | throttled periods per container during the window |
| The generator's own CPU | to detect when the client is the limit |

The three in bold are what distinguish this matrix from one that only counts
acknowledgements. Each one was born from a real failure that the others did not detect:

- **Landed rows** — a 200 from ingest does not prove the data was stored.
- **Consumer lag** — verifying only the history lets a level pass as clean when another
  route of the same flow accumulates lag with no errors and no visible gaps.
- **Throttling** — a level throttled by its own CPU limit measures the cage.

## Publication

The publishable results go in `results-fair/`. Runs that contain operational details of
the cluster, or measurements of other platforms, stay out of the public repository — see
the notes in `.gitignore`.
