# ThingsFlow RMQTT Aggressive Benchmark - 2026-05-19

> **Historical record (2026-05-19).** These runs were measured on the
> pre-rename platform: NATS subjects were then `zt.*` and the history store
> was QuestDB. The numbers stand as measured. The current platform uses
> `tf.*` subjects and GreptimeDB as the history store.

This is a sanitized pilot-cluster summary. Raw job logs and cluster evidence
stay under `benchmarks/results/` and are intentionally ignored by git because
they may include operational details.

## Test Conditions

| Field | Value |
|---|---|
| Target | ThingsFlow native MQTT path |
| Path | Device JWT -> RMQTT auth-jwt/ACL -> NATS -> Bento materializers |
| Kubernetes | v1.29.15 |
| Cluster shape | 4 worker nodes |
| RMQTT | 0.20.0, single broker pod |
| NATS subject | `zt.ingest.mqtt.raw.events` |
| Latest materializer | Bento to NATS KV |
| History materializer | Bento to QuestDB |
| Demo simulator | disabled during all runs |
| Auth mode | `deviceJwtRaw` |

## Results

| Run | Devices | Shards | Duration | Expected publishes | Observed publishes | Loadgen error lines | Runtime restarts |
|---|---:|---:|---:|---:|---:|---:|---:|
| `rmqtt1k-20260519` | 1,000 | 8 | 60 s | 60,000 | 60,000 | 0 | 0 |
| `rmqtt2k-20260519` | 2,000 | 16 | 60 s | 120,000 | 120,000 | 0 | 0 |
| `rmqtt5k-20260519` | 5,000 | 25 | 60 s | 300,000 | 299,998 | 0 | 0 |

## Observed Bottlenecks

| Run | RMQTT CPU after run | Latest freshness at first snapshot | Latest freshness after cool-down | History state |
|---|---:|---:|---:|---:|
| 1k | ~38m | 0 | 0 | 0 |
| 2k | ~221m during capture, then ~38m | 36,198 | 0 | 0 |
| 5k | ~38m after run | 84,459 | 0 | 0 |

The broker did not look like the limiting component. The architectural lesson
from this class of tests is that latest hot state must stay out of the
Postgres write fan-out. The current NATS-first profile measures NATS KV
freshness and QuestDB history independently.

## Storage Visibility

| Run | Latest visibility | QuestDB history visibility |
|---|---:|---:|
| 1k | 1,000 devices, 4,400 latest rows | history query collected in raw evidence |
| 2k | 2,000 devices, 8,800 latest rows | history query collected in raw evidence |
| 5k | 4,998 devices, 21,992 latest rows at first snapshot | 5,000 devices, 214,579 rows at first snapshot |

The 5k latest snapshot was taken while latest lag was still draining, which
explains the lower first-read latest device count. After cool-down, the
latest-state materialization caught up after cool-down.

## RMQTT Multi-Node Decision

Do not configure RMQTT multi-node yet for throughput. Current evidence says a
single broker pod handled the stress gate with low CPU and memory. RMQTT
multi-node should be introduced when availability policy requires broker HA, or
when broker metrics show RMQTT itself is the first saturated component.

Important: RMQTT clustering is not `replicas > 1`. Real clustering requires
`rmqtt-cluster-raft`, unique node IDs, gRPC node addresses, and Raft peer
addresses. The Helm chart now fails fast if `rmqttEdge.replicas > 1` is set
without a future explicit cluster profile.

## Operational Finding

During the stress run, RMQTT at `info` logged session-close lines containing
the MQTT username. Because ThingsFlow uses the compact device JWT as the username,
that can leak bearer material into broker logs. The public chart now defaults
RMQTT to `warn`, and the benchmark evidence collector redacts JWT-like strings
from captured log samples.

## Next Optimization Target

The next aggressive benchmark should focus on latest-value write throughput:

- measure Postgres write latency and lock/IO pressure during 5k+ runs;
- test 8 latest materializer replicas with the same 5k/10k ladder;
- consider batching or a connector-driven latest sink if Postgres latest lag
  becomes the pilot bottleneck;
- keep RMQTT single-node until broker evidence says otherwise.
