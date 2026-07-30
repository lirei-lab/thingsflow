# ADR 0001 — Edge store-and-forward with idempotent backfill

- **Status:** Proposed
- **Date:** 2026-06-16
- **Context owners:** edge gateway (`gate-edge-energy`), data plane (`flow-core` / GreptimeDB)
- **Supersedes / relates to:** edge telemetry path (see `docs/EDGE_GATEWAY.md`, `docs/DATA_PLANE.md`)

## Context

The Fusion Energy SEM produces high-rate raw telemetry (~every 2s, ~20 circuits).
Shipping it raw upstream would multiply bandwidth and force re-deriving aggregates
in the cloud. The edge therefore **reduces at the source**: `sem-transformer`
buffers raw readings and emits one decimated event per circuit per
`PUBLISH_INTERVAL_SECONDS` (10s), aggregating **mean** for instantaneous gauges
(`voltage`, `current`, `active_power`) and **max** for monotonic energy counters
(`energy_in_kwh`, `energy_out_kwh`). This edge reduction is correct and is kept.

The edge runs on networks we do not control (e.g. a Raspberry Pi behind a
campus/guest router) and reaches the cloud over Tailscale. Connectivity loss is a
real, observed event — not hypothetical. When the cloud is unreachable, the live
path drops telemetry: Telegraf's HTTP output retries only from an **in-memory**
buffer (`metric_buffer_limit = 10000`, ~80 min at the reduced rate) and loses
everything on a long outage or a process restart. OSS Telegraf has no durable
on-disk output queue.

A local InfluxDB already receives a copy of the upstream-bound data. Its intended
role is a **durable backup journal** so missing data can be re-synced to the cloud
after a disconnect. Storing a copy is not the same as re-syncing it: today the two
Telegraf outputs (cloud HTTP, local InfluxDB) are independent fire-and-forget, and
nothing replays InfluxDB into the cloud. There is no reconciliation.

Constraint from the edge owner: the **live path must stay cheap** on the Pi.
Routing every live message through an InfluxDB read (InfluxDB-as-queue) is rejected
— reading from InfluxDB in steady state is too costly locally.

## Decision

1. **Keep the live path direct and unchanged.** `telegraf → forwarder → cloud`
   for live delivery, plus `telegraf → InfluxDB` as a parallel durable append.
   No InfluxDB reads occur in steady state.

2. **InfluxDB local is the edge backup journal**, sized and retained to a declared
   **outage budget** (see Consequences). It is the source for backfill only.

3. **Backfill runs only on reconnect**, inside the existing `thingsflow-forwarder`
   (no new container). The forwarder already knows when the cloud is down (it
   returns 503 while upstream fails) and when it recovers.

4. **Gap detection is stateless on the edge, via the cloud's `max(ts)`.** On
   recovery the backfill asks the cloud for the latest stored timestamp for the
   device, reads from InfluxDB only the rows newer than that (`ts > max_ts − margin`,
   a small safety overlap), and replays them. The cloud's latest timestamp *is* the
   watermark — no mutable edge-side watermark to corrupt on a crash.

5. **Replay is idempotent and preserves the original timestamp.** The backfill must
   re-send each point with its original event timestamp, never re-stamp at replay
   time.

Backfill cost is proportional to outage size and occurs only during recovery: one
`max(ts)` query to the cloud, one bounded InfluxDB read of the gap, N idempotent
POSTs. Steady-state cost is identical to today.

## Why this is safe — idempotency is verified

The cloud telemetry store `device_telemetry_kv` (GreptimeDB) is idempotent on the
replay key. Verified schema (2026-06-16):

```
PRIMARY KEY (tenant_id, device_id, telemetry_key)
TIME INDEX  (greptime_timestamp)
merge_mode = 'last_non_null'
ttl ≈ 90d
```

Row identity = `(tenant_id, device_id, telemetry_key, greptime_timestamp)`. The
circuit is encoded into `telemetry_key` (e.g. `refrigerator_active_power`), so the
dedup granularity is exactly `(device, circuit-metric, timestamp)`. Re-inserting
the same point **upserts** (`last_non_null`) rather than appending — no duplicates,
no double-counting of energy — **provided the original timestamp is preserved on
replay** (decision #5). InfluxDB stores the original timestamp (Telegraf
`timestamp_path = "timestamp"`), so a backfill that reads from InfluxDB satisfies
this for free.

## Consequences

**Positive**
- InfluxDB local gets a single, governed purpose (backup journal) with an owner —
  no longer a "phantom" second copy. Aligns with the project charter: every store
  bounded, single source of truth, verified.
- Live and backfill use the **same idempotent ingest path** — one delivery logic,
  reconcilable and auditable.
- Edge stays cheap in steady state; the in-memory Telegraf buffer is no longer
  relied on as the safety net (it isn't one).

**Negative / costs**
- The forwarder gains backfill logic (custom code at the edge). Kept minimal and
  confined to the one sanctioned edge component.
- Requires a cloud endpoint (or query) to obtain the device's latest stored `ts`.

**Requirements / follow-ups**
- **Outage budget = local retention.** Pick and document a number (recommended:
  **7 days**); set InfluxDB TTL to it and size the disk for `budget × reduced rate`.
  Data lost beyond the budget is unrecoverable by design.
- Backfill must replay in time order, in bounded batches, with backpressure so a
  long gap does not overwhelm the cloud on reconnect.
- Add end-to-end pipeline observability: messages produced vs delivered vs stored,
  and a backfilled-rows counter on recovery.

## Open / unresolved

- **Device identity provenance.** During the 2026-06-15→16 outage (Pi forwarder
  crash-looping, unable to reach the cloud) the cloud still received a reduced
  stream (~44 publishes/h, 25 keys) attributed to device
  `33ae7128-0792-4a23-847f-4d9cb4e326d4`, while the full SEM stream (~320/h, 100
  keys) only resumed after the Tailscale repoint. This suggests **another
  source/gateway is using the same device identity**. Shared device identity makes
  data provenance and backfill watermarks ambiguous and must be resolved before
  relying on `max(ts)` per device. Investigate and, if confirmed, give each edge a
  distinct device identity.
