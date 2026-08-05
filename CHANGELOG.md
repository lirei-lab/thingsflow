# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.0] - 2026-08-04

### Fixed

- **The freshness guard alerted on an idle platform, not just a halted one.**
  It fired whenever the telemetry table held rows but none were recent. That is
  the signature of a halted writer, but equally of a platform where nobody is
  publishing: an evaluation cluster after a load test, a pilot overnight, a fleet
  between duty cycles. On an idle test cluster it produced a failed Job every
  five minutes indefinitely — the same failure mode it used to have on every
  fresh install, and a guard that always alerts is one operators learn to ignore.

  The discriminator is whether work is waiting. The incident this guard exists
  for — a dead NATS subscription while every pod stayed Running and the edge kept
  returning 200 — leaves messages piling up on the history consumer with nothing
  draining them; an idle platform has an empty backlog because nothing was
  published. An init container now reads that backlog (`num_pending` plus
  `num_ack_pending`, because a consumer that dies mid-flight leaves messages in
  the second bucket) and the guard alerts only when work is stuck.

  It runs as an init container because the psql image has no HTTP client at all —
  no curl, no wget, no python, checked rather than assumed — while `nats-box` is
  already pinned by this chart and carries both the CLI and `jq`. The probe never
  exits non-zero: an init container failure fails the Job, and a failed Job *is*
  this guard's alert surface, so a transient NATS blip would otherwise be
  indistinguishable from a halted data plane. An unreadable backlog **alerts**,
  deliberately — not being able to tell is not evidence of health.

  Verified in cluster on all three branches: 3,240,000 historical rows with an
  empty backlog exits 0; the history writer scaled to zero with 4,000 messages
  accepted and waiting exits 1 and names the count; an unreadable probe exits 1.
  Restoring the writer drained the backlog to 0 and landed exactly the 12,000
  rows owed, with no intervention.

- **The default CPU limits throttled the data plane at the platform's own
  target rate.** Every NATS consumer shipped capped at `750m` and NATS itself at
  `500m`. Under a plain 3,000 msg/s MQTT run (500 devices, one replica each) the
  throttling gate measured `nats-greptimedb` and `nats-alarms` pinned to their
  ceiling on 99.9% and 99.5% of CFS periods, `nats-latest-kv` on 95.5%, and NATS
  on 66% — and the run landed **396,291 of 540,000 expected rows**.

  What makes it worth calling a defect rather than a tuning choice is that
  nothing reports it. The edge returns success, all 180,000 messages are
  accepted, `0 failed`, and the shortfall appears only as consumer lag. An
  operator watching acknowledgements sees a healthy platform losing data.

  Caps raised to `2000m` (NATS, history, alarms) and `3000m` (latest-values
  writer, the most expensive consumer). **Requests are deliberately unchanged**:
  CFS throttling is a function of the limit while scheduling is a function of
  the request, so raising the ceiling costs nothing in schedulability and a
  default install still fits a modest node. Verified from the shipped defaults
  with no overlay: the identical run lands 540,000 of 540,000, p95 latency falls
  from 7.84 ms to 2.02 ms, and 24 of 25 containers show no throttling at all.

  `2000m` was tried first for the latest-values writer and is **not** enough: it
  still throttled 15.1% of periods against a 1,632m mean draw, because the mean
  hides bursts that exceed the cap inside a single 100 ms period. Clearing the
  average is not the same as clearing the limit.

  This does **not** revise `benchmarks/FINDING-twin-state.md`. That measurement
  ran with 2,000m per pod across three replicas and zero throttling, and its
  ~3,900 msg/s congestion-collapse ceiling stands; the two numbers scale
  consistently per pod. The defect here is that the *shipped defaults* hit a
  resource wall well before the platform reaches that architectural limit.
  `tools/python/test_data_plane_cpu_headroom.py` pins the measured floors.
- **Spurious `401` rejections to legitimate devices on HTTP ingest.** Envoy
  refreshed the device JWKS lazily and blockingly, so the first request after
  the 300 s cache expiry was rejected even with a valid token. Measured at
  1–2 rejections per ~500,000 requests across three independent runs, always
  with zero data loss. Fixed with `async_fetch` (pre-fetch before expiry);
  verified over 4,8 million requests with zero occurrences.
- **Retention and freshness guards could be silently disabled forever.** The
  CronJob templates did not declare `suspend`, so a guard stopped by hand with
  `kubectl` stayed stopped through every subsequent `helm upgrade`, invisible to
  the deployment. Found with one guard 47 h dormant while the chart reported it
  enabled. All five CronJob templates now declare `suspend: false` explicitly.
  Note: recovering an already-suspended guard needs `helm upgrade
  --force-conflicts` once, because `kubectl patch` takes ownership of the field
  and server-side apply refuses to change it.

### Added

- **RMQTT cluster mode (3-node raft).** The plugin shipped in the image but was
  never configured; the chart hard-failed on `replicas > 1`. Runs as a
  StatefulSet with stable per-pod identity. `leaderId` defaults to `1`, not `0`:
  leader election on a parallel cold start splits the cluster into a stable
  three-way disagreement that does not self-heal.

### Changed

- **`docs/SECURITY.md` — the MQTT ACL is a refusal, not a containment boundary.**
  The document claimed a device "cannot address another device's topic at all".
  Reproduced in cluster: the broker does refuse the publish (no PUBACK,
  connection dropped) but the message reaches the internal stream first. The
  boundary that actually holds is the materializer, which derives `device_id`
  from the JWT claim — verified: a cross-device publish landed under the
  publisher's own identity, never the impersonated one.

### Known limitations

- **The latest-value (twin state) writer saturates around 3,900 msg/s and then
  degrades**, while the history path sustains ≥16,000 msg/s with zero loss over
  the same stream. Above that rate no data is lost — history stays complete and
  correct — but the "current value" a dashboard reads falls behind. It fails
  silently: no errors, no gaps in charts. Cause not yet identified; consumer CPU,
  quota, replica count and NATS saturation have all been ruled out. See
  `benchmarks/FINDING-twin-state.md`. **Size deployments on this number, not on
  the ingest number.**

## [2.1.0] - 2026-07-30

First public release of ThingsFlow.

### Added

- **ThingsBoard UI contract coverage**: all 373 endpoints used by the bundled
  ThingsBoard UI (v4.3.1.1) verified against Flow Core with zero gaps, and a
  runtime contract check (`cmd/ui-contract-check`) that enforces the contract
  against a running instance so regressions cannot land silently.
- **Declarative retention with guards across every store**: Postgres sweeps run
  as k8s CronJobs (plain psql, no extensions), GreptimeDB uses DB-native TTL,
  and NATS JetStream streams are bounded by `max_age`/`max_bytes`. Each policy
  ships with a guard that fails visibly when the policy is absent or drifts —
  no store can grow unbounded.
- **GreptimeDB S3 backup CronJob**: scheduled telemetry-history backups to
  S3-compatible object storage, managed by the Helm chart.
- **Per-install JWT seed key**: each installation generates its own JWT signing
  seed instead of shipping a shared default.
- **Helm chart published as an OCI artifact** (`ghcr.io/lirei-lab`), version
  `2.1.0`.
- **Documentation site** (MkDocs Material) with mermaid architecture diagrams,
  covering architecture, data plane, security, operations, device SDK, and the
  UI compatibility contract.

### Changed

- **NATS subjects renamed `zt.*` → `tf.*`** across the platform, with a
  live-stream migration path so existing deployments move without losing
  in-flight telemetry.

### Historical note

Everything before 2.1.0 is pre-release history: internal iterations of the
control plane, data plane, and chart that were never published as supported
versions.

[2.1.0]: https://github.com/lirei-lab/thingsflow/releases/tag/v2.1.0
