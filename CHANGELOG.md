# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.0] - 2026-08-04

### Fixed

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
