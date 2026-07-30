# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- **Helm chart published as an OCI artifact** (`ghcr.io/lirei-uqtr`), version
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

[2.1.0]: https://github.com/lirei-uqtr/thingsflow/releases/tag/v2.1.0
