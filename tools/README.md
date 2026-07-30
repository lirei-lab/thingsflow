# ThingsFlow tools

This directory contains public developer, release, and local verification tools
for ThingsFlow. Tools here must be generic, reproducible, and safe to publish.

## Layout

Python utilities and Python tests live under `tools/python/`. Shell scripts
and the Playwright visual smoke stay at the `tools/` root.

## Publishable tools

- `check-oss-release.sh`, `check-log-policy.sh`, `check-topology-consistency.sh`:
  static and API-level release guardrails.
- `generate-pilot-secrets.sh`:
  dry-run-first Kubernetes Secret manifest generator for controlled pilots.
- `smoke-local.sh`, `verify-local.sh`, `verify-*.sh`:
  local compose verification entrypoints.
- `verify-pilot-acceptance.sh`:
  portable Kubernetes acceptance gate for controlled pilot readiness.
- `verify-platform-health.sh`:
  lightweight Kubernetes readiness check for the deployed platform databases,
  NATS KV hot state, noauth Flow Core surfaces, recent logs, and resource
  snapshot. It does not run load generation.
- `python/telemetry_generator.py`, `python/bench-mqtt.py`, `visual-dashboard-smoke.js`:
  local data and UI smoke helpers.
- `python/extract-system-images.py`:
  reproducible extractor for ThingsBoard-compatible system widget images.
- `repair-topology-backfill.sh`:
  dry-run-first topology maintenance helper.
- `python/cleanup-devices-by-prefix.py`:
  dry-run-first device cleanup utility for generated test devices.

## Not publishable here

- Cluster-specific deployment scripts, kubeconfigs, private registry jobs, and
  pilot-only overlays.
- Generated artifacts such as `target/`, `__pycache__/`, local logs, or runtime
  data directories.
- Upstream ThingsBoard tool modules that are not part of the ThingsFlow runtime or
  release process.

Run `bash tools/check-oss-release.sh` before publishing.
