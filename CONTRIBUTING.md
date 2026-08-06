# Contributing to ThingsFlow

Thanks for considering a contribution. This document explains how the repo is
wired so your change passes the gates on the first try.

## Ground rules

- **No custom code in the telemetry hot path.** Device telemetry flows through
  RMQTT/Envoy → Bento → NATS → GreptimeDB. Flow Core (Go) reads stores; it does
  not sit between a device and the database. Features that need per-message
  processing belong in Bento/NATS configuration, not in new Go code.
- **Retention is declarative.** Every store that accumulates data must have a
  TTL/retention policy expressed in the chart (SQL, DB-native TTL, or a
  CronJob), and a guard that verifies it is actually applied. A store that can
  grow without bound is a bug even if nothing is broken today.
- **Fail loudly.** The project's recurring incident class is silent failure —
  empty 200s, skipped tests, guards that stay green while the thing they watch
  is dead. Prefer a visible error over a forgiving default, and when you add a
  guard, make sure silence is impossible: it must emit on failure, not only on
  success.

## Repository layout

```text
flow-core/            Go control plane + ThingsBoard-compatible API
docker/               Local compose stack (full platform)
k8s/helm/thingsflow/  Helm chart (the deployment source of truth)
sdk/python/           Device SDK
tools/python/         Contract tests for the chart and platform
docs/                 Architecture, security, operations
```

## Building and testing

Go 1.23. From `flow-core/`:

```bash
gofmt -l .          # must print nothing (CI fails otherwise)
go vet ./...
go build ./...
go test -p 1 -count=1 ./...
```

`-p 1` is required, not a preference: DB-backed packages drop and recreate the
same tables against one database, so parallel packages destroy each other's
fixtures. The suite is serial by design.

DB-backed tests skip without a database. To run them for real:

```bash
docker run -d --name pgtest -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
FLOW_TEST_PG_DSN='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
  go test -p 1 -count=1 ./...
```

CI treats a skipped DB test as a failure — if you see `FLOW_TEST_PG_DSN not
set` in CI output, the wiring is broken, not the test.

Python contract tests (chart/platform invariants):

```bash
python3 -m unittest discover -s tools/python -p 'test_*.py' -v
```

## Routing rules (flow-core/api.go)

Routes are Go 1.22+ method+wildcard patterns (`GET /api/device/{id}`)
registered in `registerRoutes`. Two things will bite you:

- **Overlapping patterns panic at startup.** A methodless literal next to a
  method wildcard (`/api/x/types` vs `GET /api/x/{id}`) is a registration
  conflict — the pod never becomes ready. `TestRegisterRoutes_NoPatternConflicts`
  builds the mux in a test so this is a failing build instead of an outage.
  Where two wildcard shapes overlap (`info/{id}` vs `{id}/customers`), use one
  `{a}/{b}` pattern with a small dispatch — see the existing examples.
- **The UI contract is enforced.** `internal/uicontract/testdata/tb-ui-*.json`
  declares every endpoint the ThingsBoard UI calls as `routed` or `no-goal`.
  New endpoints must land in the contract; undeclared paths answer 404 by
  design. Verify against a running instance with:

  ```bash
  # :8082 if running against the compose stack
  go run ./cmd/ui-contract-check -base-url http://localhost:8080 \
    -contract internal/uicontract/testdata/tb-ui-4.3.1.1.json
  ```

## Conventions

- Errors through `httputil.WriteError`, success through `httputil.WriteJSON` —
  never hand-rolled error JSON (the UI interceptor depends on the envelope).
- Tenant isolation is enforced explicitly in every mutating handler.
- One concern per file; packages under `flow-core/internal/<domain>/`.
- Comments explain *why* (TB-compatibility decisions, constraints), not what.
- Commit messages follow conventional-commit style:
  `fix(mqtt): …`, `feat(alarms): …`, `security(seed): …`, `docs: …`.

## CI gates

| Workflow | Gate |
|---|---|
| `test-flow-core.yml` | gofmt, go vet, build, full test suite against real Postgres, plus QuestDB for the optional-backend branches (skips are failures; GreptimeDB paths are covered by the compose smoke and the contract gate) |
| `lint-migrations.yml` | migration hygiene: every migration applies, rolls back to baseline, and re-applies against a real Postgres seeded with the chart's baseline SQL |
| `oss-release-gate.yml` | runs on every PR: SDK packaging gate (clean-venv install of `sdk/python` plus the `[mqtt]` extra; runtime `__version__` must match `pyproject.toml`); `bash tools/check-oss-release.sh` (required public files present, no tracked private/runtime files, private hostname/IP grep, no `:latest` chart images, `helm lint` + `helm template` of the chart and all 4 tracked values overlays); full Python contract-test discovery over `tools/python` — including the repo-hygiene 500KB tracked-file ceiling (`test_repo_hygiene.py`) and the fresh-install contract pinning INSTALL.md's commands to the smoke workflow; `go vet` + `go test` for flow-core |
| `fresh-install-smoke.yml` | compose E2E, path-filtered (`docker/`, `flow-core/`, chart `files/`, `tools/smoke/`): builds the full stack from scratch with `docker compose ... up -d --build` and proves demo login, device creation, device JWT issuance, telemetry through both real edges (Envoy HTTP ingest and RMQTT), and rows readable back via the platform API |
| `docs.yml` | `mkdocs build --strict` on PRs touching `docs/`; deploys GitHub Pages on push to main/develop |
| `chart-publish.yml` | `helm lint` + `helm template`, then publishes the OCI chart to ghcr.io on pushes touching the chart (no-op if that chart version is already published) |
| `docker-publish.yml` | builds and pushes the flow-core image to ghcr.io on every branch push and `v*` tag (`:latest` moves only on the default branch and releases) |

## Reporting security issues

Do not open a public issue — see [SECURITY.md](SECURITY.md).
