# Security policy

For the project's security architecture (threat model, trust
boundaries, controls inventory, production deployment checklist), see
**[docs/SECURITY.md](docs/SECURITY.md)**. This file covers the
vulnerability reporting policy only.

## Reporting a vulnerability

Use GitHub's "Report a vulnerability" button on this repository (see
`MAINTAINERS` for the reporting policy) with the details of the issue. Please do **not** open a public GitHub
issue for suspected security problems.

We aim to respond within 5 business days with an acknowledgement and
either a fix timeline or a request for clarification. Please do not
disclose publicly until either a release with the fix is shipped or
90 days have passed without progress.

## Scope

This policy covers **Flow Core (`flow-core/`), the Helm chart
(`k8s/helm/thingsflow/`), the dev docker-compose stack (`docker/`), and the
NATS/Bento/edge configuration shipped in this repository**. Issues in upstream
projects we consume — PostgreSQL, GreptimeDB, NATS, Bento, Envoy, RMQTT (and QuestDB when enabled), and the
ThingsBoard web UI binary — should be reported directly to those projects.

The deployment posture documented in
[docs/SECURITY.md](docs/SECURITY.md) is the reference for
"how this is supposed to be configured in production". Issues that
require a configuration not matching that posture are out of scope
unless they identify an inherently unsafe default in the chart.

## What's in scope

- Auth bypass (login, JWT validation, MQTT auth/ACL, X.509 fingerprint).
- Cross-tenant data leakage (Flow Core enforces tenant on every
  handler — a request with tenant A's JWT must never see tenant B's
  rows).
- Injection (SQL, ILP, LDAP, command).
- DoS via the public HTTP surface (slowloris, body-size bypass, etc.).
- Secret exposure in logs or `/metrics`.
- Container escape from the Flow Core image.

## What's out of scope

- Default credentials in `values.yaml` or
  `docker/docker-compose-nats.yml`. They are documented as dev-only; the
  preflight check (`FLOW_ENV=production`) blocks them in production.
- Self-XSS in the upstream tb-web-ui binary.
- Network-level attacks the cluster operator is expected to mitigate
  (NetworkPolicy, mTLS at ingress, etc.).
- Rate-limit bypass when the chart is scaled beyond `replicas: 1`
  without a Postgres-backed throttle (known limitation: the login
  throttle is in-memory per replica, so horizontal scaling divides its
  effectiveness until a shared store backs it).
