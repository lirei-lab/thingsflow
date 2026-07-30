# ThingsFlow Documentation

This directory contains the public ThingsFlow documentation site. The generated
site starts at [index.md](index.md), uses the navigation in `mkdocs.yml` (co-located in this directory), and is
published by `.github/workflows/docs.yml`.

The docs follow a Mainflux-style information architecture: overview first,
then getting started, architecture, messaging/data plane, provisioning/API,
security, operations, and release gates. The content should stay practical and
modular: one concept per page, cross-links instead of duplicated prose, and
explicit boundaries between control plane, data plane, storage, and security.

Treat the pages listed below, plus `README.md`, as the current product
documentation source of truth. AI coding-agent artifacts (agent instructions,
plans, session state) are local tooling and are git-ignored, so they never reach
the published site or a clone.

## Reader Paths

| Need | Start With | Then Read |
|---|---|---|
| Understand the platform | [index.md](index.md) | [ARCHITECTURE.md](ARCHITECTURE.md), [RELEASE.md](RELEASE.md) |
| Run it locally or in Kubernetes | [GETTING_STARTED.md](GETTING_STARTED.md) | [INSTALL.md](INSTALL.md), [DEMO_PROFILE.md](DEMO_PROFILE.md) |
| Integrate devices | [API_REFERENCE.md](API_REFERENCE.md) | [DATA_PLANE.md](DATA_PLANE.md), [SECURITY.md](SECURITY.md) |
| Understand telemetry flow | [DATA_PLANE.md](DATA_PLANE.md) | [DIGITAL_TWIN.md](DIGITAL_TWIN.md), [OPERATIONS.md](OPERATIONS.md) |
| Build against APIs | [API_REFERENCE.md](API_REFERENCE.md) | [DIGITAL_TWIN.md](DIGITAL_TWIN.md) |
| Prepare production pilots | [SECURITY.md](SECURITY.md) | [OPERATIONS.md](OPERATIONS.md), [RELEASE.md](RELEASE.md) |

## Page Inventory

### Overview And Start

- [index.md](index.md): public landing page and documentation map.
- [GETTING_STARTED.md](GETTING_STARTED.md): shortest path to a working stack.
- [DEMO_PROFILE.md](DEMO_PROFILE.md): seeded dashboards, demo devices, and
  simulator.
- [INSTALL.md](INSTALL.md): full install and operations guide.

### Architecture And Concepts

- [ARCHITECTURE.md](ARCHITECTURE.md): platform map, runtime boundaries, and
  storage ownership.
- [DIGITAL_TWIN.md](DIGITAL_TWIN.md): Things, attributes, features,
  topology, and NATS KV latest state.

### Messaging And Data Plane

- [DATA_PLANE.md](DATA_PLANE.md): MQTT/HTTP ingress, NATS event contract,
  Bento materializers, latest KV, GreptimeDB history, optional QuestDB, alarms,
  replay, DLQ, lag, logging, and retention expectations.
- [EDGE_GATEWAY.md](EDGE_GATEWAY.md): field gateway against a remote endpoint
  and the multi-circuit metering pattern.

### API, Provisioning, Security, And Operations

- [API_REFERENCE.md](API_REFERENCE.md): OpenAPI contract, generation strategy,
  API-first control plane, provisioning, OIDC broker, and UI compatibility
  contract.
- [DEVICE_SDK.md](DEVICE_SDK.md): Python device SDK for provisioning and
  telemetry.
- [MQTT_DEVICE_AUTH.md](MQTT_DEVICE_AUTH.md): device-side MQTT contract —
  Device JWTs, TLS, topics, ACLs, refresh.
- [UI_CONTRACT_COVERAGE.md](UI_CONTRACT_COVERAGE.md): measured ThingsBoard UI
  compatibility (373 endpoints) and its runtime enforcement.
- [SECURITY.md](SECURITY.md): threat model, controls, and production checklist.
- [OPERATIONS.md](OPERATIONS.md): public access, logging, observability,
  backup/restore, controlled pilot runbook, resource planning, benchmark
  methodology, and pilot acceptance checks.

### Release

- [RELEASE.md](RELEASE.md): product identity, public artifact boundary,
  publication checklist, and controlled industrial pilot gates.

### Decisions

- [adr/](adr/): architecture decision records.

(The canonical page list is the `nav` section of the repo-root `mkdocs.yml`;
keep this inventory in sync with it.)
