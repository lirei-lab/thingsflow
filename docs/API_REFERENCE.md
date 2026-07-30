# API Reference

ThingsFlow exposes two API surfaces:

- **Native public APIs** for platform automation, provisioning, device security,
  digital-twin reads, and telemetry reads.
- **ThingsBoard compatibility APIs** for the upstream UI, dashboards, widgets,
  aliases, relations, WebSocket subscriptions, and existing integrations.

The ThingsBoard UI is a supported console. It is not the core product boundary.
Custom portals, CLIs, provisioning services, and industrial automation tools
should use the public API contract first. Native device MQTT and HTTP telemetry
use the provisioning-issued `deviceJwt` with edge JWT guards while
TB-compatible clients keep the classic access-token path where explicitly
enabled.

## OpenAPI Contract

The initial public contract lives at
[`api/openapi.yaml`](api/openapi.yaml) (in the repo:
`docs/api/openapi.yaml`).

It currently covers:

- login and current-user lookup;
- device CRUD basics;
- device credential reads and rotation;
- device security status;
- self-provisioning with classic `ACCESS_TOKEN` plus native `deviceJwt`;
- device PEM for RMQTT and JWKS for Envoy `http-ingest` JWT validation;
- native HTTP telemetry through `http-ingest`, outside Flow Core's hot path;
- legacy HTTP telemetry compatibility ingress where enabled;
- telemetry read APIs;
- native Flow Thing reads;
- ThingsBoard-compatible relations;
- health, readiness, and metrics.

The contract intentionally documents the stable control-plane and integration
surface. It does not attempt to describe every internal ThingsBoard UI endpoint.

## Generation Strategy

ThingsFlow should move toward generated API assets without forcing a disruptive
rewrite of `flow-core`.

Recommended path:

1. Keep `docs/api/openapi.yaml` as the source contract for public APIs.
2. Validate the contract in CI with lightweight repository tests.
3. Generate Go types, clients, and optional server interfaces with
   `oapi-codegen`.
4. Require new native endpoints, especially Things APIs, to start from the
   OpenAPI contract.
5. Keep legacy ThingsBoard compatibility handlers hand-maintained until they
   are stable enough to model precisely.

This is a contract-first approach. It is less invasive than replacing the
current `net/http` handlers with a new framework, and it gives external users a
stable integration artifact.

## Go Tooling Options

| Tool | Fit for ThingsFlow | Tradeoff |
|---|---|---|
| `oapi-codegen` | Best default. OpenAPI 3 contract first, generated Go types, clients, and server interfaces. | Requires maintaining the spec as source of truth. |
| `swaggo/swag` | Good for annotating existing handlers quickly. | Handler comments can drift and the historical center is Swagger 2.0. |
| `Huma` | Good for new services where handlers and schemas should generate OpenAPI 3.1 automatically. | More framework adoption than ThingsFlow needs right now. |
| `Goa` | Strong design-first framework for larger generated microservices. | More invasive than the current compatibility layer warrants. |

Decision for the public release: use `oapi-codegen` as the planned generation
tool, but keep the first release lightweight by publishing and testing the
OpenAPI file.

## Compatibility Rule

Native APIs should be clear, versionable, and easy for custom applications to
consume. TB-compatible APIs should remain stable enough for the ThingsBoard UI
and classic dashboards.

When there is a conflict, do not bend the native API into a UI-specific shape.
Add an adapter in the compatibility layer and keep the native contract clean.

## API-First Control Plane

ThingsFlow supports operation without the ThingsBoard UI. The UI remains a
compatible console, while Flow Core exposes public control-plane APIs for
automation, custom portals, provisioning services, and industrial workflows.

The API-first lifecycle covers:

- `POST /api/auth/login`
- `POST /api/device`
- `POST /api/device-with-credentials` — create a device and set its credential in one
  call (the UI "Add device" wizard). The credential is validated before the device is
  created, so a bad credential leaves nothing behind.
- `POST /api/device/bulk_import` — CSV fleet onboarding
  (`NAME`, `TYPE`, `LABEL`, `ACCESS_TOKEN`, `DESCRIPTION`, `IS_GATEWAY`).
- `POST /api/device/{deviceId}/jwt`
- `POST /api/v1/devices/me/jwt/refresh` — device-side renewal with the current JWT
- `GET /api/device-connectivity/{id}` — ready-to-run connection snippets for a device
  (device JWT, pinned MQTT client id, TLS endpoint); backs the UI "Check connectivity"
  dialog
- `GET /api/device/{id}/credentials`
- `POST /api/device/credentials`
- `GET|POST /api/device/{id}/security`
- `POST /api/v1/provision`
- `POST /api/relation`
- `GET /api/relations`
- `GET /api/plugins/telemetry/{entityType}/{entityId}/values/timeseries`
- `POST /api/entitiesQuery/find` and `POST /api/alarmsQuery/find` — the entity/alarm
  data queries dashboards are built on
- `GET /api/audit/logs/entity/{id}`
- `GET /api/ws/plugins/telemetry` — WebSocket telemetry/attribute subscriptions
- `POST /api/v1/telemetry` through `http-ingest` with `Authorization: Bearer <deviceJwt.token>`

`POST /api/v1/{token}/telemetry` and `POST /api/v1/{token}/attributes` are **not
available**: they return `503` and direct callers to the HTTP ingest gateway, which
matches only the exact path `/api/v1/telemetry`. `GET /api/v1/{token}/attributes`
still works.

Run `tools/verify-control-plane-api.sh` for the full API-first smoke and
`tools/demo-ui-independent-device-flow.sh` for the UI-independent device flow.
That flow proves a custom portal can provision, publish, read telemetry, rotate
credentials, and change device security without the ThingsBoard UI.

Known Gaps:

- RMQTT native MQTT writes raw MQTT records to NATS; `http-ingest` writes
  canonical HTTP records to NATS.
- Suspending a device stops new JWT issuance but does not invalidate a token already
  in flight: neither Envoy nor RMQTT checks `security_status` per message. The
  short-lived-token mitigation is in place (`flowCore.deviceJwt.ttlSeconds`, 900s on
  the cluster overlay), so the TTL bounds the exposure; online revocation is still
  missing.
- TB classic MQTT topics remain on the compatibility path until a safe native
  credential translation strategy exists.

## Provisioning

ThingsFlow provisioning is API-first and UI-independent. Fleet provisioning does
not depend on the ThingsBoard UI.

### How A Device Receives A Device JWT

There are two supported delivery paths:

1. **Initial self-provisioning**. A device calls `POST /api/v1/provision` with
   `deviceName`, `provisionDeviceKey`, and `provisionDeviceSecret`. Flow Core
   validates the provisioning profile, creates or finds the device, writes the
   compatible `ACCESS_TOKEN`, and returns a short-lived native `deviceJwt`.
2. **Control-plane renewal**. An authenticated operator, portal, or fleet
   service calls `POST /api/device/{deviceId}/jwt` to issue a fresh
   short-lived `deviceJwt` for an existing device. This endpoint is not a
   telemetry path and should not be exposed as an unauthenticated device API.
3. **Device-native refresh**. A device calls
   `POST /api/v1/devices/me/jwt/refresh` with a still-valid
   `Authorization: Bearer <deviceJwt.token>`. Flow Core validates the Device
   JWT and the active device state, then returns a fresh short-lived
   `deviceJwt` without requiring tenant user credentials on the edge.

The provisioning key/secret is a bootstrap credential. The returned
`deviceJwt.token` is a bearer credential for MQTT and HTTP telemetry, so it
must be delivered over TLS, kept out of logs, and refreshed before `expiresAt`.

Example provisioning request:

```json
{
  "deviceName": "sensor-001",
  "deviceType": "temperature",
  "provisionDeviceKey": "profile-key",
  "provisionDeviceSecret": "profile-secret"
}
```

Successful self-provisioning keeps the ThingsBoard-compatible fields:

```json
{
  "status": "SUCCESS",
  "credentialsType": "ACCESS_TOKEN",
  "credentialsValue": "classic-device-token"
}
```

ThingsFlow also returns native fields for NATS-first operation:

```json
{
  "deviceId": "canonical-device-id",
  "tenantId": "tenant-id",
  "deviceJwt": {
    "tokenType": "Bearer",
    "subject": "canonical-device-id",
    "mqttIdentity": "topicSafeDeviceId",
    "mqttUsername": "Bearer <token>"
  }
}
```

Use `credentialsValue` for classic ThingsBoard-compatible clients. Use
`deviceJwt.token` as the RMQTT username for the native MQTT-to-NATS path or as
the HTTP bearer token for `http-ingest`. Existing devices can obtain the same
native token with `POST /api/device/{deviceId}/jwt`, and long-running devices
should prefer `POST /api/v1/devices/me/jwt/refresh` before the current token
expires.

The recommended device-side loop is:

1. bootstrap with provisioning credentials or a managed fleet portal;
2. store only the minimum credential material required by the device;
3. publish MQTT or HTTP telemetry with `deviceJwt.token`;
4. renew before `deviceJwt.expiresAt`;
5. stop publishing and re-bootstrap if renewal fails or the device is
   suspended.

The Python device SDK in `sdk/python` implements this loop for HTTP and MQTT
clients. See [Device SDK](DEVICE_SDK.md).

Credential model:

| Credential | Use |
|---|---|
| `ACCESS_TOKEN` | Classic TB-style MQTT/HTTP compatibility clients. |
| `DEVICE_JWT` | Native RMQTT/http-ingest-to-NATS telemetry path. |
| `X509_CERTIFICATE` | Future stronger fleet identity path with TLS/mTLS. |

## OIDC Auth Broker

ThingsFlow can broker login through an external OIDC provider configured with
`OIDC_PROVIDERS_JSON` or the chart OIDC values. The provider identity is stored
as `external_identity`, but Flow Core still emits its own JWT for the UI and public
control-plane APIs. This keeps the API contract stable and avoids accepting
external bearer tokens directly across internal authorization boundaries.

When an issuer is configured without explicit endpoint URLs, Flow Core reads the
provider's OIDC discovery document at
`<issuer>/.well-known/openid-configuration`. This keeps providers such as
ZITADEL manageable with the standard issuer/client settings while still
allowing explicit token/JWKS URL overrides for local Docker networking.
If the provider does not place `email` in the ID token, configure
`OIDC_USERINFO_URL` or `oidc.userInfoUrl`; Flow Core will call UserInfo with the
access token and verify that the returned `sub` matches the ID token before
creating the UI session.

The broker callback returns the same shape expected by the ThingsBoard UI, for
example `accessToken=...&refreshToken=...`, while preserving the ThingsFlow
session and tenant mapping.

## UI Compatibility Contract

Supported UI target:

- stable: `thingsboard/tb-web-ui:4.3.1.1`
- canary: `thingsboard/tb-web-ui:latest`

Compatibility classes:

| Class | Meaning |
|---|---|
| `native` | First-class product behavior. |
| `compat` | Implemented to preserve ThingsBoard UI behavior. |
| `stub` | Small valid response for disabled or empty UI states. |
| `no-goal` | Outside the current contract. |

The normal-navigation contract covers auth, dashboards, widgets, devices,
assets, customers, users, telemetry REST, attributes REST, WebSocket
subscriptions, alarms, resources, OTA, audit, notifications, disabled rule
chain states, calculated-field disabled states, repository settings disabled
states, device counts, asset/entity-view aliases, 2FA disabled states,
server-to-device RPC (one-way and two-way, over MQTT — opt-in via
`flowCore.rpc.enabled`; see [MQTT Device Authentication](MQTT_DEVICE_AUTH.md)),
mobile disabled state, and clean no-goal responses for edge administration.

No-goals include full classic rule-chain editor behavior, edge runtime and
administration, version-control flows, API key management, domain management,
AI model management, full mobile bundle management, queue statistics, jobs
management, OAuth2/domain administration beyond disabled-state behavior, and
persistent offline RPC.

Executable checks:

```bash
docker compose -f docker/docker-compose-nats.yml up -d postgres greptimedb nats nats-bootstrap flow-core
cd flow-core
go run ./cmd/ui-contract-check \
  -base-url http://localhost:8082 \
  -contract internal/uicontract/testdata/tb-ui-4.3.1.1.json
```

Browser-level coverage lives in `tools/visual-dashboard-smoke.js`.

## Related Documents

- [Digital Twin](DIGITAL_TWIN.md)
- [Security architecture](SECURITY.md)
- [Data plane](DATA_PLANE.md)
- [Operations](OPERATIONS.md)
