# Security Architecture

ThingsFlow separates human/API security, device-edge security, and data-plane
processing. A device credential that can publish telemetry does not grant access
to Flow Core APIs, and a public UI/API ingress does not imply public MQTT or HTTP
telemetry exposure.

## Trust Boundaries

```mermaid
flowchart LR
    Browser["Browser / API client"] --> Ingress["Operator ingress + TLS"]
    Ingress --> Core["Flow Core"]
    Core --> PG["Postgres<br/>users, credentials, audit, alarms"]
    Core --> KV["NATS KV<br/>latest/twin hot reads"]
    Core --> QDB["GreptimeDB<br/>history reads"]

    MQTTDevice["MQTT device"] --> MQTTEdge["RMQTT endpoint<br/>private or TLS"]
    MQTTEdge --> RMQTT["RMQTT auth-jwt + ACL"]

    HTTPDevice["HTTP device"] --> HTTPEdge["HTTP telemetry endpoint<br/>private or TLS"]
    HTTPEdge --> Envoy["Envoy jwt_authn"]
    Envoy --> BentoHTTP["Bento http-ingest"]

    Core -->|"device JWT public PEM"| RMQTT
    Core -->|"device JWT JWKS"| Envoy

    RMQTT --> NATS["NATS JetStream"]
    BentoHTTP --> NATS
    NATS --> Latest["Bento latest KV"]
    NATS --> History["Bento GreptimeDB"]
    NATS --> AlarmDet["Bento alarm detector"]
    AlarmDet --> NATS
    NATS --> AlarmMat["alarm-materializer"]

    Latest --> KV
    History --> QDB
    AlarmMat --> PG
```

## Authentication Surfaces

| Surface | Mechanism | Owner |
|---|---|---|
| Browser/API | platform JWT, refresh token, optional OIDC | Flow Core |
| ThingsBoard UI adapter | Same Flow Core auth contract as API users | Flow Core |
| MQTT telemetry | ES256 Device JWT in MQTT username | RMQTT validates with Flow Core public PEM |
| HTTP telemetry | ES256 Device JWT bearer token | Envoy validates with Flow Core JWKS |
| Legacy compatibility | Classic access token where supported | Flow Core compatibility handlers |

Flow Core publishes public device verifiers:

```text
/api/noauth/device-jwt-public.pem
/.well-known/thingsflow-device-public.pem
/api/noauth/device-jwks
```

Production deployments must pin the platform JWT signing key and the device
JWT ES256 private key. Prefer Kubernetes Secrets over inline Helm values:

```yaml
production: true
flowCore:
  allowedOrigin: "https://ui.example.com"
  jwtTokenSigningKeyExistingSecret:
    name: thingsflow-platform-keys
    key: jwt-token-signing-key
  deviceJwt:
    privateKeyExistingSecret:
      name: thingsflow-device-jwt
      key: device-jwt-es256-private-key-pem-b64
    additionalPublicJwksExistingSecret:
      name: thingsflow-device-jwt
      key: previous-public-jwks-b64
```

Inline `flowCore.jwtTokenSigningKey` and
`flowCore.deviceJwt.privateKeyPemBase64` remain available for local smoke tests
and private overlays, but should not be committed or printed in CI logs.
Ephemeral signing keys are acceptable only for local smoke tests.

This is deliberately similar to Mainflux/Magistrala's split: user tokens manage
platform resources, device credentials publish telemetry, and adapters enforce
the device/channel permission before messages reach the broker. In ThingsFlow the
channel equivalent is the device's allowed MQTT topic or HTTP telemetry route.

## Security Workflow

The production workflow is meant to be strict by default but easy to operate:

1. Choose the human identity mode.
   - Local smoke/demo: `testAuth.enabled=true` deploys Dex with static users.
   - Pilot/production: `testAuth.enabled=false` and `oidc.enabled=true` point
     Flow Core at the organization identity provider.
2. Generate or import the operator-owned Secrets before Helm install.
   - `thingsflow-platform-keys` signs Flow Core API/session JWTs.
   - `thingsflow-device-jwt` signs native device JWTs for MQTT and HTTP telemetry.
   - `thingsflow-postgres` owns the metadata database password.
   - `thingsflow-nats-auth` owns the internal data-plane credential.
   - `thingsflow-oidc` owns the OIDC client secret and state signing key.
   - `thingsflow-backup-s3` owns backup/object-storage credentials when backups
     are enabled.
3. Install with `production=true` and Secret references, not inline private
   values.
4. Expose only the surfaces required for the pilot.
   - Browser/API ingress is separate from MQTT and HTTP telemetry exposure.
   - Public MQTT or HTTP telemetry must use TLS or mTLS and JWT verification.
5. Provision devices through Flow Core.
   - Flow Core stores device metadata, credentials, audit, and lifecycle state.
   - Devices publish with Device JWTs; RMQTT/Envoy validate before NATS.
6. Keep telemetry out of Flow Core.
   - RMQTT/HTTP ingest write into NATS.
   - Bento updates NATS KV latest/twin hot state and historical storage.
   - Flow Core reads NATS KV and GreptimeDB for UI/API hydration.
7. Validate after every security-sensitive change.
   - Render the chart with production values.
   - Run the public test suite and `tools/verify-pilot-acceptance.sh`.
   - Check logs with `tools/check-log-policy.sh` before sharing artifacts.

For pilots, use the helper to generate the required Secret manifest:

```bash
NAMESPACE=thingsflow OUT=/tmp/thingsflow-pilot-secrets.yaml \
  tools/generate-pilot-secrets.sh

kubectl -n thingsflow apply -f /tmp/thingsflow-pilot-secrets.yaml
```

The helper writes the manifest with mode `0600`. It does not apply anything
unless `APPLY=true` is set. Backup credentials are generated only when the
`BACKUP_*` variables are supplied; otherwise set `backup.enabled=false` in the
private overlay until object storage is ready.

### Rotation Workflow

Rotate credentials with overlap instead of replacing everything at once:

| Secret | Rotation approach |
|---|---|
| `thingsflow-platform-keys` | Rotate during a maintenance window. Existing platform JWT sessions become invalid and users sign in again. |
| `thingsflow-device-jwt` | Generate a new ES256 private key, keep old public JWKs in `previous-public-jwks-b64`, lower Device JWT TTL during the window, roll RMQTT/HTTP verifier clients, then remove old public keys after TTL/cache expiry. |
| `thingsflow-nats-auth` | Update Secret, roll NATS clients together with the server or use a short dual-account window when using external NATS accounts. |
| `thingsflow-postgres` | Rotate with the database password procedure, then roll Flow Core and materializer pods. |
| `thingsflow-oidc` | Rotate in the identity provider and Kubernetes Secret, then roll Flow Core. |
| `thingsflow-backup-s3` | Rotate in object storage and Secret, then verify the next backup and restore drill. |

This gives operators a small number of durable secrets, all owned outside the
chart, while keeping the high-volume data plane independent from human API
credentials.

## Device Bootstrap And JWT Delivery

Devices receive native telemetry credentials through provisioning, not through
the ThingsBoard UI runtime:

1. The operator creates or configures a device profile with a
   `provisionDeviceKey` and hashed `provisionDeviceSecret`.
2. The device performs initial bootstrap with `POST /api/v1/provision`.
3. Flow Core validates the profile secret, creates or finds the device, records
   compatible `ACCESS_TOKEN` credentials, and returns a short-lived
   `deviceJwt`.
4. The device uses `deviceJwt.token` as the MQTT username for RMQTT or as the
   HTTP bearer token for `http-ingest`.
5. A fleet service, portal, or automation can renew an existing device JWT with
   authenticated `POST /api/device/{deviceId}/jwt`.

The provisioning key/secret is a bootstrap credential and should not be reused
as the long-term telemetry credential. The Device JWT is a bearer credential:
deliver it only over TLS, keep it out of logs, and size
`flowCore.deviceJwt.ttlSeconds` so suspended devices age out quickly enough for
the deployment risk model.

For higher-assurance fleets, prefer one of these bootstrap patterns:

| Pattern | Use |
|---|---|
| Per-device provisioning secret | Simple pilot and controlled factory enrollment. |
| Claim token from a fleet portal | Operator or installer claims a device once, then the device receives short-lived JWTs. |
| X.509/mTLS bootstrap | Stronger production identity, especially for devices crossing untrusted networks. |

## Device Authorization

The device edge is deny-by-default:

- MQTT devices publish telemetry only under
  `thingsflow/devices/{mqttIdentity}/telemetry`.
- MQTT devices publish attributes only under
  `thingsflow/devices/{mqttIdentity}/attributes`.
- The `mqttIdentity` in the topic must match the verified Device JWT. This is
  enforced at two independent layers:
    1. **Broker** — RMQTT pins the MQTT Client ID to the JWT's `clientid` claim
       (`validate_claims.clientid`), rejecting a mismatched connection, and the
       ACL grants publish only on `thingsflow/devices/%c/…` (`%c` being that
       pinned Client ID). A device therefore cannot address another device's
       topic at all.
    2. **Materializer** — for MQTT ingest the written `device_id` is taken from
       the JWT's `deviceId` claim, never from the payload or topic, so a forged
       body cannot re-attribute a reading.
- HTTP telemetry uses `POST /api/v1/telemetry`; the device stream comes from
  verified JWT claims stamped by Envoy, not from a client-provided URL path.
- Suspended devices must not receive fresh Device JWTs from provisioning or
  control-plane renewal endpoints.
- Wildcard subscriptions and system topics are denied for devices.

**Residual risk — no ingest-time revocation.** Device JWTs are bearer tokens and
neither Envoy nor RMQTT consults device `security_status` per message. Suspending
a device stops *new* token issuance but does not invalidate a token already in
flight, so the TTL is the effective revocation window. It is therefore kept short
(`flowCore.deviceJwt.ttlSeconds` — chart default 86400s; hardened overlays use 900s; size it to your exposure model) and the edge
refreshes ahead of expiry. Fleets needing immediate cut-off require an online
denylist consulted at the edge — not currently implemented.

## Device Lifecycle And Rotation

Device security state is part of the control plane:

| State | Meaning |
|---|---|
| `active` | Device credentials can be used by native edges. |
| `suspended` | New credentials should be denied and active tokens should age out quickly. |
| `revoked` | Credentials are no longer valid and should not be reused. |

Provisioning writes device credentials, audit events, and native Device JWT
metadata. Rotating credentials should:

1. issue a new classic access token where compatibility clients need it;
2. issue new Device JWT material for native RMQTT and HTTP telemetry;
3. keep old public JWKs available for a bounded overlap window;
4. shorten token TTLs or introduce online revocation for higher-risk fleets.

Public MQTT and HTTP telemetry exposure must use TLS or a trusted private
network boundary. Device JWTs are bearer credentials and must never cross
untrusted plaintext networks.

Long-running demos and fleet agents must renew Device JWTs before `exp`. The
in-cluster demo simulator uses `demoSimulator.deviceJwtRefreshMarginSeconds`
to refresh and reconnect before RMQTT starts rejecting publishes. Production
agents should follow the same pattern and avoid storing Device JWTs longer than
their TTL.

The Python SDK in `sdk/python` follows this pattern and supports renewal by
re-provisioning, by an authenticated fleet/control-plane token supplier, or by
the device-native `POST /api/v1/devices/me/jwt/refresh` endpoint while the
current Device JWT is still valid. Avoid shared provisioning secrets on field
devices after bootstrap.

## NATS And Processor Security

NATS receives telemetry only after edge validation. Inside the cluster:

- raw telemetry subjects are not public endpoints;
- processors run with bounded service accounts and no browser/API credentials;
- Bento processors use trusted metadata from RMQTT/Envoy, not untrusted payload
  tenant claims;
- NATS KV `twin_state` is an internal hot-state store, not a public database;
- alarm intents are internal events consumed by `alarm-materializer`.

If NATS is exposed outside the cluster, use NATS authentication, TLS, account
boundaries, and subject permissions. The public chart keeps it internal but can
enable username/password auth for the built-in profile:

```yaml
nats:
  auth:
    enabled: true
    existingSecret:
      name: thingsflow-nats-auth
      userKey: username
      passwordKey: password
```

In production, the built-in NATS event plane must run with `nats.auth.enabled`.
RMQTT does not receive the password in its ConfigMap. The chart writes a
credential-free NATS server URL plus `auth.username` and `auth.password`
placeholders into the RMQTT bridge template. An init container renders those
placeholders from the NATS Secret into an `emptyDir` volume before RMQTT starts.
This matches the `rmqtt-bridge-egress-nats` auth contract and avoids leaking
NATS credentials into public chart values or ConfigMaps.

Use a URL-safe password in `thingsflow-nats-auth`: Bento and Flow Core consume the
same credential through `nats://user:password@host`, while RMQTT consumes it
through the bridge `auth.*` fields. `openssl rand -hex 32` is a safe generator.

Recommended production subject boundaries:

| Client | Publish | Subscribe |
|---|---|---|
| RMQTT bridge | `tf.ingest.mqtt.raw.>` | none |
| HTTP ingest Bento | `tf.ingest.http.raw.>` | none |
| latest KV Bento | KV writes to `twin_state` | `tf.ingest.*.raw.>` |
| GreptimeDB Bento | none | `tf.ingest.*.raw.>` |
| alarm detector Bento | `tf.alarm.intent.>` | `tf.ingest.*.raw.>` |
| alarm materializer | none | `tf.alarm.intent.>` |
| Flow Core | none | NATS KV `twin_state` reads/watch |

## Data Protection

- Put browser/API traffic behind HTTPS ingress.
- Keep RMQTT and `http-ingest` private unless a fleet requires external access.
- Use TLS or mTLS for any public device-edge exposure.
- Never send Device JWTs over plaintext networks outside local smoke tests.
- Do not log full telemetry payloads at INFO.
- Do not log access tokens, refresh tokens, Device JWTs, MQTT passwords, OIDC
  secrets, or backup credentials.
- Use GreptimeDB for telemetry history and NATS KV for latest/twin hot state;
  do not use logs as telemetry storage.

## Public Exposure

Public access is opt-in. `ingress.enabled=false` and `ClusterIP` services are
the safe defaults for local and private test environments. Enabling browser/API
ingress does not automatically publish MQTT or HTTP telemetry.

Common exposure values:

```yaml
ingress:
  enabled: true
rmqttEdge:
  serviceType: LoadBalancer
```

Any public device edge must validate JWT-bearing traffic before events reach
NATS, and must terminate TLS.

**Publishing MQTT over TLS.** `rmqttEdge.tls` adds a TLS listener (port 8883,
the IANA `secure-mqtt` port) serving a certificate from a Kubernetes Secret —
typically a cert-manager Certificate (e.g. Let's Encrypt via a DNS-01 solver),
renewed automatically:

```yaml
rmqttEdge:
  tls:
    enabled: true
    port: 8883
    secretName: rmqtt-mqtt-tls
```

The plain `1883` listener stays a `ClusterIP` service and is not published. Note
that MQTT clients assume TLS on 8883 by convention, so exposing plaintext on
that port fails the handshake rather than falling back. See
[MQTT Device Auth](MQTT_DEVICE_AUTH.md) for the client-side contract.

## Alarm Security

Alarm detection is data-plane processing. Bento emits alarm intents, and
`alarm-materializer` writes Postgres alarm state. User actions such as
acknowledge, clear, comments, and assignment remain Flow Core API operations
subject to normal tenant authorization.

This split prevents device telemetry credentials from performing operator alarm
actions while still allowing alarms to be generated without Flow Core ingest.

## OIDC And Human Identity

OIDC is brokered into local Flow Core/ThingsBoard-compatible users:

- external provider tokens are exchanged at the auth boundary;
- Flow Core maps external identity to tenant and authority;
- clients use platform JWTs against the platform API;
- provider-specific tokens do not become the internal platform contract.

When OIDC is enabled, production must configure client secrets, issuer URLs,
JWKS URLs, redirect URLs, and state signing keys explicitly.

Dex is a test/demo identity provider in this repository. The `testAuth` profile
deploys Dex with static credentials so CI, local compose, and demo clusters can
exercise the full ThingsBoard UI authorization-code flow quickly. The profile
must stay disabled for production. `testAuth.enabled=true` is rejected when
`FLOW_ENV=production` or `production=true`.

Prefer existing Secrets for OIDC private values:

```yaml
oidc:
  enabled: true
  clientId: thingsflow-ui
  issuer: https://idp.example.com
  userInfoUrl: https://idp.example.com/oidc/v1/userinfo   # ZITADEL shape; adjust per provider
  clientSecretExistingSecret:
    name: thingsflow-oidc
    key: client-secret
  stateSigningKeyExistingSecret:
    name: thingsflow-oidc
    key: state-signing-key
```

## Audit And Logs

Postgres stores platform audit events for:

- login and authentication events;
- entity changes;
- provisioning and credential changes;
- device security status changes;
- alarm lifecycle actions;
- relevant administrative/security operations.

Telemetry messages, successful broker auth checks, and routine health checks are
not audit events. They are operational metrics/logs.

## Production Checklist

- Set `production=true` or `flowCore.env=production`.
- Override development credentials.
- Use `postgres.passwordExistingSecret` or a private Postgres password overlay;
  the chart rejects the `postgres/postgres` dev default in production.
- Pin JWT signing keys and device JWT ES256 private keys with existing Secrets
  or a private values overlay.
- Enable `nats.auth.enabled=true` for the built-in NATS event plane; use
  `nats.auth.existingSecret` in production.
- Enable file-backed NATS persistence for any pilot that must survive pod or
  node restarts.
- Use explicit CORS origins; avoid `allowedOrigin: "*"`.
- Enable HTTPS for browser/API ingress.
- Keep RMQTT and `http-ingest` internal or expose only behind TLS/mTLS.
- Verify Envoy JWKS settings and cache duration for HTTP telemetry.
- Configure NATS with authentication/TLS if exposed beyond the namespace; for
  production, prefer subject-level permissions/accounts as the next hardening
  layer.
- Keep raw NATS, NATS KV, GreptimeDB, and Postgres internal.
- Validate OIDC secrets and state signing keys when OIDC is enabled.
- Rotate device JWT signing keys with a controlled window. Current Flow Core
  signs with a single active key, can publish previous public keys in JWKS via
  `DEVICE_JWT_ADDITIONAL_PUBLIC_JWKS_B64`, and still exposes one active public
  PEM for RMQTT. Plan RMQTT PEM rollover around token TTL/cache windows.
- Run Helm template checks, local NATS smoke, and pilot readiness checks before
  public or industrial deployment.
