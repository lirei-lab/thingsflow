# Digital Twin

ThingsFlow Twin is the native digital-twin layer of the platform. It keeps
ThingsBoard UI compatibility, but gives custom UIs, provisioning services,
industrial automation, and analytics clients a more standard representation of
devices, assets, state, and relationships.

The key decision is architectural: **ThingsBoard UI remains a supported
compatibility console, not the core model of the platform**. ThingsFlow exposes a
ThingsBoard-compatible API where that is useful, but the long-term platform
model is a governed digital twin surface over Postgres topology, GreptimeDB
telemetry history, and the native NATS data plane.

ThingsFlow Twin is inspired by Eclipse Ditto and W3C Web of Things vocabulary, but it
does not embed either project as a runtime dependency. The goal is to adopt the
useful contract shape while keeping the current lightweight OSS stack:

- RMQTT for native MQTT telemetry ingress;
- `http-ingest` for native HTTP telemetry ingress;
- NATS JetStream as the raw event bus and NATS KV as latest/twin hot state;
- Bento materializers for telemetry persistence;
- GreptimeDB for historical time series by default, with QuestDB available as
  an optional backend;
- Postgres for operational source of truth, topology, registry, audit, and
  ThingsBoard UI compatibility metadata;
- `flow-core` as control plane, compatibility API, provisioning API, and twin
  read API.

## Model Vocabulary

| Concept | Meaning in ThingsFlow | Current source |
|---|---|---|
| `Thing` | Digital representation of a physical or logical entity. | `twin_registry` over `device` / `asset` |
| `thingId` | Stable native twin identity, independent from the UI route shape. | `twin_registry.thing_id` |
| `policyId` | Access policy reference separated from the payload. | `twin_registry.policy_id` |
| `definition` | Model/capability identifier for the twin or feature. | `twin_registry.definition`, later `twin_model` |
| `attributes` | Mostly static metadata such as name, type, tenant, labels, serials. | `twin_registry.attributes` with fallback from entity rows |
| `features` | State/capability groups, currently latest telemetry. | NATS KV `twin_state`, with optional Postgres snapshot/backup where configured |
| `relations` | Typed links to other twins. | `topology_edge` |

Current supported entity types:

- `DEVICE`
- `ASSET`

The model intentionally starts with the two ThingsBoard entities that matter
most for classic dashboards. Sites, lines, rooms, machines, gateways, areas,
and process nodes can be represented today as assets/devices plus governed
relations. Native kinds can be added later through `twin_model` without breaking
TB UI compatibility.

## Architecture

```mermaid
flowchart LR
    subgraph Clients["Clients"]
        TBUI["ThingsBoard UI<br/>compatibility console"]
        CUI["Custom UI / CLI"]
        OPS["Provisioning / automation"]
    end

    subgraph Core["flow-core control plane"]
        API["TB-compatible REST/WS"]
        TWIN["ThingsFlow Twin API<br/>/api/twins/*"]
        PROV["Provisioning + CRUD"]
        AUTH["Auth / OIDC / device security"]
    end

    subgraph DataPlane["Native telemetry data plane"]
        RMQTT["RMQTT<br/>MQTT JWT edge"]
        HEDGE["Envoy<br/>HTTP JWT gateway"]
        HBENTO["Bento<br/>HTTP -> NATS"]
        NATS["NATS<br/>tf.ingest.*.raw.>"]
        MAT["Bento materializers"]
    end

    subgraph Store["Storage"]
        PG["Postgres<br/>entities, registry, topology"]
        KV["NATS KV<br/>latest/twin hot state"]
        QDB["GreptimeDB<br/>historical telemetry"]
    end

    TBUI --> API
    CUI --> TWIN
    CUI --> API
    OPS --> PROV
    PROV --> PG
    AUTH --> PG
    TWIN --> PG
    TWIN --> KV
    TWIN --> QDB
    API --> PG
    API --> KV
    API --> QDB

    RMQTT --> NATS
    HEDGE --> HBENTO
    HBENTO --> NATS
    NATS --> MAT
    MAT --> QDB
    MAT -->|latest cache for UI + current twin features| KV
```

`flow-core` does not sit in the telemetry hot path. It reads the materialized
state and governs platform operations. This keeps the digital twin API useful
for applications without turning the control plane into a high-volume ingest
worker.

## Source Of Truth

ThingsFlow Twin uses a layered ownership model:

| Data | Source of truth | Why |
|---|---|---|
| Device/asset existence | Postgres `device`, `asset` | TB UI compatibility and existing control-plane APIs. |
| Native twin identity | Postgres `twin_registry` | Stable `thingId`, `policyId`, `definition`, normalized attributes. |
| Twin models | Postgres `twin_model` | Versioned catalog and future validation schemas. |
| Relationships | Postgres `topology_edge` | Tenant-scoped industrial topology, SQL/PGQ-ready. |
| Latest telemetry | NATS KV `twin_state` | Authoritative latest/twin hot state for TB UI hydration and twin reads in the NATS event plane. |
| Historical telemetry | GreptimeDB by default, QuestDB optional | High-volume time-series store outside Postgres. |
| Telemetry events | NATS JetStream | Decoupled ingest and replay boundary. |

This means TB-compatible tables still exist, but they are not the only product
model. `twin_registry` and `topology_edge` are the native layers that let Flow
move toward a Ditto-style twin surface while the upstream TB UI keeps working.

## Latest State In NATS KV

NATS KV is the authoritative hot-state store for latest telemetry and current
twin features. Latest values are read from NATS KV, not from Postgres.

Telemetry key layout:

```text
DEVICE.{tenantId}.{deviceId}.telemetry.{key}
```

Value:

```json
{"ts":1778692040123,"value":22.4}
```

`bento-nats-latest-kv` is the principal writer. Flow Core reads the bucket for
ThingsBoard-compatible latest telemetry endpoints, dashboard WebSocket
hydration, entity-list widgets, active/inactive derivation, and native ThingsFlow Twin
`features.telemetry`.

Postgres may receive eventual snapshots or backup exports, but it is not the
source of truth for current twin state.

## Cache And Record Semantics

The twin layer distinguishes a **hot cache** from the **stores of record**, and
the distinction is operationally load-bearing:

- **Hot cache**: the NATS KV bucket `twin_state`. It is configured with a
  **1-hour TTL and `history: 1`** (`k8s/helm/thingsflow/values.yaml`, `twinKv`
  block, applied in `templates/nats.yaml`). The TTL expires **whole KV entries,
  including the entity state document** — so attributes written through the
  REST API into the state doc evaporate together with the latest telemetry
  after one hour of silence. The next merge then starts from an empty
  `State{}` and recreates the entry (`flow-core/internal/twinstore/nats.go`,
  the `kv.Create` path). This is by design: nothing in the KV is allowed to be
  the only copy of anything.
- **Stores of record**: `twin_registry` (identity), `attribute_kv` (scoped
  attributes), `topology_edge` (relations) in Postgres, and GreptimeDB for
  telemetry history. Reads cascade: when the KV entry is missing or expired,
  twin features fall back to `ts_kv_latest`/GreptimeDB and attribute reads are
  served from `attribute_kv`.
- **No Postgres mirror of latest state**: migration `0012_device_latest_state`
  introduced one and `0013_drop_device_latest_state` removed it deliberately.
  A second authoritative "latest" store reintroduces the write-path coupling
  and drift this architecture exists to avoid — do not bring it back.

Cache expiry is therefore an availability concern (a silent device's latest
view rebuilds lazily), never a durability concern. Anything that must survive
the TTL belongs in a store of record.

### Watch Semantics (`TWIN_STATE_WATCH_ENABLED`)

The KV watch (`flow-core/twin_state.go`) is the **single publisher** of live
WebSocket attribute pushes and re-broadcasts data-plane telemetry. Its flag
semantics are deliberately narrow:

- Unset (default) or any value other than `false`: the watch runs whenever a
  twin store is configured, resubscribing automatically with backoff if the
  underlying KV watch channel closes (NATS consumer death, server restart).
- `TWIN_STATE_WATCH_ENABLED=false`: an operational **kill switch** for a
  broadcast storm. With it set, REST attribute writes still persist to
  `attribute_kv` and the KV, but **no live WS attribute pushes are emitted at
  all** — dashboards only see values at (re-)subscription time. flow-core
  logs a boot-time WARN when a store is configured but the watch is disabled,
  so a forgotten override cannot masquerade as "attributes are broken".

Known residual: the `ts` carried by live attribute pushes (both protocol
generations) is **broadcast time**, not the write's own timestamp — the watch
diff hands `BroadcastAttributes` values without their per-key ts. Widgets that
display the value are unaffected; anything deriving latency/ordering from a
pushed attribute `ts` will see the push time. Accepted for now — changing it
means threading per-key timestamps through the `BroadcastAttributes`
signature.

## Registry Schema

Migration `0011_twin_registry` adds two tables.

### `twin_model`

`twin_model` is the future model catalog. It is intentionally small now:

| Column | Purpose |
|---|---|
| `tenant_id` | Tenant owner. Models are tenant-scoped. |
| `model_id` | Stable model identifier, for example `thingsflow:device:meter`. |
| `version` | Semantic model version, default `1.0.0`. |
| `kind` | Model kind, such as device, asset, feature, gateway. |
| `definition` | JSON metadata for the model. |
| `schema` | Future validation schema for attributes/features. |
| `created_time`, `updated_time` | Millisecond timestamps. |

Primary key: `(tenant_id, model_id, version)`.

### `twin_registry`

`twin_registry` maps existing ThingsBoard-compatible entities to stable native
twin identities.

| Column | Purpose |
|---|---|
| `tenant_id` | Explicit tenant boundary. |
| `thing_id` | Native stable twin id. |
| `entity_type` | Currently `DEVICE` or `ASSET`. |
| `entity_id` | UUID of the TB-compatible entity. |
| `policy_id` | Policy reference for future authorization model. |
| `definition` | Model identifier, for example `thingsflow:device:meter:1.0.0`. |
| `attributes` | Normalized JSON attributes. |
| `created_time`, `updated_time` | Millisecond timestamps. |
| `version` | Optimistic version counter. |

Constraints:

- unique `(tenant_id, thing_id)`;
- unique `(tenant_id, entity_type, entity_id)`;
- `entity_type IN ('DEVICE', 'ASSET')`.

The registry is safe for rolling upgrades. If a registry row is missing, the
twin API falls back to the classic entity projection until migration/backfill
has caught up.

## Backfill And Idempotency

The migration backfills existing `device` and `asset` rows into
`twin_registry`.

Generated defaults:

```text
thingId    = <tenantId>:device:<deviceId>
policyId   = tenant:<tenantId>:default
definition = thingsflow:device:<sanitized type>:1.0.0
```

For assets the same rule uses `asset` instead of `device`.

The backfill is idempotent:

- first run inserts missing registry rows;
- later runs only update rows whose `thing_id`, `policy_id`, `definition`, or
  `attributes` actually changed;
- unchanged rows are not rewritten.

This matters operationally because the same SQL can be used during bootstrap,
local tests, or controlled repair without incrementing versions or producing
false drift.

### Live Registry Maintenance

The registry is no longer populated only by the one-shot migration; it is kept
convergent at runtime:

- **Create hooks**: every device/asset create path — UI CRUD
  (`internal/device/crud_device.go`, `internal/asset/asset.go`), CSV bulk
  import (`internal/device/bulk_import.go`,
  `internal/system/asset_bulk_import.go`), device provisioning
  (`internal/provisioning/provisioning.go`), and demo bootstrap
  (`internal/bootstrap/demo.go`) — upserts the entity's registry row through a
  hook injected at boot (`flow-core/main.go`) into
  `twin.SyncRegistryRow`.
- **Delete hooks**: device and asset deletion reclaim the registry row via
  `twin.DeleteRegistryRow`. This is required because `twin_registry` has no
  foreign key to `device`/`asset`, so nothing cascades.
- **Boot convergence**: `twin.BackfillRegistryContext` runs in the maintenance
  goroutine on every boot (30s budget, log-not-fatal) — it upserts rows for
  entities created while hooks were not live and sweeps orphaned rows whose
  entity no longer exists.

Hook failures are logged and never fail the CRUD operation itself; the boot
pass is the safety net that re-converges the registry.

## API Contract

Current endpoint:

```http
GET /api/twins/{entityType}/{entityId}
```

Authentication:

- same `X-Authorization: Bearer <jwt>` header as the TB-compatible API;
- tenant users can only read twins from their own tenant;
- cross-tenant reads return `403`;
- unsupported entity types return `400`;
- missing entities return `404`.

Example:

```http
GET /api/twins/DEVICE/33333333-3333-3333-3333-333333333333
X-Authorization: Bearer eyJ...
```

Response shape:

```json
{
  "thingId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:device:33333333-3333-3333-3333-333333333333",
  "policyId": "tenant:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:default",
  "definition": "thingsflow:device:meter:1.0.0",
  "entity": {
    "entityType": "DEVICE",
    "id": "33333333-3333-3333-3333-333333333333"
  },
  "attributes": {
    "id": "33333333-3333-3333-3333-333333333333",
    "entityType": "DEVICE",
    "tenantId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "createdTime": 1778692040123,
    "name": "Meter A",
    "type": "meter",
    "label": "Main meter",
    "serial": "M-1"
  },
  "features": {
    "telemetry": {
      "definition": "thingsflow:feature:telemetry:1.0.0",
      "properties": {
        "temperature": {
          "value": 22.5,
          "ts": 1778692040123,
          "source": "nats_kv"
        },
        "active": {
          "value": true,
          "ts": 1778692040123,
          "source": "nats_kv"
        }
      }
    }
  },
  "relations": [
    {
      "type": "Contains",
      "group": "COMMON",
      "direction": "IN",
      "source": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:asset:11111111-1111-1111-1111-111111111111",
      "target": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:device:33333333-3333-3333-3333-333333333333",
      "sourceEntity": {
        "entityType": "ASSET",
        "id": "11111111-1111-1111-1111-111111111111"
      },
      "targetEntity": {
        "entityType": "DEVICE",
        "id": "33333333-3333-3333-3333-333333333333"
      }
    }
  ]
}
```

## Compatibility With ThingsBoard UI

ThingsBoard-compatible APIs remain unchanged:

- `Device` and `Asset` CRUD;
- `/api/relation` and `/api/relations`;
- `entitiesQuery` and dashboard aliases;
- telemetry read APIs;
- WebSocket subscriptions;
- dashboards, widgets, resources, SCADA symbols, and demo bootstrap.

ThingsFlow Twin is additive. It does not require operators to abandon the TB UI and it
does not require custom applications to depend on TB UI internals.

Compatibility rule:

```mermaid
flowchart LR
    TB["ThingsBoard UI"] -->|classic API| ZC["flow-core"]
    ZC -->|classic shape| TBCompat["device / asset / relation APIs"]
    ZC -->|latest state| KV["NATS KV twin_state"]
    ZC -->|native shape| Twin["twin_registry + topology_edge"]
    Apps["Custom apps"] -->|/api/twins| ZC
```

The same physical device can be seen through both contracts:

- TB UI sees a `DEVICE` with TB-style relations and latest telemetry.
- Native clients see a `Thing` with attributes, features, policy, definition,
  and topology relations.

## Relationship To Topology

ThingsFlow Twin relations come from `topology_edge`, not from ad hoc joins against the
legacy `relation` table.

`topology_edge` provides:

- explicit `tenant_id` on every edge;
- governed relation types such as `Contains`, `LocatedIn`, `Feeds`,
  `DependsOn`;
- direction and metadata;
- traversal-oriented indexes;
- a future path to SQL/PGQ property graph projection.

The legacy `relation` table remains synchronized for the TB UI. The native twin
contract should treat `topology_edge` as the relationship source of truth.

Topology direction:

- keep Postgres as the operational source for topology;
- expose governed relation types such as `Contains`, `LocatedIn`, `Feeds`, and
  `DependsOn`;
- reject cross-tenant relation creation;
- maintain compatibility rows for the ThingsBoard UI;
- prepare for SQL/PGQ projection when PostgreSQL support is stable enough for
  the target runtime.

This avoids adding a separate graph database before the pilot has proven that
Postgres topology queries are insufficient.

## Security And Multi-Tenancy

Current controls:

- twin reads require a valid platform JWT;
- `tenantId` from the JWT is enforced against the entity tenant;
- registry rows carry explicit `tenant_id`;
- topology edges carry explicit `tenant_id`;
- cross-tenant relation creation is rejected in the topology layer;
- the data plane resolves device credentials before events are accepted;
- device telemetry flow does not grant direct access to the twin API.

Near-term policy work:

- make `policyId` resolve to a real policy document;
- add per-feature authorization for commands and desired state;
- expose policy checks to custom UIs without coupling them to TB UI roles;
- add audit events for native twin write operations once writes exist.

## Operations

### Local verification

Use the compose stack and a test database:

```bash
docker compose -f docker/docker-compose-nats.yml up -d postgres

docker compose -f docker/docker-compose-nats.yml exec -T postgres \
  psql -U postgres -d postgres \
  -c "DROP DATABASE IF EXISTS flowtest_twin WITH (FORCE);" \
  -c "CREATE DATABASE flowtest_twin;"

FLOW_TEST_PG_DSN='postgres://postgres:postgres@localhost:5432/flowtest_twin?sslmode=disable' \
  go test ./internal/twin
```

Full release gate:

```bash
(cd flow-core && go test ./...)
python3 -m unittest \
  tools/python/test_nats_config.py \
  tools/python/test_postgres_latest_state_cleanup.py
helm template thingsflow k8s/helm/thingsflow >/tmp/thingsflow-render.yaml
bash tools/check-oss-release.sh
```

### Cluster verification

For the controlled pilot, the twin registry is part of the normal `flow-core`
migration path. After deploy, verify:

- `/ready` is OK;
- TB UI dashboards still render classic demo data;
- MQTT/HTTP telemetry still lands in GreptimeDB and NATS KV latest state;
- `GET /api/twins/DEVICE/{id}` returns a registry-backed `thingId`;
- `relations` reflect `topology_edge` and do not show cross-tenant data.

## Failure Modes

| Failure | Expected behavior |
|---|---|
| `twin_registry` table missing during rolling upgrade | `/api/twins` falls back to classic entity projection. |
| Entity exists but no registry row yet | `/api/twins` returns fallback `thingId`, `policyId`, `definition`, attributes. |
| Entity belongs to another tenant | `403 Cross-tenant access denied`. |
| Entity type is unsupported | `400 Unsupported twin entity type`. |
| Device/asset does not exist | `404 Twin entity not found`. |
| Latest telemetry is empty | `features.telemetry.properties` is an empty object. |
| Topology has no edges | `relations` is an empty list. |

## Current Limitations

These are intentional limits of the current implementation:

- The API is read-only.
- `twin_model` is created but not yet exposed through CRUD APIs.
- `policyId` is a stable reference, not yet an enforced policy document.
- `features.telemetry` is backed by NATS KV `twin_state` in the NATS event
  plane. Postgres latest-style tables are compatibility/snapshot surfaces only,
  not the authoritative hot-state store.
- The NATS KV hot state carries a 1-hour whole-document TTL: latest telemetry
  AND any attributes written into the state doc expire together, and reads
  fall back to the stores of record (see Cache And Record Semantics).
- Desired/reported state is not implemented yet.
- Twin search/list APIs are not implemented yet.
- Native twin writes are not implemented yet; device/asset CRUD still happens
  through existing TB-compatible control-plane APIs.

## Things API Direction

The current native endpoint is intentionally read-only. The next API slice
should expose Things as first-class resources without removing
ThingsBoard-compatible device and asset APIs. The planned shape is:

| Operation | Purpose | Notes |
|---|---|---|
| `GET /api/twins` | Search native things by tenant, kind, definition, relation, and text. | Should paginate and avoid dashboard-specific aliases. |
| `GET /api/twins/{entityType}/{entityId}` | Read current projection. | Implemented today for `DEVICE` and `ASSET`. |
| `PUT /api/twins/{thingId}/attributes` | Update native attributes. | Must mirror safe fields back to TB-compatible entities when required. |
| `GET /api/twin-models` | List model catalog. | Backed by `twin_model`. |
| `POST /api/twin-models` | Add or update a model version. | Start from OpenAPI contract first. |

New Things endpoints should be added to `api/openapi.yaml` before
implementation and then generated into Go types with the API tooling described
in [API_REFERENCE.md](API_REFERENCE.md).

## Roadmap

Already delivered on this path: the twin registry is kept live at runtime
(create/delete hooks on every entity path plus boot convergence with orphan
sweep — see Live Registry Maintenance). Attribute change propagation through
the twin-state watch is the next slice of the same phase.

Recommended next phases:

1. **Twin feature state API**: extend filtering and partial reads over NATS KV
   `twin_state`; keep any Postgres latest-style projection as an optional
   backup/snapshot surface only.
2. **Twin search API**: add `GET /api/twins` with pagination, search, kind,
   definition, relation count, and feature count.
3. **Model catalog**: expose CRUD/validation for `twin_model`, seed common
   device/asset/feature models, and document model versioning.
4. **Desired/reported state**: model commands and configuration as events,
   keeping device writes out of `flow-core` hot paths.
5. **Policy model**: make `policyId` resolve to enforceable authorization
   policies for custom UIs and automation.
6. **SQL/PGQ projection**: expose topology as a property graph once PostgreSQL
   support is stable enough for the target runtime.

## Why This Matters

Classic ThingsBoard is excellent at giving operators a working UI quickly, but
its entity model is loose: devices, assets, relations, dashboards, and rule
chains can become tightly coupled to UI behavior. ThingsFlow keeps the useful UI
surface while adding a native platform model that is:

- API-first;
- UI-independent;
- tenant-scoped;
- topology-aware;
- compatible with existing TB dashboards;
- ready for more standard digital-twin concepts;
- inexpensive to operate in the current OSS stack.

That is the strategic bridge: TB UI can remain a mature console, while Flow
becomes the actual IoT middleware and digital twin foundation.
