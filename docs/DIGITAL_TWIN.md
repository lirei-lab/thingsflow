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

## Twin Models

`twin_model` is the tenant-scoped, versioned catalog that defines a twin's
static attributes, feature declarations, and allowed graph relationships. A
model is authored as JSON and normalized before persistence. The catalog stores
both the normalized authored document (`definition`) and a deterministic,
metadata-free validation document (`schema`). Model versions are immutable;
deleting a version marks it deprecated rather than removing it.

The model identity contract is intentionally narrow:

- `modelId` is stored as a bare slug such as `energy_meter`, never as a URN;
- normalization lowercases first, replaces non-`[a-z0-9_]` runs with `_`,
  trims boundary underscores, and rejects empty or over-255-byte results;
- `version` is exactly three canonical decimal components, each in PostgreSQL
  int32 range, for example `1.0.10`;
- `kind` is `DEVICE` or `ASSET`;
- consumers compose a twin definition such as
  `thingsflow:device:energy_meter:1.0.10`.

The authored language is DTDL-shaped but uses a fixed JSON Schema subset for
properties: `type`, `enum`, numeric bounds, string length/pattern, `required`,
`unit`, and `writable`. Features have their own definition URN plus
`properties` and `desiredProperties`. Unsupported validation/applicator
keywords such as `$ref`, `oneOf`, or `additionalProperties` are rejected at
catalog creation; unrelated authored metadata is retained for forward
compatibility. Validation accepts `null` defensively and treats documents as
partial unless a complete-document check is requested.

Catalog operations are OpenAPI-first and authenticated:

```text
GET    /api/twin-models
POST   /api/twin-models
GET    /api/twin-models/{modelId}/{version}
DELETE /api/twin-models/{modelId}/{version}       # deprecate
PUT    /api/twins/{entityType}/{entityId}/model   # explicit re-point
```

### Model pins and skeletons

Each modeled `twin_registry` row stores `model_id` and `model_version`. On the
first registry sync/backfill, selection is constrained by tenant, entity kind,
and the exact canonical entity type slug before versions are ordered
numerically. Once selected, the pair is immutable during ordinary sync and
backfill: publishing `2.0.0` does not silently move an entity pinned to
`1.0.0`. Only the explicit re-point operation advances a pin. Deprecated
versions remain readable for already-pinned twins.

Twins without a matching model keep the pre-catalog definition and response
bytes. An unrelated model, a same-named model in another tenant, or a model of
the wrong kind cannot affect them.

At read time, a pinned model contributes declaration-only feature skeletons:

```json
{
  "electrical": {
    "definition": "thingsflow:feature:electrical:1.0.0",
    "properties": {},
    "desiredProperties": {}
  }
}
```

These empty maps declare capability; they do not route telemetry or fabricate
state. Observed `features.telemetry` from NATS KV/Postgres remains authoritative
and is never overwritten by a skeleton. Phase 3 feature writes and routing must
reuse these declarations and the same validator.

### Relationship narrowing

`topology_relation_type` remains the global vocabulary and its endpoint-type
check always runs first. A model can only narrow that vocabulary through
`relationships.<name>`:

```json
{
  "target": ["energy_meter", "main_total"],
  "targetEntityTypes": ["DEVICE"],
  "maxCardinality": 8,
  "bidirectional": true
}
```

Narrowing runs only when both endpoints have complete, resolvable pins. One or
both absent pins preserve legacy behavior. Catalog/query/schema failures do not
pass through: they fail the save. For directed edges the environment variable
`TWIN_MODEL_RELATION_ENFORCE` selects behavior:

- unset, empty, invalid, or `warn`: save the edge, emit one structured warning,
  and increment the unlabeled counter
  `flow_twin_model_relation_violations_total` once;
- `reject`: return the typed `ErrRelationNotAllowedByModel`, mapped by the
  classic relation HTTP handler to status `400`.

The warning records `tenant_id`, `relation_type`, both endpoint types and IDs,
both model IDs and versions, `violation_count`, and `enforcement_mode`. The
counter deliberately has no tenant/model labels, avoiding unbounded metric
cardinality.

`maxCardinality` is language and documentation today, not an enforced runtime
limit. A count-then-insert implementation would be a TOCTOU race. Enforcement
must wait for a concurrency-safe database constraint or serialized design; the
current code does not claim a guarantee it cannot provide.

### Bidirectional edges

A bidirectional relation is one `topology_edge` row with
`direction='BIDIRECTIONAL'` in the orientation first submitted by the user.
The ThingsBoard-compatible `relation` table receives both directed mirrors.
The global relation type must allow both endpoint orientations. When both
endpoints are modeled, both models must declare the same relationship with
`bidirectional:true`, authorize the other bare model ID, and authorize the
other entity kind. One-sided declarations and target mismatches are rejected
even while ordinary directed narrowing runs in warn mode.

Reverse saves serialize on a PostgreSQL transaction advisory lock. Its
length-delimited identity includes tenant UUID, relation group, relation type,
and the sorted typed UUID endpoints, exactly matching migration 0014. A reverse
save therefore updates the first stored orientation instead of creating a
second logical row. Tenant, group, relation type, and `DIRECTED` direction stay
independent identities. Traversal sees either endpoint; deleting by either
orientation removes the topology row and both legacy mirrors; consistency
checks treat the two legacy orientations as mirrors of the same modern edge.

### Traversal limitations carried to Phase 3

The current `Neighbors` helper predates the first-class twin API and still has
two known limitations that this phase measures but does not silently repair:

- its topology and legacy queries have no tenant predicate;
- fallback is all-or-nothing: legacy rows are queried only when the topology
  query returns zero rows.

Consequently, if a root has one modern topology child and another legacy-only
child, the legacy-only child is hidden. The Phase 3 traversal API must add the
tenant predicate and explicitly decide/implement a deduplicated topology
`UNION` legacy read rather than preserving this hiding behavior by accident.

### Phase 3 tenant-scoped traversal

The Phase 3 twin API adds two tenant-scoped traversal primitives in
`flow-core/internal/topology` that fix the two limitations above instead of
inheriting them:

- `NeighborsTenant(db, tenantID, root, direction, relationTypes)` — a one-hop
  read that carries the tenant predicate in every SQL branch and reads a single
  deduplicating `UNION` of `topology_edge` plus legacy `relation` rows that are
  not present in `topology_edge`. A legacy-only child is therefore surfaced
  even when the root also has modern topology children, and a row present in
  both stores is reported once. This replaces the all-or-nothing fallback for
  the twin API; the legacy `Neighbors` helper is unchanged for the WS plane.
- `ExpandWithCTE(db, tenantID, root, direction, relationTypes, maxDepth,
  maxNodes)` — a `WITH RECURSIVE` traversal bounded by a depth ceiling and a
  node budget, returning a deterministic, depth-ordered `[]EntityRef` (depth,
  then normalized type, then id).

**Tenant-scoping guarantee.** Every branch of both traversals filters by tenant
at the SQL level, never post-hoc in Go. `topology_edge` rows are filtered on
their `tenant_id` column; legacy `relation` rows (which have no tenant column)
are tenant-resolved through the entity tables (`asset`, `device`, `customer`,
`entity_view`, `dashboard`, `device_profile`, `asset_profile`), so a relation
whose neighbor belongs to another tenant is excluded even if it is
structurally reachable. A cross-tenant edge planted directly into the tables
can therefore never open the graph behind it.

**Deduplicating union.** The traversal read is `topology_edge UNION ALL
legacy-relation-not-in-topology`. The legacy branch carries a `NOT EXISTS`
against `topology_edge`, so a child mirrored in both stores is expanded once;
the recursive result is deduplicated by node at its shallowest depth. There is
no all-or-nothing fallback, so a legacy-only child is never masked by the
presence of any topology child.

**Budget-bounded limits (benchmark-fixed).**

| Constant | Value | Fixed by |
|----------|-------|----------|
| `DefaultExpandMaxDepth` | 10 | `BenchmarkExpandCTEDepth5` / `BenchmarkExpandCTEDepth10` |
| `DefaultExpandMaxNodes` | 5000 | `BenchmarkExpandCTEDepth5` / `BenchmarkExpandCTEDepth10` |

`ExpandWithCTE` returns the typed `ErrTraversalBudget` when the node budget is
exceeded, instead of returning an unbounded result. The defaults were fixed by
the synthetic CTE benchmark (`go test -run '^$' -bench BenchmarkExpandCTE
./internal/topology/`), which runs `ExpandWithCTE` at depth 5 and 10 over a
40,000-edge synthetic graph (two interleaved 20,000-node rings, fan-out 2).
Measured baseline (Intel Xeon W-2245, 2026-08-07, `-benchtime=3x`):

| Benchmark | ns/op | visited nodes | B/op | allocs/op |
|-----------|-------|---------------|------|-----------|
| `BenchmarkExpandCTEDepth5` | ~113 ms | 21 | ~175,509 | 174 |
| `BenchmarkExpandCTEDepth10` | ~1.69 s | 66 | ~181,269 | 444 |

The depth-10 cost is dominated by re-scanning the tenant's edge source once per
recursive level; that measured ceiling is why `DefaultExpandMaxDepth` must not
be raised casually. **Raising either limit requires re-measuring on a
representative topology first.** The existing `idx_topology_edge_*` indexes
serve the traversal; any traversal-index tuning belongs to the later write/REST
waves, not to this foundation.

## Registry Schema

Migration `0011_twin_registry` adds two tables.

### `twin_model`

`twin_model` is the future model catalog. It is intentionally small now:

| Column | Purpose |
|---|---|
| `tenant_id` | Tenant owner. Models are tenant-scoped. |
| `model_id` | Stable bare model identifier, for example `energy_meter`. |
| `version` | Semantic model version, default `1.0.0`. |
| `kind` | Model kind, such as device, asset, feature, gateway. |
| `definition` | JSON metadata for the model. |
| `schema` | Future validation schema for attributes/features. |
| `deprecated` | Lifecycle flag; immutable versions are deprecated, not deleted. |
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
| `model_id`, `model_version` | Optional immutable catalog pin selected on first touch or explicit re-point. |
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
- `twin_model` CRUD/versioning and explicit twin re-pointing are available;
  native twin state writes remain a later API slice.
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
- Model feature skeletons are declarations only; feature routing arrives with
  the Phase 3 native twin API.
- Relationship `maxCardinality` is not enforced because no concurrency-safe
  database constraint exists yet.
- `Neighbors` remains tenantless and uses all-or-nothing legacy fallback; it
  must become tenant-scoped and make the explicit topology/legacy UNION
  decision before Phase 3 exposes traversal over REST.

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
