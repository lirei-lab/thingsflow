# ThingsBoard UI contract — data fidelity

[UI Contract Coverage](UI_CONTRACT_COVERAGE.md) verifies that every endpoint the
real ThingsBoard UI calls reaches a real handler: 284 routed, 89 declared
no-goal, 0 gap. That check only ever asked "does this respond with the right
status and shape?" — never "is the data in the body real?" A handler can be
fully "routed" and still return hardcoded zeros, silently re-list instead of
create, or answer `200` to a `DELETE` that deleted nothing. This document is
that second, orthogonal axis: for each of the 284 routed entries, does it do
real work, or does it fake it?

Background and methodology: [ADR-0002](adr/0002-ui-contract-data-fidelity-audit.md).

## Result

| | count |
|---|---|
| Audited | 284 / 284 routed entries |
| `confirmed-gap` | 28 |
| `confirmed-gap-conditional` | 9 |
| `out-of-scope` (deliberate, documented non-answer) | ~90 |
| `verified` (real, complete work) | ~150 |
| `needs-live-check` (undecidable statically) | 2 |
| **Fixed so far** | **14** |

The 37 real findings cluster almost entirely in `internal/system/*.go` (8 of
9 files carry zero dedicated tests) and inline closures in `api.go` — exactly
where [ADR-0002](adr/0002-ui-contract-data-fidelity-audit.md)'s Tier-0 signal
predicted risk would concentrate, not spread evenly across all 284.

## Fixed: pass 1 — endpoints that faked success

Four endpoints answered `200`/data to a request that did nothing — worse than
an honest stub, because the caller has evidence of success for an action that
never happened. All four now fail honestly (`404`/`405`), matching the
"answered honestly" convention [UI Contract Coverage](UI_CONTRACT_COVERAGE.md)
already established for its 12 endpoints that return `501` rather than a
misleading `200`.

| Endpoint | Was | Now | Why |
|---|---|---|---|
| `DELETE /api/calculatedField/{id}` | `200 OK` | `404` (matches sibling GET) | No `calculated_field` row is ever created (POST doesn't persist either — see backlog); GET already honestly says "not found." DELETE agreeing is the fix, not new delete logic. |
| `DELETE /api/widgetType/{id}` | `200` + the row's data | `405 Method Not Allowed` | Route registered method-agnostic (comment: "read-only") but nothing enforced that. No `DELETE FROM widget_type` exists anywhere in the repo. |
| `DELETE /api/widgetsBundle/{id}` | `200` + the row's data | `405 Method Not Allowed` | Same bug, same fix, same "not supported yet" convention already used for `POST`/`PUT /api/widgetsBundle`. |
| `DELETE /api/ruleChain/{id}` | `200` + the chain's data | `405 Method Not Allowed` | `HandleRuleChainByID` has no method branch; only ever `SELECT`s. No `DELETE FROM rule_chain` exists anywhere in the repo. |

Files: `flow-core/internal/system/feature_handlers.go`,
`flow-core/internal/widget/widget.go`, `flow-core/api.go`. Tests:
`flow-core/internal/system/calculated_field_delete_test.go`,
`flow-core/internal/widget/widget_delete_test.go`,
`flow-core/rulechain_delete_test.go` — each asserts the honest status, red
against the prior code, green now. None touch Postgres: all four fixes
reject the bad method before any DB access.

## Fixed: pass 2 — real data that existed but was never read

Eight P2 findings where a real, populated subsystem already existed and the
handler simply never called it. Each of these is a wire-up, not new
behaviour — the data, the tables and the query helpers were all already
there.

| Endpoint | Was | Now |
|---|---|---|
| `GET /api/usage` | `transportMessages: 0`, every `max*: 0` | `transportMessages` from the `ts_kv` snapshot `internal/usage` already writes every minute (the multi-replica-safe source, up to 60s stale — deliberately chosen over the process-local atomic); the five quota maximums from `quotas.LimitsFor`, TTL-cached. `0` still legitimately means "unlimited". |
| `GET /api/oauth2/client/infos` | `[]` always | reads the real `oauth2_client` table, same rows `GET /api/oauth2/client` already served |
| `GET /api/oauth2/config/template` | `[]` always | reads `oauth2_client_registration_template`, seeded at every boot by `internal/bootstrap.loadOAuth2Templates` |
| `GET /api/tenant/dashboard/home/info` | `{nil, true}` always, no DB read | reads `tenant.additional_info` |
| `POST /api/tenant/dashboard/home/info` | no method branch — silently discarded the selection | persists into `tenant.additional_info`, **merging** so it can't clobber other keys |
| `GET /api/dashboard/home` | empty `200` always | reads `tb_user.additional_info.homeDashboardId` and forwards to `ByID`; still empty when genuinely unconfigured |
| `GET /api/admin/featuresInfo` | `oauthEnabled: false` always | reflects real OIDC configuration |
| `POST /api/assets`, `POST /api/entityViews` | silently re-listed instead of creating | dispatch to `asset.Save` / `entityview.Save`, the real create handlers already wired at the singular routes |
| `PUT /api/image/import` | response omitted 8 fields it had already computed and written | returns the full shape, matching sibling `ImageUpload` |

Tests: `internal/system/{usage,oauth2_infos,tenant_dashboard_home,features_info}_test.go`,
`internal/dashboard/home_test.go`, `internal/resource/image_import_test.go`,
`assets_entityviews_post_test.go`. Most seed a real Postgres via
`FLOW_TEST_PG_DSN` (skipping when unset, the repo's existing convention) —
they were **not** run against the local operational Postgres, since they
`DROP TABLE`. `features_info_test.go` and the pass-1 tests need no DB and run
unconditionally.

The `/api/usage` test covers the quota fields only; `transportMessages`
resolves through `telemetry.DeviceKVLatest`, which needs a live GreptimeDB
connection, so its fallback-to-0 path is what the unit test exercises. Verify
that field end-to-end on a live cluster.

## Fixed: pass 3 — entity-query filters that silently returned nothing

`POST /api/entitiesQuery/find` dropped whole classes of query on the floor: an
unhandled entity type or filter name hit a `default` case that logged a
server-side `WARN` and answered an empty `200`. The caller saw "no results",
not "unsupported" — the two are indistinguishable to a UI widget.

| What | Was | Now |
|---|---|---|
| `resolveEntity` for `CUSTOMER` / `USER` / `ENTITY_VIEW` | `nil` — any `singleEntity`/`entityList` filter naming one was dropped | real tenant-scoped lookups against `customer` / `tb_user` / `entity_view`. `USER` names by full name, falling back to email, matching the user list's own precedence. |
| `entityName` filter | unhandled → empty | name-prefix search over a closed allow-list of entity types; an unsupported type is refused rather than guessed at |
| `entityViewType` filter | unhandled → empty | the `entity_view` equivalent of the existing `deviceType`/`assetType` filters |
| `deviceSearchQuery` / `assetSearchQuery` / `entityViewSearchQuery` | unhandled → empty | relation walk from the root via `topology.NeighborsTenant` — the same tenant-scoped primitive `relationsQuery` uses, so a foreign root yields nothing rather than leaking — then narrowed by the requested entity type and optional subtypes |
| `stateEntityOwner` filter | unhandled → empty | resolves the entity's owning customer, falling back to the tenant (including for TB's nil-UUID "no owner" sentinel, which is stored instead of NULL) |

Test: `internal/entityquery/entityquery_legacy_filters_test.go`. Unlike the
earlier passes, this one **was run against a real Postgres** — its harness
follows the existing `entityquery_relations_test.go` pattern of creating a
throwaway schema and dropping it with `CASCADE`, so it never touches real
tables. All 12 subtests pass, including the two cross-tenant isolation
assertions. Running it for real caught a genuine gap in the test schema
(`NeighborsTenant`'s union needs the profile tables to exist), which a
skipped test would have hidden.

## Priority backlog (confirmed, still open)

### P1 — a whole UI feature is non-functional

**Public sharing is broken end-to-end, and there is no real "Public"
customer to share to.** `POST /api/customer/public/{asset,dashboard,device,
entityView}/{id}` and `DELETE /api/customer/public/dashboard/{id}` always
`403`. The immediate cause: the closures at `api.go:636,644` pass the literal
path segment `"public"` as a customer ID straight into
`customer.HandleAssignToCustomer`/`HandleAssignDashboardToCustomer`
(`internal/customer/assign.go`), and `customerBelongsToTenant` only accepts a
real customer row or the TB nil-UUID sentinel
(`13814000-1dd2-11b2-8080-808080808080`, `internal/bootstrap.SystemTenantID`
— confirmed this is TB's generic "no owner" convention, *not* a
public-customer ID). The deeper cause, found while investigating a
substitution fix: **no code anywhere in this repo ever creates a
`title = 'Public'` customer row for a tenant.** Every reference to one
(`internal/system/missing_handlers.go`, `internal/ws/ws.go`) only *excludes*
`title != 'Public'` from listings, assuming the row exists. It doesn't. The
`customer.is_public` column real read-side code already checks
(`internal/dashboard/dashboard_customer.go:109`,
`internal/entityview/get.go:45`, `internal/device/device_handler.go:132`,
`internal/system/info_handlers.go`) has nothing to ever find `true` on. A
correct fix needs a real design decision (auto-create the Public customer row
at tenant-creation time, keyed how?) before any code changes — deliberately
not attempted in this pass.

### P2 — real backing data, never wired

Two remain; the rest were closed in passes 2 and 3 above.

- **Notification system is unwired end-to-end, not out-of-scope.** Real
  tables exist (`notification_target`, `notification_template`,
  `notification_request`, `notification` — `01_schema-entities.sql`), but
  `POST /api/notification/target`, `.../template`, `.../request`, and
  `PUT /api/notifications/read` all route through `saveJSONEntity`
  (`internal/system/feature_handlers.go:338`), which only echoes the posted
  JSON with a generated id — never an `INSERT`. The corresponding `GET`
  list/read endpoints correctly report empty because nothing was ever
  written; they are not separately broken, they are downstream of these four
  writers. `POST /api/notification/request/preview`'s
  `totalRecipientsCount: 0` is conditionally-correct for the same reason —
  recipient resolution over `notification_target.configuration` doesn't
  exist yet either.
- **`POST /api/widgetType`** (`internal/widget/widget.go:23`, `Type`) —
  never reads `r.Body` or checks `r.Method`; only GET query params are
  handled. Real `INSERT INTO widget_type` exists
  (`internal/bootstrap/bootstrap.go`) but only runs at boot/seed time.
  Custom widget authoring via the UI is non-functional.

### P3 — wrong or incomplete data on an otherwise-real path

- **`GET /api/alarm/{id}`** (`internal/tenant/tenant_handler.go:470`,
  `handleAlarmById`) — `originator.entityType` hardcoded `"DEVICE"` even
  though the real column + converter (`originatorTypeOrdinalToString`) are
  used one function away for the list query; mis-derives `"CLEARED_ACK"` for
  the cleared-but-unacked state (should be `CLEARED_UNACK`, per the same
  file's own 4-way logic at lines 317-324); `customerId`/`assigneeId`/
  `propagate*`/`ackTs`/`clearTs`/`assignTs`/`originatorName` are all computed
  by the sibling list query but never selected here.
- **`GET /api/alarms`, `GET /api/v2/alarms`** — `assignee` hardcoded `nil`
  even when `assigneeIdStr` is set and `user.FindByID` (already imported in
  the same file) could resolve it. `api.go`'s own comment documents
  `assignee` as v2's promised extra field over v1.
- **`POST /api/alarmsQuery/find`** — never reads `r.Body`; silently returns
  the full unfiltered tenant alarm page instead of the entity-scoped result
  the UI posts a filter for. The real filtering pattern already exists in
  `handleAlarmsByDevice` in the same file, just not reused here.
- **`GET /api/noauth/userPasswordPolicy`**
  (`internal/system/stubs_handlers.go:81`) — fixed TB defaults, correct only
  until an admin saves a custom policy via the real, configurable
  `admin_settings` row under key `securitySettings`
  (`internal/system/system_handler.go`), which this handler never reads.

### `needs-live-check` (undecidable statically)

- **`GET`/`POST /api/queues`** (`internal/system/stubs_handlers.go:369`) —
  GET is a real query; POST silently list-only, same as assets/entityViews
  above, but unclear whether TB's real "add queue" UI flow expects this to
  persist or whether queue creation was deliberately left DB-CRUD-free
  (queue/consumer config is normally Helm/k8s-owned on this platform, per
  root `CLAUDE.md`). Needs a decision, not just a query.
- **`POST /api/device/bulk_import`** — real per-row logic exists; only
  the created/updated/error *counts* in the response are unverified against
  seeded CSV rows.

## Confirmed out-of-scope (sample — not exhaustive)

The remainder of the 284 (roughly 90 entries) are deliberate, honest
non-answers, consistent with the same philosophy documented in
[UI Contract Coverage](UI_CONTRACT_COVERAGE.md)'s "answered honestly (12)"
section: 2FA, mobile QR, version-control/repository settings, Trendz
analytics, edge administration, mail transport, SMS transport, and dashboard
visit-tracking are all explicitly documented in code comments and/or
`docs/API_REFERENCE.md` as features this platform does not implement.
`GET /api/components` (rule-node catalogue) and the `connections` field of
rule-chain metadata are honest empties because flow-core ships no rule
engine (alarm detection runs in Bento/NATS instead, per `api.go`'s own
comment). These were read and classified, not skipped — see the individual
batch transcripts referenced in ADR-0002 for the full per-entry list if
auditing this document's completeness.

## Re-running

Data fidelity isn't (yet) part of the automated `ui-contract-check` — that
tool only asserts status/shape and is safe to run against a shared
pilot/production cluster for exactly that reason (fixed placeholder UUID, no
seeding). The P1–P3 findings above were confirmed by direct source reading
against the running code at commit time; re-verifying after future changes
means re-reading the cited handlers, or (preferred, going forward) adding a
seeded `httptest` case per finding the way this pass's four fixes did.
