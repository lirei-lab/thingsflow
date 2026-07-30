# ThingsBoard UI contract coverage

Every endpoint the ThingsBoard UI calls, and what this platform does with it.

## Where the list comes from

The catalogue is extracted from the JavaScript bundle of the
`thingsboard/tb-web-ui:4.3.1.1` image this platform actually deploys — the verb
and path of every `http.get/post/put/delete` call — rather than from a spec.
It is what the UI requests, not what a document says it might.

## How coverage was decided

Not by status code. At the time of the audit, flow-core answered an
unimplemented GET with an empty `PageData` and HTTP 200, so a 200 proved
nothing on a read path; several endpoints were believed working for exactly
that reason. The verdict came from the server's own `not_implemented` log
line. (That forgiving response now survives only for declared no-goals — see
below.)

Path parameters use a fixed placeholder UUID, so a run is reproducible and
mutating calls cannot touch real objects.

## Result

| | count | |
|---|---|---|
| **Routed** | 284 | reaches a real handler |
| **Declared no-goal** | 89 | edge, rule-chain editor, mobile, AI models, API keys, domains, version control, lwm2m, queues — out of scope in [API_REFERENCE.md](API_REFERENCE.md) |
| **Gap** | 0 | |

373 endpoints, verified identical on the pilot cluster and in production.

The contract is enforced at runtime, not only at audit time. The server's
`/api/` catch-all carries a generated copy of the 89 no-goal entries
(`flow-core/nogoals_gen.go`, drift-checked against this contract by test):
declared no-goals keep the forgiving empty-page response so their screens
render, while any *undeclared* path answers a 404 envelope with a distinct
`not_routed_undeclared` log tag. The silent empty-200 that once hid 34
unimplemented endpoints is structurally impossible for new paths — an
endpoint is implemented, declared out of scope, or loudly missing.

## What was closed, and how

An earlier pass reported 49 gaps. Fifteen of those were misclassified: version
control (`/api/entities/vc/*`, `autoCommitSettings`), lwm2m and mail OAuth2 are
declared no-goals, and the area heuristic filed them under `entities`, `admin`
and `resource`. The genuine figure was 34, and all 34 are now answered.

**Implemented (22).** Most shared one shape: a subtree router that handled a
single verb and let every other method fall through, so `GET` on entity view,
resource, asset profile and dashboard info rendered a blank page and reported
nothing. Also added: resource listing by tenant, customer asset/entity-view/edge
sub-listings, tenant dashboards and users, bulk dashboard-to-customer
assignment, default profile promotion, and asset CSV import.

Two carry constraints worth knowing. Default profile promotion runs in one
transaction — between clearing the old default and setting the new one the
tenant would have none, and provisioning in that window fails to resolve a
profile. The tenant sub-listings require the caller to be a sysadmin or asking
about its own tenant; without that any authenticated tenant could enumerate
another's dashboards and user addresses.

**Answered honestly (12).** These cannot be implemented on this platform, and
return `501` rather than a misleading `200`:

- user activation and 2FA code delivery need an outbound mail transport that
  does not exist here. Activation still mints and returns a real link so an
  operator can deliver it out of band — what it will not do is report an
  invitation as sent when nothing was;
- user impersonation and public dashboard login would mint credentials whose
  scope is undefined here;
- persistent RPC has no queue, so `404` is truthful for any id. An empty `200`
  would render as command history that was recorded and then lost.

**Telemetry deletion** is implemented but off unless
`TELEMETRY_DELETE_ENABLED=true`. It is the only path that removes stored
measurements and it is one button away in the UI. It
also refuses the unscoped "all keys, all time" form the UI offers, requires an
explicit range, and logs every deletion with user, keys, range and row count.

## Re-running it

```bash
go run ./cmd/ui-contract-check \
  -base-url http://<flow-core> \
  -contract internal/uicontract/testdata/tb-ui-4.3.1.1.json
```

Exits non-zero on any drift. Implementing a gap changes its status, so the
contract has to be regenerated — that edit is the record that coverage moved.
Bumping the UI image means regenerating from the new bundle.
