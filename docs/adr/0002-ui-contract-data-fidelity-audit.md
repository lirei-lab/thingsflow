# ADR 0002 — TB UI contract data-fidelity audit

- **Status:** Proposed
- **Date:** 2026-08-19
- **Context owners:** flow-core (UI/API contract)
- **Supersedes / relates to:** `docs/UI_CONTRACT_COVERAGE.md`, `flow-core/internal/uicontract/`, `flow-core/cmd/ui-contract-check`

## Context

`docs/UI_CONTRACT_COVERAGE.md` reports **0 gap** across the 373 endpoints the
real ThingsBoard UI calls (284 "routed" + 89 declared "no-goal"), verified by
`flow-core/cmd/ui-contract-check` against
`flow-core/internal/uicontract/testdata/tb-ui-4.3.1.1.json`. But that checker
(`internal/uicontract/contract.go` `ValidateBody`) only asserts HTTP status +
JSON shape + key **presence** (`RequiredKeys`) — never field **values**.
"Routed" means *reaches a real handler*, not *returns correct or complete
data*. That gap is real and provable: `/api/usage`
(`internal/system/missing_handlers.go:564-601`, `HandleUsage`) is "routed,"
returns 200, has the right shape — and silently returns `transportMessages: 0`
even though a real per-tenant counter for exactly that number already exists
in `internal/usage/usage.go` and is never read by this handler.

Pulling on that one thread during planning surfaced that `/api/usage` isn't
one bug, it's three different dispositions wearing the same `0` literal:

1. **Real gap.** `transportMessages` — `usage.go`'s live atomic counter (and
   its `ts_kv` snapshot) exists and is simply never queried by `HandleUsage`.
2. **Unwired-but-conditionally-correct.** `maxDevices`/`maxAssets`/`maxUsers`/
   `maxCustomers`/`maxDashboards` — `internal/quotas/quotas.go`'s
   `LimitsFor(tenantId)` reads real per-tenant limits, `0` legitimately means
   *unlimited* (TB-classic convention, documented in the struct comment) — so
   wiring this in means `0` will *often still be the right answer*. The bug
   is "never checks," not "always wrong."
3. **Correctly out of scope.** `jsExecutions`/`tbelExecutions`/`emails`/`sms`
   — `usage.go` already hardcodes these to 0 with an explicit comment that
   they're out of scope, matching the same "answered honestly" philosophy
   `UI_CONTRACT_COVERAGE.md` documents for its 12 endpoints that return `501`
   rather than fake a `200`. `edges` is the same story — edge is a declared
   no-goal domain. These are not bugs to fix.

So the goal of this work is not "find and fix every 0" — a naive sweep would
misclassify (2) and try to fabricate data for (3). It's: build a second,
orthogonal axis of rigor (data fidelity) on top of the existing routing-
coverage framework, correctly triage each finding into one of those three
dispositions, and only remediate the genuinely real gaps — starting with
`/api/usage`'s `transportMessages`.

## Decision

Four phases. 1 and 2 are read-only audit/tooling and can run to completion
in one pass across all 284 routed entries. 3 and 4 (writing tests, fixing
code) are scoped to whatever Phase 2 actually flags — expected to be well
under 284, based on how concentrated the risk signal already looks.

### Phase 1 — path → handler map

**1a (scripted, exhaustive).** A `go/ast` walk over `registerRoutes` in
`api.go` (routes are registered via `mux.HandleFunc("PATH", cors(...,
pkg.HandleX))` triples — regular enough to walk mechanically, not regex:
multi-line registrations and closures make text matching unreliable).
Produces, for all 284 routed manifest entries, the registered top-level
handler symbol and its file:line. One-off script, not committed — this is
tooling to drive Phase 2, not a shipped artifact.

**1b (targeted trace, ~≤50 entries).** Several registered symbols are
subtree routers that dispatch internally by string-matching
`r.URL.Path`/`r.Method` rather than by `net/http` routing — confirmed in
`internal/tenant/tenant_handler.go`'s `HandleAlarmRest`, which alone fans out
to `HandleAlarmsQueryFind`/`handleAlarmsByDevice`/`handleAlarmAck`/
`handleAlarmClear`/`handleAlarmById`, collapsing multiple manifest `alarm`-area
entries onto one registration line. `UI_CONTRACT_COVERAGE.md` already names
this exact shape as the root cause of most of the original 34 routing gaps —
it is not a corner case here either. 1a's output flags every symbol that
manifest-maps to more than one entry; each gets a short manual read to
resolve to its real sub-handler before Phase 2 reads it.

### Phase 2 — two-tier data-fidelity sweep, all 284 routed entries

**Tier 0 (scriptable, exhaustive, cheap).** Per entry, compute cheap
structural signals without deep reading: (a) does the handler live in
`internal/system/missing_handlers.go` — confirmed 1,117 lines / 22 handler
functions (`HandleUsage`, `HandleRuleChains`, `HandleTenantAssets`,
`HandleAlarmTypes`, `HandleEntitiesQueryCount`, …) with **zero** dedicated
test files anywhere in the repo, versus domain packages
(`internal/device`, `internal/asset`, `internal/customer`, `internal/dashboard`,
`internal/tenant`, `internal/user`) that each carry 1-4 `_test.go` files; (b)
does the response-building code mix a literal `0`/`nil`/`[]` into a
`map[string]interface{}` or struct literal alongside at least one real
DB/twinstore/telemetry query in the same function — the `/api/usage` smell,
grep-able; (c) does a plausibly-matching real subsystem package exist for a
zeroed field (derive a keyword from the field name — `quota`, `usage`,
`alarm`, etc. — and grep the repo for a package that could back it, the way
`internal/quotas` backs `maxDevices`); (d) cross-reference the entry's `area`
against `nogoals_gen.go`/the 12 documented `501` "answered honestly" endpoints
— a literal that already matches a documented deliberate non-implementation
is not a finding.

This alone classifies all 284 without deep-reading most of them. All 22
`missing_handlers.go` functions are Tier-1 by default (zero test coverage is
itself the signal); everything else only gets a full read if Tier-0 flags it.

**Tier 1 (targeted read).** For every entry Tier-0 flags: read the handler in
full, trace whether the zeroed/stubbed field has a real backing subsystem,
and assign one of three dispositions (matching the `/api/usage` breakdown
above, not a flat stub/no-stub binary):

- `confirmed-gap` — real subsystem exists, handler never queries it.
- `confirmed-gap-conditional` — real subsystem exists, but the zero value can
  be legitimately correct in some states (e.g. quota `0` = unlimited); the
  bug is "never checks," verification must not treat `0` post-fix as
  "still broken."
- `out-of-scope` — the zero is already a deliberate, documented non-answer
  (mirrors `UI_CONTRACT_COVERAGE.md`'s "answered honestly" category). Not a
  finding to remediate.

Execute Tier 1 as a handful of read-only Agent calls batched by `area` group
(the manifest's 70+ areas cluster naturally into domain-package batches),
reporting back file:line evidence and disposition per flagged entry.

### Phase 3 — live value-verification tests, `confirmed-gap*` entries only

Go tests, not an extension to `ui-contract-check`. The contract runner is
explicitly designed to run safely against a live, already-populated
pilot/production cluster (fixed placeholder UUID, no seeding phase, kept
deliberately declarative with no handler reference at all) — value-fidelity
checks need known seeded state first, which can't be done safely against
shared/prod data without either polluting it or being at the mercy of
whatever's already there.

Model on `flow-core/attributes_roundtrip_test.go`'s pattern: `httptest`
against the real handler, real Postgres via `FLOW_TEST_PG_DSN` (skip if
unset), production wiring, assert actual returned values against seeded
reality. But factor a **shared minimal-schema seed helper** (baseline
tenant/user/device/customer/asset) once rather than each Phase-3 test
duplicating `attributes_roundtrip_test.go`'s from-scratch
`DROP TABLE`/`CREATE TABLE` block — there will likely be more than one of
these tests and they shouldn't diverge on boilerplate.

Each test: seed the specific real counter/limit the finding is about, hit
the handler, assert the field reflects it (proves a real gap) — or seed
nothing unusual and confirm the field stays a legitimate default (reclassifies
a Tier-1 `confirmed-gap*` down to `out-of-scope` if the static read was wrong).

### Phase 4 — report + remediate `confirmed-gap*` only

New doc `docs/UI_CONTRACT_DATA_FIDELITY.md`, same rigor/style as
`UI_CONTRACT_COVERAGE.md`: full list of the 284 routed entries' dispositions,
with the confirmed real gaps and their fixes named explicitly, and the
`out-of-scope` ones documented with the same "answered honestly" reasoning
already established for the 12 `501`s — so this doesn't silently regress into
"fixed" fabricated data later.

Remediate each `confirmed-gap`/`confirmed-gap-conditional` by wiring the
handler to its real subsystem, verified by the matching Phase-3 test flipping
from failing to passing. For `/api/usage`'s `transportMessages` specifically:
`usage.go`'s live counter is an **in-memory, per-process atomic** (`byTenant`
map) — correct only for a single flow-core replica; the `ts_kv` snapshot
`writeSnapshot()` persists once a minute is cross-instance-correct but can lag
up to 60s. This is a real design choice, not a detail to skip past — pick one
deliberately (recommend the `ts_kv` snapshot for `HandleUsage`, since it's
already the multi-replica-safe source other read paths use, and note the
staleness bound in a comment) and have the Phase-3 test assert against that
choice, not raw immediacy. For quota fields, reuse `quotas.LimitsFor`'s
existing TTL cache rather than adding a new unbounded per-request DB round
trip.

## Verification

- `go build ./... && go vet ./...` and the full `go test ./...` suite stay
  green throughout (existing convention, enforced in CI).
- Each Phase-3 test is a concrete, runnable proof per finding: red before the
  Phase-4 fix, green after.
- Re-run `go run ./cmd/ui-contract-check -base-url http://<flow-core>
  -contract internal/uicontract/testdata/tb-ui-4.3.1.1.json` after Phase 4 —
  routing coverage must stay at 0 gap (this work adds a fidelity axis, it
  must not regress the existing one).
- `docs/UI_CONTRACT_DATA_FIDELITY.md` is the audit's durable record — every
  one of the 284 routed entries has an explicit disposition, not just the
  ones that turned out to be findings, so "unaudited" is never confusable
  with "verified clean."
