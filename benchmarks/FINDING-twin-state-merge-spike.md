# Merge-spike (one-document-per-device) — verdict: BLOCKED on environment, with the gate question answered

**Discovered:** 2026-08-17, Milestone 4 Phase 1 (Plans 01-01 + 01-02), test cluster `thingsflow-fresh` (kubectl --context=microk8s, server 172.16.128.7:16443).
**Status:** **BLOCKED for measurement — with the decisive gate question answered by evidence.**
**Scope:** The one-document-per-device twin-state merge write path (Milestone 4's Balanced approach).

---

## The gate question, answered

**Q: Is the config-only CAS merge write path (Bento `branch` + `nats_kv` cache get → merge → CAS publish + retry) buildable and race-safe?**

**A: NO — it is not buildable in Bento 1.8.1.** Three live probes on the test cluster:

| # | Probe | Result | Evidence |
|---|-------|--------|----------|
| A | `nats_kv` cache get exposes the KV entry's revision/sequence (the CAS header source)? | **FAIL** | `benchmarks/twin-state-localization/configs/cas-probe-get.yaml` returned only value bytes `{"schema":"probe","ts":1,"value":"seed"}` — no revision/sequence field in the emitted result |
| B | A raw `$KV.<bucket>.<key>` publish with a STALE `Nats-Expected-Last-Subject-Sequence` is rejected by the KV backing stream? | **UNKNOWN (probe) / PASS (verifier)** | Probe: the fire-and-forget `nats pub` returns no explicit rejection signal, so the probe now honestly reports UNKNOWN (review fix 2026-08-17 — no inferential PASS). Corroboration: `verify-kv-doc-publish-semantics.sh` CASREJECT=PASS (settle-delayed readback, committed artifact) — the authoritative CAS-enforcement proof. |
| C | A `nats_jetstream` INPUT on the KV backing stream exposes subject/stream sequence (the only other config-only source)? | **FAIL** | `configs/cas-probe-jsinput.yaml` (committed 2026-08-17), correct `$KV.twin_state.`-prefixed subject, message delivered (`num_delivered=1`) but `nats_subject_sequence`/`nats_sequence` both `"none"` |

**Missing primitive: no revision/sequence is exposed by Bento 1.8.1 from either the `nats_kv` cache get or a `nats_jetstream` input.** The `Nats-Expected-Last-Subject-Sequence` header therefore cannot be sourced config-only, so the optimistic-CAS merge loop (read → merge → CAS write → retry-on-conflict) cannot be implemented in pure Bento config.

**Consequence:** the **Balanced approach is INFEASIBLE as a config-only change** — the design doc's highest-priority open question (#1) is answered with evidence. The one-document-per-device model is not dead as a concept; it is dead as a *pure-Bento-config write path*. It would need either a non-Bento writer (breaks the config-only constraint → explicit re-scope) or a Bento version with revision exposure.

**Important nuance:** CAS *enforcement* works — `verify-kv-doc-publish-semantics.sh` CASREJECT=PASS (settle-delayed readback) is the authoritative proof. Only the config-only *revision source* is missing. The enforcement side is sound and reusable for any future design.

---

## The measurement: BLOCKED (environment unfit — no number published)

Plan 01-02 re-scopes to measure the Conservative pre-split control end-to-end (since 01-01 was INFEASIBLE). **The fair-ramp was NOT run**, per the plan's own stop-gates, because the environment fails three hard gates:

### Gate 1 — Cage conformance: FAIL (measured, not assumed)
The live release (`thingsflow` in `thingsflow-fresh`, **revision 7**) is at `values.yaml` topology, NOT the fair cage `benchmarks/profiles/fair-thingsflow.yaml` that the original finding's levels are comparable to:

| Component | Live (rev 7) | fair-thingsflow.yaml | Match |
|-----------|--------------|----------------------|-------|
| latestKv | **1** replica, 3000m | **3** replicas | ❌ |
| http-ingest | **1** replica, 500m | **3** replicas, 3000m | ❌ |
| nats | **1** replica, 2000m | 6000m | ❌ |
| rmqtt-edge | **1** replica | 3-node raft STS | ❌ |

Applying the fair cage requires `helm upgrade thingsflow` — **explicitly forbidden** by Plan 01-02 ("do NOT touch the production Helm release topology"). A fair-ramp on the current 1× cage would produce a number **uninterpretable** against the original finding's levels for BOTH the Balanced and Conservative worlds — the exact "cage deviation → do not run contaminated" case the plan names. Not run.

### Gate 2 — SUT isolation: FAIL
A second full ThingsFlow stack runs in namespace `thingsflow` (10d old, 5+ workloads at 1/1: alarm-materializer, flow-core, http-ingest, nats-alarms, nats-entity-greptimedb, …). Scaling it to zero is outside Plan 01-02's allowed actions (its scope is `thingsflow-fresh` only) and carries a restore obligation. Not isolated → the merge-consumer measurement would be contaminated by a parallel platform. Not run.

### Gate 3 — Machine quiet: WARNING (moderate)
Load average 3.81 (1.45-min), one python process at ~41% CPU (transient — `ps` lookup for its PID returned empty on re-check). Moderate, not the catastrophic 13.7/16 of Milestone 2 Fase 3 attempt 1, but combined with gates 1–2 the run is already blocked; no further action needed.

---

## Verdict

```
**Verdict**: BLOCKED — environment unfit for a fair-ramp (cage deviation vs fair-thingsflow.yaml,
second stack not isolated, both per Plan 01-02's stop-gates; applying the cage requires a
forbidden helm upgrade). No measurement was run and NO number is published for the Conservative
pre-split control in this run.
```

**Separately, the gate question IS answered:** the config-only CAS merge writer (Balanced approach) is **INFEASIBLE** — see the table above. This is the milestone's substantive finding.

---

## What this means / options (for the operator)

1. **Flip to Conservative (pre-split upstream)** — the evidence-supported fallback. Requires the fair cage applied (an operator-authorized `helm upgrade thingsflow` to `fair-thingsflow.yaml` topology, or a decision that the 1× cage is acceptable for a first-look), the second stack scaled to zero, then a re-run of Plan 01-02. Milestone 2's pre-split control (the full latest-KV consumer pipeline with the `unarchive` fan-out removed, ingest bypassed, synthetic-publisher-limited — NOT the rung1 output-stage +20.8% micro-bench) measured **~7,272–7,742 msg/s across two independently filed runs** (`FINDING-twin-state-localization.md`). Note: the earlier 10,212.8 msg/s run B is **superseded and should not be cited** — so the verified pre-split evidence tops out below the ≥8,000 msg/s gate; the end-to-end fair-ramp would confirm or refute whether the Conservative flip reaches the target.
   **Before the re-run, measurement tooling must be authored**: the candidate's own consumer delivered/ack rate via the monitoring port (18222:8222) and a key-reconciliation check adapted to the pre-split per-key shape (the merge-writer's `reconcile-merge-doc.sh` does not fit the Conservative control). This is required by Plan 01-02's attribution-correct design (review finding 2026-08-17).
2. **Re-scope the constraint** — allow a non-Bento writer (flow-core already has the correct CAS `MergeTelemetry` loop in `twinstore/nats.go`, but putting it in the hot path violates `CLAUDE.md`'s anti-pattern). Requires `/legion:plan` rework of Phase 1 and an explicit constraint decision.
3. **Close/archive Milestone 4** — the cause is localized with direct evidence (config-only CAS infeasible), consistent with how Milestone 2 closed. The one-document-per-device model stays parked.

---

## Option A (2026-08-17) — single-writer merge: FEASIBLE (revises the gate answer)

**This section REVISES the "Consequence" above.** The gate answer stands for the **CAS variant** (optimistic read-modify-write with `Nats-Expected-Last-Subject-Sequence`), but a **different config-only write path is buildable and race-safe**: the **single-writer (single serialized consumer) merge**. Verified live on the test cluster with **2 consecutive PASS runs** by `benchmarks/twin-state-localization/single-writer-merge-probe.sh`.

### The mechanism (no CAS needed — remove the concurrent writer instead of coordinating it)
A single JetStream consumer (single-replica pod + input `max_ack_pending:1` + consumer `--max-pending 1`) makes the server deliver **one message at a time**; the pipeline (`cache get` → Bloblang merge → `cache set`) runs to completion and acks before the next delivery. With exactly one writer on the doc key, the read-modify-write is atomic — **no revision/sequence primitive is required**. The one-document-per-device model is therefore NOT dead as a config-only write path; only the optimistic-CAS version is.

### Evidence (2/2 runs, both EXIT=0)
- `MERGE_WRITER_SINGLE=FEASIBLE` — config-only, no custom Go, stock Bento 1.8.1
- `RACE_SAFE=PASS` — 200 concurrent messages for ONE device (20 keys × 10 updates, increasing ts): **20/20 keys present, 0 lost**, every key converges to its highest-ts value (LWW correct regardless of delivery order)
- `STALE_DROP=PASS` — a deterministic stale write (ts=1) is dropped by the per-key LWW merge
- `ROUNDTRIP=PASS` — attributes/schema/tenantId/entityId carried forward untouched (full-doc round-trip)
- `UPDATEDTS=PASS` — updatedTs == max ts (mirrors flow-core `merge()`)
- Merge semantics verified identical to flow-core `twinstore/nats.go` `merge()`: LWW strictly per key by ts; keys absent from the batch preserved (`deleted()` map_each); disjoint-map `merge()` (Bento's `merge()` is DEEP — concatenates on conflict, so only disjoint maps are combined)

### Bento 1.8.1 gotchas discovered live (all verified on-cluster)
`try`/`catch` are SEPARATE processors (the `try` type has no `catch` field); `max_in_flight` is NOT a `nats_jetstream` INPUT field (use `max_ack_pending`); `consumer add` requires `--target`+`--deliver-group` (nats-box 0.16.0 non-interactive); `bind:true` validates the deliver policy matches the consumer (`deliver: new` must equal `--deliver new`); `meta()` returns BYTES (`.int()`/`.parse_int()` unreliable — `.string()` serializes objects to JSON, `.parse_json()` works on bytes); lambda bodies must be single-line; `fold` is unusable (`$acc` undefined at runtime) — use `map_each`+`deleted()`+disjoint `merge()`; `merge()` is deep; `.contains()` on an object returns FALSE (use `.exists()`); `cache set` REQUIRES an explicit `value:` field (writes 0 bytes otherwise); `kv get --raw` emits no trailing newline so `kubectl run --rm` appends `pod deleted` on the SAME line (extract with `grep -o '^{.*}'`); `printf | python3 - <<heredoc` lets the heredoc override the pipe (pass the doc as ARGV).

### Throughput caveat — the ≥8,000 msg/s target is NOT proven by this probe
The probe proves **correctness and race-safety**, not throughput. A serialized single consumer caps per-device merge rate (one get→merge→set round-trip per message). The design doc's "Later" scope — **sharded merge consumers** (per-device-hash partition, one ordered consumer per shard) — is what would scale toward the target; each shard keeps the single-writer safety property. No fair-ramp measurement was run (the same environment gates from Plan 01-02 apply: cage deviation, second stack not isolated). So: **Balanced re-opened as CORRECT; the throughput question is deferred to a sharded-consumer measurement (Phase 3) or an operator decision.**

### What this changes for the operator
- Option 2 (re-scope to non-Bento writer) is **no longer required** for a config-only merge — the single-writer variant keeps the config-only constraint.
- The Balanced path is re-opened: Phase 2 (read-contract unification, doc-first reads) and Phase 3 (flip + verify + pin) are back on the table, with the throughput/sharding question the key open item.
- Artifacts: `benchmarks/twin-state-localization/single-writer-merge-probe.sh` + `configs/merge-doc-writer-single.yaml` (new); production files + the three other findings remain byte-identical; `TF_MERGE_DIAG` stream and the diag doc key cleaned up, no orphans.

## Known risk (accepted, 2026-08-19) — second writer races this design

Once this single-writer merge ships to production (Phase 3), it becomes a **second, independent writer** on the same doc key (`DEVICE.<tenant>.<device>`) alongside flow-core's own writer: `flow-core/internal/twinstore/nats.go` `MergeTelemetry` (called from `POST .../timeseries`, `api.go:1362`) and `MergeAttributes` (called from `internal/tenant/attributes.go:37`) both already do a real CAS loop (`kv.Get` → apply → `kv.Update(key, payload, entry.Revision())`, retrying on conflict) directly against the KV — pre-existing code, shipped since the initial public release, unrelated to this milestone. This writer is **not part of the device-telemetry ingest hot path** (that's Bento → HTTP Line Protocol → GreptimeDB, entirely outside flow-core's process); it's a separate, comparatively low-frequency REST path.

The single-writer consumer above does an **unconditional `cache set`** (no revision check — that's the whole point, it trades CAS for serialization). If flow-core's CAS write lands between the consumer's `get` and `set` on the same doc key, the consumer's write silently clobbers it (lost update). Today this race **cannot occur**: production's live writer (`k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml`) still writes per-key entries (`DEVICE.<tenant>.<device>.telemetry.<key>`), a disjoint key space from flow-core's whole-doc key. The race only becomes possible once Phase 3 productionizes this doc-merge design.

**Investigated and rejected for now**: routing flow-core's two write call sites through the same NATS subject/consumer this single-writer owns (publish-and-let-Bento-merge, instead of direct CAS write), which would close the race without adding a CAS primitive to Bento. Found to be materially bigger than it looks, not a contained swap:
- flow-core has no NATS publish handle today (`internal/twinstore` only opens a KV client) — needs new plumbing, mirroring the separate connection `internal/twinevents` already keeps for its own journal.
- Changes the `POST .../timeseries` response contract: today it blocks until the CAS write durably lands (`200`) or fails (`502`); publish-and-return would make `200` mean "accepted," not "applied" — a real behavior change for API callers, not an internal implementation detail.
- No Bento equivalent exists for `MergeAttributes` — the spike config here only merges the flat `Telemetry` map and carries `Attributes` forward untouched; attributes are scope-nested (CLIENT/SHARED/SERVER) and would need net-new pipeline design, not an extension of this work.
- Would silently drop the implicit read-your-writes guarantee the synchronous CAS path gives today (save-attribute-then-immediately-reread UI flows), which is not contract-tested today but is structurally true.

**Decision**: accept the race as a documented, low-frequency risk rather than add new Go to the write path (the project's stated direction is *removing* custom Go from that path, not adding coordination logic to it — see root `CLAUDE.md`). REST-triggered telemetry/attribute writes are comparatively rare next to device-ingest volume, and `SaveAttributesKV` already treats its KV write as best-effort (a failure there is just logged, not surfaced — `attributes.go:36-39`), so this is consistent with the existing tolerance for that path, not a new weakening. Revisit if production evidence shows actual data loss, not on theoretical grounds alone.

## Files / artifacts
- `benchmarks/twin-state-localization/cas-feasibility-probe.sh` — the probe (Half A + Half B)
- `benchmarks/twin-state-localization/configs/cas-probe-get.yaml` — probe config (Bento 1.8.1 `cache_resources:` schema)
- `benchmarks/twin-state-localization/configs/cas-probe-jsinput.yaml` — Half-C JS-input metadata probe config (committed 2026-08-17 so the "no sequence from JS input" claim is reproducible)
- `benchmarks/twin-state-localization/verify-kv-doc-publish-semantics.sh` — 4-check verifier (READBACK/WATCHER/WRITETWICE/CASREJECT)
- `benchmarks/twin-state-localization/run-merge-spike.sh` — orchestrator (records `MERGE_WRITER=INFEASIBLE`)
- This finding (`FINDING-twin-state-merge-spike.md`) — new, standalone; **the three existing FINDING files are byte-identical** (unmodified)

## Preservation / hygiene
- Original `benchmarks/FINDING-twin-state.md` and the two other existing findings: **byte-identical** (`git diff --quiet` exit 0).
- Production files: byte-identical (`bento-nats-latest-kv.yaml`, `values.yaml`, `templates/`, `flow-core/`).
- No orphaned diagnostic pods/ConfigMaps/consumers in `thingsflow-fresh`; the KV_twin_state probe consumers (`thingsflow-kv-rev-probe*`) were cleaned up; only the pre-existing `5Lh9qTAQ` remains.
- No throughput number was fabricated or inherited: the verified pre-split figure (~7,272–7,742 msg/s; the 10,212.8 run B superseded and not cited) is presented only as prior evidence, NOT as this run's measurement.
