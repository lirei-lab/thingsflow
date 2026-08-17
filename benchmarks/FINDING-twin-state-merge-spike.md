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
| B | A raw `$KV.<bucket>.<key>` publish with a STALE `Nats-Expected-Last-Subject-Sequence` is rejected by the KV backing stream? | **PASS** | Stale expected-seq publish rejected; readback stayed at the base value; also `verify-kv-doc-publish-semantics.sh` CASREJECT=PASS |
| C | A `nats_jetstream` INPUT on the KV backing stream exposes subject/stream sequence (the only other config-only source)? | **FAIL** | Correct `$KV.twin_state.`-prefixed subject, message delivered (`num_delivered=1`) but `nats_subject_sequence`/`nats_sequence` both `"none"` |

**Missing primitive: no revision/sequence is exposed by Bento 1.8.1 from either the `nats_kv` cache get or a `nats_jetstream` input.** The `Nats-Expected-Last-Subject-Sequence` header therefore cannot be sourced config-only, so the optimistic-CAS merge loop (read → merge → CAS write → retry-on-conflict) cannot be implemented in pure Bento config.

**Consequence:** the **Balanced approach is INFEASIBLE as a config-only change** — the design doc's highest-priority open question (#1) is answered with evidence. The one-document-per-device model is not dead as a concept; it is dead as a *pure-Bento-config write path*. It would need either a non-Bento writer (breaks the config-only constraint → explicit re-scope) or a Bento version with revision exposure.

**Important nuance:** CAS *enforcement* works (probe B + CASREJECT). Only the config-only *revision source* is missing. The enforcement side is sound and reusable for any future design.

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

1. **Flip to Conservative (pre-split upstream)** — the evidence-supported fallback. Requires the fair cage applied (an operator-authorized `helm upgrade thingsflow` to `fair-thingsflow.yaml` topology, or a decision that the 1× cage is acceptable for a first-look), the second stack scaled to zero, then a re-run of Plan 01-02. Milestone 2's output-stage isolation measured pre-split at 7,700–10,400 msg/s — the end-to-end fair-ramp would confirm or refute ≥8,000 msg/s.
2. **Re-scope the constraint** — allow a non-Bento writer (flow-core already has the correct CAS `MergeTelemetry` loop in `twinstore/nats.go`, but putting it in the hot path violates `CLAUDE.md`'s anti-pattern). Requires `/legion:plan` rework of Phase 1 and an explicit constraint decision.
3. **Close/archive Milestone 4** — the cause is localized with direct evidence (config-only CAS infeasible), consistent with how Milestone 2 closed. The one-document-per-device model stays parked.

## Files / artifacts
- `benchmarks/twin-state-localization/cas-feasibility-probe.sh` — the probe (Half A + Half B)
- `benchmarks/twin-state-localization/configs/cas-probe-get.yaml` — probe config (Bento 1.8.1 `cache_resources:` schema)
- `benchmarks/twin-state-localization/verify-kv-doc-publish-semantics.sh` — 4-check verifier (READBACK/WATCHER/WRITETWICE/CASREJECT)
- `benchmarks/twin-state-localization/run-merge-spike.sh` — orchestrator (records `MERGE_WRITER=INFEASIBLE`)
- This finding (`FINDING-twin-state-merge-spike.md`) — new, standalone; **the three existing FINDING files are byte-identical** (unmodified)

## Preservation / hygiene
- Original `benchmarks/FINDING-twin-state.md` and the two other existing findings: **byte-identical** (`git diff --quiet` exit 0).
- Production files: byte-identical (`bento-nats-latest-kv.yaml`, `values.yaml`, `templates/`, `flow-core/`).
- No orphaned diagnostic pods/ConfigMaps/consumers in `thingsflow-fresh`; the KV_twin_state probe consumers (`thingsflow-kv-rev-probe*`) were cleaned up; only the pre-existing `5Lh9qTAQ` remains.
- No throughput number was fabricated or inherited: Milestone 2's pre-split 7,700–10,400 msg/s figure is cited only as prior evidence, NOT as this run's measurement.
