# Rung-1 bypass-ingest verification: INCONCLUSIVE — the publisher's own ceiling, not either output mechanism, bounded this run

**Discovered:** 2026-08-13, Milestone 2 Phase 2, Plan 02-01
**Status:** measured, single clean run, **verdict INCONCLUSIVE** · extends
`benchmarks/FINDING-twin-state-localization.md` (unmodified) — resolves that
finding's stated Phase 2 next step (a bypass-ingest re-run of the
`nats_kv` vs `nats_jetstream` output comparison) mechanically, but the fresh
data does not actually discriminate between the two output mechanisms; a
different confound (the synthetic publisher's own fixed-pool ceiling) took its
place.

This finding is the output of `benchmarks/twin-state-localization/run-localization.sh`,
run live against the test cluster (`kubectl --context=microk8s -n thingsflow-fresh`)
scoped via `VARIANTS="pre-split pre-split-jetstream-output"`. It does not modify
`k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml`, any file under
`k8s/helm/thingsflow/templates/`, `values.yaml`, `flow-core/internal/twinstore`, or
either prior finding document (`benchmarks/FINDING-twin-state.md`,
`benchmarks/FINDING-twin-state-localization.md`).

## What this resolves from Phase 1's finding, and what it does not

Phase 1's finding (`benchmarks/FINDING-twin-state-localization.md`) flagged that its
own `jetstream-output` variant (the rung-1 candidate: `output.nats_kv` →
`output.nats_jetstream` publishing to `$KV.<bucket>.<key>`) could not be measured at
interesting load — it shared the production HTTP-ingest path with `drop-output`, and
all 4 filed runs came back `CLIENT WAS THE BOTTLENECK` at 430-700 msg/s, nowhere near
the ~3,900-8,000 msg/s regime where the original congestion collapse was observed.
Its recommendation for Phase 2 was to re-run the output-mechanism comparison using a
bypass-ingest synthetic publisher analogous to `publish-presplit.sh`, sidestepping the
HTTP-ingest confound the same way `pre-split` already does.

This plan did exactly that: `configs/pre-split-jetstream-output.yaml` reuses
`pre-split.yaml`'s exact bypass-ingest `http:`/`input:`/`pipeline:` blocks and
publisher (`publish-presplit.sh`, direct `nats pub` onto
`tf.ingest.http.raw.diag-presplit.>`), with ONLY the `output:` block swapped to
`jetstream-output.yaml`'s `nats_jetstream` candidate. The HTTP-ingest confound is
gone — neither run touched HTTP ingest at all.

**But a new confound took its place.** See "Why this is INCONCLUSIVE" below.

## Results (2026-08-13T020905Z, single run, both variants back-to-back in one session)

| variant | offered / achieved (publisher) | PROCESSED_RATE | pending at close | CPU (post-load snapshot) |
|---|---:|---:|---:|---|
| pre-split (nats_kv baseline) | 240,000 msgs / 31s = 7,741.9 msg/s | **7,741.9 msg/s** | 0 | `1m` CPU, 56Mi mem |
| pre-split-jetstream-output (nats_jetstream candidate) | 240,000 msgs / 31s = 7,741.9 msg/s | **7,741.9 msg/s** | 0 | `1m` CPU, 59Mi mem |

Both runs: `diag_consumer_pending_before=0`, `diag_consumer_pending_after=0`,
`production_consumer_pending_before=0`, `production_consumer_pending_after=0`. No
errors, no `DATAPLANE_UNHEALTHY_AFTER_LOAD`, no `BOOTSTRAP_FAILED`/`ROLLOUT_FAILED`.
`check_dataplane_health()` reported all 4 production durables Active both before the
run started and after each variant's load-drive step. `$KV.<bucket>.<key>`
publish-semantics check (`verify-kv-publish-semantics.sh`) passed all three checks
(READBACK, WATCHER, WRITETWICE) — a seventh consecutive PASS across this project's
diagnostic runs, further confirming rung 1's correctness precondition (unaffected by
today's throughput ambiguity).

Evidence: `.results/pre-split-20260813T020905Z.txt`,
`.results/pre-split-20260813T020905Z-load.json`,
`.results/pre-split-jetstream-output-20260813T020905Z.txt`,
`.results/pre-split-jetstream-output-20260813T020905Z-load.json`,
`.results/kv-semantics-20260813T020905Z.txt` (all untracked, per
`benchmarks/README.md`'s artifact policy — raw per-run results are not committed).

Cluster sweep after the run confirmed zero orphaned diagnostic resources:
`kubectl --context=microk8s -n thingsflow-fresh get deploy,cm -l app=twin-state-diag`
returned empty, and `nats consumer ls TF_RAW` listed only the 3 production durables
that share that stream (`thingsflow-alarms-durable`, `thingsflow-greptimedb-durable`,
`thingsflow-latest-kv-durable`) — both diagnostic consumers
(`thingsflow-latest-kv-diag-pre-split`,
`thingsflow-latest-kv-diag-pre-split-jetstream-output`) were torn down cleanly.

## The objective GO/NO-GO criterion (stated before applying it)

Per this plan's contract:
- **GO** iff `pre-split-jetstream-output`'s `PROCESSED_RATE` exceeds the fresh
  `pre-split` baseline's `PROCESSED_RATE` by ≥10%, with `pending_after` ≈ 0 for both
  runs and no new failure mode.
- **NO-GO** iff the new variant's rate is within ±10% of the baseline or lower.
- **INCONCLUSIVE/BLOCKED** iff either run failed to produce valid (non-INVALID) data
  even after one retry.

## Why this is INCONCLUSIVE, not NO-GO: the publisher's own ceiling, not either output, is what was measured

The two `PROCESSED_RATE` figures are not merely close — they are **identical**:
7,741.9 msg/s for both, from an identical `count=240000 elapsed_s=31` publisher
report in both runs. Mechanically applying the ±10% rule to two bit-for-bit identical
numbers would satisfy the letter of the "NO-GO iff within ±10%" criterion, but doing
so here would misrepresent an *unmeasured* comparison as a *measured* one, and this
finding deliberately does not do that. Three separate pieces of evidence make this
conclusion unavoidable, not merely cautious:

1. **`compute_processed_rate()`'s own formula for the pre-split family measures the
   publisher, not the consumer.** For `pre-split`/`pre-split-jetstream-output`,
   `accepted` and `duration` come from `publish-presplit.sh`'s own `PUBLISH_RESULT`
   line (`count=240000 elapsed_s=31`) — i.e., how fast the synthetic publisher itself
   could push 240,000 messages through its fixed 2-connection `nats pub --count` pool,
   not how fast either Bento consumer could process them.
   `pending_after=0` only proves each consumer kept up with whatever the publisher
   sent; it is a floor on consumer throughput (≥7,741.9 msg/s), not a ceiling, and
   cannot by itself discriminate `nats_kv` from `nats_jetstream` beyond that floor.
2. **This exact number already exists as Phase 1's own historical `pre-split`
   measurement.** `benchmarks/FINDING-twin-state-localization.md`'s "run C"
   (`.results/pre-split-20260813T001301Z.txt`, a different day, same 2-publisher
   default) reported the identical 240,000 messages / 31s = 7,741.9 msg/s. Two
   independent sessions, on two different days, with two different diagnostic
   consumers (`nats_kv` output both times, since Phase 1 never ran the
   `nats_jetstream` bypass-ingest variant), reproducing the exact same figure down to
   the second, is strong evidence that `publish-presplit.sh`'s 2-connection pool has
   its own fixed, deterministic ceiling on this cluster — independent of which
   consumer variant happens to be listening.
3. **Today's own two runs reproduced that ceiling a third and fourth time, across
   the ONE dimension this plan changed.** Swapping `pre-split`'s output block from
   `nats_kv` to `nats_jetstream` produced zero change in the publisher's own achieved
   rate — exactly what would be expected if the publisher, not the consumer output,
   is the binding constraint in this specific experimental design. This is
   structurally the same failure mode Phase 1's finding already documented for the
   HTTP-ingest variants (`drop-output`/`jetstream-output`, both `CLIENT WAS THE
   BOTTLENECK`) — a generator/client-side ceiling masquerading as a platform result —
   now resurfaced on the publisher side of the "bypass-ingest" harness instead of the
   HTTP-ingest side.

Per this plan's own edge-case instruction ("Either run is generator-limited... record
it as INVALID with the reason; if this makes the comparison impossible, the
`## Verdict` is INCONCLUSIVE/BLOCKED, not a guessed GO or NO-GO"), that is exactly
this situation. Neither run is individually invalid in the sense Phase 1 used the
term (no errors, no backlog, no unhealthy data plane) — both are clean, valid
measurements of "this publisher pool, unchanged, produces 7,741.9 msg/s against
either output." What is invalid is treating that pair as a comparison of the two
**outputs**' capacities, because the publisher never pushed either consumer hard
enough to reveal a difference. `pending_after=0` for both is consistent with both
outputs comfortably exceeding 7,741.9 msg/s — including the scenario where they are
meaningfully different from each other, just both above today's floor.

## What this does NOT prove

- **That `nats_jetstream` output does not outperform `nats_kv` output.** It might;
  today's design could not detect that difference, in either direction, because the
  publisher's own ceiling was reached first, at a rate both outputs drained without
  backlog.
- **That `nats_kv` and `nats_jetstream` are equivalent in throughput.** Equally
  unproven — `pending_after=0` for both is consistent with either output having
  meaningfully more headroom above 7,741.9 msg/s than the other; this design cannot
  see past its own publisher's ceiling to say.
- **That the `$KV.<bucket>.<key>` publish-semantics correctness result is in
  question.** It is not — that is a separate, already-settled precondition (seven
  consecutive PASS runs across this and Phase 1), unaffected by today's throughput
  ambiguity.
- **That there was any data loss, error, or unhealthy data-plane state.** There was
  none — `check_dataplane_health()` passed at every checkpoint, and every diagnostic
  resource was torn down cleanly.

## Verdict

**INCONCLUSIVE.**

Both the `pre-split` (nats_kv) and `pre-split-jetstream-output` (nats_jetstream) runs
completed cleanly, with zero backlog and zero errors, but produced **bit-for-bit
identical** `PROCESSED_RATE` figures (7,741.9 msg/s) that match Phase 1's own
historical `pre-split` number exactly. This is not a coincidence consistent with
"the two outputs perform identically" — it is the signature of
`publish-presplit.sh`'s fixed 2-connection publisher pool being the binding
constraint in both runs, not either consumer's output mechanism. The GO/NO-GO
criterion requires a comparison of the two outputs' actual throughput; today's data
does not contain one. Applying the ±10% rule mechanically to two identical numbers
would produce a NO-GO verdict that overstates what was actually measured — this
finding declines to do that, per the plan's own INCONCLUSIVE/BLOCKED instruction for
exactly this class of confound.

**Recommendation for Plan 02-02: do not apply rung 1 or rung 2 to production on the
strength of this finding.** Plan 02-02 should read this verdict as INCONCLUSIVE and
either escalate/emit `BLOCKED` (per its own stop_gates for an inconclusive Plan 02-01
verdict) or trigger a narrow follow-up re-run before any production config change is
made. Specifically, to actually resolve the comparison:

1. **Re-run with a higher `PRESPLIT_PUBLISHERS` pool** — `publish-presplit.sh`
   already supports this via its existing `MAX_SAFE_PUBLISHERS`/
   `--i-understand-the-oom-risk` guard (added after this same harness's OOM incident
   in Phase 1). Push `PRESPLIT_PUBLISHERS` up to `MAX_SAFE_PUBLISHERS` (4, no opt-in
   required) first — if the identical-rate pattern persists at 4 publishers, that
   would itself be informative (a harder ceiling than the pool size alone). Only
   consider `--i-understand-the-oom-risk` to exceed 4 with explicit, deliberate
   operator sign-off, given the documented OOM history on this specific cluster —
   and only after confirming cluster headroom via `check_dataplane_health()`
   immediately beforehand, exactly as that flag's own guard text requires.
2. **Alternatively, increase `PRESPLIT_COUNT` at the same publisher count** to see
   whether a longer sustained run at the same rate eventually reveals divergence
   (e.g., output-side backpressure or ack-latency differences that a 31-second burst
   would not surface) — cheaper than adding publishers, but less likely to actually
   raise the ceiling if the bottleneck is per-connection publish throughput rather
   than run duration.
3. Whichever approach is used, the same discipline this plan followed still applies:
   a fresh, same-session, matched pair (not reused historical numbers), with the
   GO/NO-GO criterion computed only if the publisher's own rate is verifiably no
   longer the binding constraint for at least one of the two runs (e.g., visible
   backpressure, growing `pending_after`, or the two variants' rates finally
   diverging).
4. This harness (`benchmarks/twin-state-localization/`, now extended with
   `pre-split-jetstream-output` and the `VARIANTS` scoping mechanism) is reusable
   for that follow-up without further extension — only the `PRESPLIT_PUBLISHERS`/
   `PRESPLIT_COUNT` env vars need to change for the next attempt.

## Phase 2 Deployment Decision

**Decision: BLOCKED. No production config change applied.**

**Plan:** `.planning/phases/02-config-fix-ladder/02-02-PLAN.md` (Phase 2, Wave 2).
**Executed:** 2026-08-13.
**Rung applied:** None. `k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml` was not
modified. No `helm upgrade` was run against `thingsflow-fresh` or any other release.

### Verdict text this decision is gated on

Quoted verbatim from the `## Verdict` section above (unmodified by this addendum):

> **INCONCLUSIVE.**
>
> Both the `pre-split` (nats_kv) and `pre-split-jetstream-output` (nats_jetstream) runs
> completed cleanly, with zero backlog and zero errors, but produced **bit-for-bit
> identical** `PROCESSED_RATE` figures (7,741.9 msg/s) that match Phase 1's own
> historical `pre-split` number exactly. This is not a coincidence consistent with
> "the two outputs perform identically" — it is the signature of
> `publish-presplit.sh`'s fixed 2-connection publisher pool being the binding
> constraint in both runs, not either consumer's output mechanism. The GO/NO-GO
> criterion requires a comparison of the two outputs' actual throughput; today's data
> does not contain one. Applying the ±10% rule mechanically to two identical numbers
> would produce a NO-GO verdict that overstates what was actually measured — this
> finding declines to do that, per the plan's own INCONCLUSIVE/BLOCKED instruction for
> exactly this class of confound.
>
> **Recommendation for Plan 02-02: do not apply rung 1 or rung 2 to production on the
> strength of this finding.** Plan 02-02 should read this verdict as INCONCLUSIVE and
> either escalate/emit `BLOCKED` (per its own stop_gates for an inconclusive Plan 02-01
> verdict) or trigger a narrow follow-up re-run before any production config change is
> made.

This is also independently corroborated by `.planning/phases/02-config-fix-ladder/02-01-SUMMARY.md`
("What Plan 02-02 should do": *"Emit `BLOCKED`, not guess a rung."*).

### Why this triggers Plan 02-02's stop_gate, not a judgment call

Plan 02-02's Task 1 decision tree (`.planning/phases/02-config-fix-ladder/02-02-PLAN.md`,
lines 181-188) names exactly three allowed outcomes: GO → rung 1, NO-GO → rung 2,
"Anything else (INCONCLUSIVE, BLOCKED, missing verdict, internally inconsistent
verdict) → STOP... Emit `BLOCKED` for the whole plan." The verdict above is an explicit,
unambiguous INCONCLUSIVE (not a placeholder, not missing, not internally
inconsistent — the reasoning is self-consistent and the recommendation to Plan 02-02
is stated in plain language). Plan 02-02's own stop_gates (line 159) restate the same
condition. No rung was "probably fine" to default to; applying either config to
production on this evidence would misrepresent an unmeasured comparison (publisher's
own fixed-pool ceiling, not either output's headroom) as a measured one — exactly the
outcome both the finding and the plan explicitly decline to do.

### What was (and was not) done as a result

- Task 2 (apply diff + `helm upgrade` to `thingsflow-fresh`) was **not started**. No
  cluster state was touched by this plan. `kubectl --context=microk8s cluster-info`
  and the `thingsflow-fresh` release identity were not re-verified because no deploy
  was attempted — nothing depended on them.
- Task 3's live sanity-check burst (HTTP ingest + `nats kv get` read-back +
  `num_pending` settle check) was **not run** — there is nothing deployed to sanity-check.
- `k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml` is confirmed byte-identical to
  its pre-plan state: `git diff --quiet -- k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml`
  passes (no tracked diff exists for this plan to have introduced).
- All `files_forbidden` paths (`values.yaml`, `templates/**`, `flow-core/**`,
  `benchmarks/twin-state-localization/**`, `benchmarks/FINDING-twin-state.md`,
  `benchmarks/FINDING-twin-state-localization.md`) confirmed byte-identical to their
  pre-plan state via `git diff --quiet` — see verification commands below.
- The existing `## Verdict` section above this addendum was not rewritten or deleted —
  only this `## Phase 2 Deployment Decision` section was appended.

### Verification commands run

```
$ git status --porcelain -- k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml \
    k8s/helm/thingsflow/values.yaml k8s/helm/thingsflow/templates \
    benchmarks/twin-state-localization benchmarks/FINDING-twin-state.md \
    benchmarks/FINDING-twin-state-localization.md
(no output — nothing modified)

$ git diff --quiet -- k8s/helm/thingsflow/values.yaml k8s/helm/thingsflow/templates \
    benchmarks/twin-state-localization benchmarks/FINDING-twin-state.md \
    benchmarks/FINDING-twin-state-localization.md \
  && echo "CLEAN (forbidden paths untouched)"
CLEAN (forbidden paths untouched)
```

### Escalation / recommended next step

Per the finding's own "What this does NOT prove" and recommendation sections: re-run
the bypass-ingest micro-bench with `PRESPLIT_PUBLISHERS` raised toward
`MAX_SAFE_PUBLISHERS=4` (no `--i-understand-the-oom-risk` opt-in required at that
ceiling) using the existing `benchmarks/twin-state-localization/` harness, so that at
least one of the two variants shows the publisher is no longer the binding constraint
(visible backpressure, growing `pending_after`, or the two variants' rates finally
diverging). Only once that follow-up produces a genuine GO or NO-GO should Plan 02-02
(or a successor plan applying the same contract) be re-attempted. This decision does
not recommend a rollback of anything, because nothing was deployed by this plan — the
test cluster's `thingsflow-fresh` `bento-nats-latest-kv` consumer remains on its
pre-Phase-2 `output.nats_kv` configuration, unchanged.

## Follow-up re-run at PRESPLIT_PUBLISHERS=4 (2026-08-13T022440Z) — still INCONCLUSIVE, plus a transient tooling incident

Executed by the orchestrator directly (same incident-response rule as the original
run): `PRESPLIT_PUBLISHERS=4 VARIANTS="pre-split pre-split-jetstream-output" \
./run-localization.sh --live`, per the prior section's own recommendation, using the
harness unmodified (no code changes were needed — only the env var changed).

### Results

| variant | publisher achieved | PROCESSED_RATE | pending/unprocessed at close | note |
|---|---:|---:|---:|---|
| pre-split (nats_kv) | 240,000 msgs / 24s = 10,000.0 msg/s | **10,000.0 msg/s** | 0 | clean, automated capture |
| pre-split-jetstream-output (nats_jetstream) | 240,000 msgs / 23s = 10,434.8 msg/s | **~10,434.8 msg/s (reconstructed, see below)** | 0 | automated capture failed transiently; reconstructed from the consumer's own authoritative state |

**Still within ~4.3% of each other — below the ≥10% GO threshold, and each rate still
tracks its own run's publisher-achieved rate almost exactly.** Raising
`PRESPLIT_PUBLISHERS` from 2 to 4 raised the shared ceiling from 7,741.9 msg/s to
~10,000-10,435 msg/s (confirming the publisher pool was indeed a real, measurable
constraint — informative on its own, as the prior section anticipated), but neither
consumer showed backpressure (`pending`/`unprocessed` = 0 for both) at the new,
higher rate either. The publisher remains the practical ceiling at
`MAX_SAFE_PUBLISHERS=4` — the comparison this finding needs still has not happened.

### Transient tooling incident during this run (investigated and resolved)

`pre-split-jetstream-output`'s automated `PROCESSED_RATE` computation failed:
`consumer_pending()`'s ephemeral verification pod returned `nats: no servers
available for connection`, so `pending_after=-1` and `compute_processed_rate()`
correctly reported `PROCESSED_RATE=UNAVAILABLE` rather than fabricating a number.
The same connectivity blip then caused `teardown-diag-consumer.sh` to report
`AMBIGUOUS_STATE` (unable to confirm whether the consumer still existed) and orphan
the diagnostic consumer `thingsflow-latest-kv-diag-pre-split-jetstream-output`.
Immediately afterward, the `$KV.` publish-semantics check's READBACK sub-check
**FAILED** for the first time in this project's history (7 prior consecutive PASSes
across Phase 1 and this plan's original run) — `got [] want
[readback-v1-0813022755-27440]`.

The orchestrator stopped and investigated directly rather than proceeding, given this
project's documented NATS-instability incident history (the Phase 1 NATS OOM
restart). Findings:
- **Production data-plane health**: confirmed OK (`check_dataplane_health()`, all 4
  durables Active) both during the run and on independent re-check immediately after.
- **The orphaned diagnostic consumer's own authoritative state proves the actual data
  path worked correctly**: `nats consumer info TF_RAW
  thingsflow-latest-kv-diag-pre-split-jetstream-output` showed `Last Delivered
  Message: Consumer sequence: 240,000`, `Acknowledgment Floor: Consumer sequence:
  240,000`, `Unprocessed Messages: 0`, `Redelivered Messages: 0` — all 240,000
  messages were delivered and acked with zero redeliveries. The failure was isolated
  to a short-lived verification pod's own connection attempt (plausible transient
  DNS/service-resolution delay for an ephemeral pod launched immediately after a
  burst of heavy concurrent traffic), not a fault in the production NATS server, the
  diagnostic consumer, or the `nats_jetstream` output mechanism itself.
- **The orphaned consumer was manually torn down** (`teardown-diag-consumer.sh
  pre-split-jetstream-output`, confirmed gone via a follow-up `consumer info` call
  returning the expected NOT_FOUND error) — no orphaned cluster state remains.
- **The READBACK failure was confirmed transient, not reproducing**: an immediate,
  isolated re-run of `verify-kv-publish-semantics.sh` (no load in flight) passed all
  three checks (READBACK, WATCHER, WRITETWICE) cleanly. Rung 1's correctness
  precondition remains solid — this was a one-off tooling hiccup during a moment of
  concurrent heavy cluster activity, not a regression in `$KV.` publish semantics.

Given the consumer's own authoritative state, the reconstructed `PROCESSED_RATE` for
`pre-split-jetstream-output` — `(accepted - pending_after) / duration =
(240,000 - 0) / 23 = 10,434.8 msg/s` — is used in the results table above with
appropriately reduced confidence (derived from `consumer info`, not the harness's own
automated capture), not fabricated.

### Updated Verdict: still INCONCLUSIVE

Doubling `PRESPLIT_PUBLISHERS` (2→4) raised the shared ceiling by ~29-35% but did not
produce a real comparison — both outputs remain within noise of their own run's
publisher-achieved rate, and neither showed backpressure. Two independent attempts at
two different publisher-pool sizes have now both failed to push either consumer to
its actual ceiling. `MAX_SAFE_PUBLISHERS=4` is the harness's own conservative,
deliberately-set limit (per the Phase 1 NATS OOM incident) — exceeding it requires
explicit, deliberate operator sign-off (`--i-understand-the-oom-risk`), not a default
next step.

**Recommendation:** before spending another attempt at a higher, riskier publisher
count, consider whether a different diagnostic design would resolve this faster and
more safely — e.g., stressing each consumer's output stage directly (bypassing the
synthetic publisher's own connection-pool ceiling entirely, such as a saturating
in-cluster load generator colocated with the consumer, or reusing `nats bench`'s own
higher-throughput pattern referenced in `benchmarks/scripts/run-nats-benchmark.sh`)
rather than continuing to scale `publish-presplit.sh`'s pool size incrementally.
Escalating past `MAX_SAFE_PUBLISHERS=4` should only happen with explicit operator
sign-off and a fresh `check_dataplane_health()` confirmation immediately beforehand,
per `publish-presplit.sh`'s own existing guard text.

## Redesigned diagnostic (2026-08-13T024648Z onward): GO — jetstream output measurably faster once the publisher confound is removed entirely

Per this section's own prior recommendation, rather than escalating
`PRESPLIT_PUBLISHERS` past the safe cap, the diagnostic was redesigned to remove the
external synthetic publisher entirely — the root cause common to every prior
INCONCLUSIVE attempt.

### Design

Two new self-contained Bento configs
(`benchmarks/twin-state-localization/configs/generate-nats-kv.yaml` and
`configs/generate-nats-jetstream.yaml`) use Bento's `input.generate` to synthesize
messages **in-process**, inside the pod itself — no NATS input consumer, no
JetStream stream read, no external publish over the network at all. Verified live
before use (per this project's "verify --help/behavior first" discipline):
`interval: ""` runs the generator fully unthrottled — a `generate -> drop`
smoke test processed 200,000 messages in 5.25s (~38,095 msg/s) on this cluster's
pinned `bento:1.8.1` image, roughly 4x above the highest ceiling any prior
publisher-based attempt reached. This removes the external-publisher confound in one
stroke: the pipeline+output combination becomes the sole constraint on throughput,
which is exactly the dimension this comparison needs to isolate.

Both configs generate the identical message shape as `pre-split.yaml`'s own pipeline
output (`{"ts":...,"value":...}` with `kv_key` in metadata, cycling across the same
50-key diagnostic namespace), differing ONLY in their `output:` block (`nats_kv` vs
`nats_jetstream`, byte-identical to every prior variant's corresponding block) — the
same single-variable-isolation methodology used throughout this harness, just with a
different (unthrottled, in-process) input mechanism.

Since `input.generate` has no external consumer to read a pending/delivered count
from, throughput is measured via a different but equally authoritative source: the
`twin_state` bucket's backing stream (`KV_twin_state`)'s `last_seq` field, confirmed
live to increment monotonically on every successful Put (`nats_kv` or
`nats_jetstream`, both are JetStream publishes under the hood) regardless of KV
compaction. `run-generate-benchmark.sh` reads `last_seq` before and after each run;
the delta is the authoritative count of writes that actually landed — not a
self-reported number from the publisher or the Bento process. Deployed as a single
throwaway `Pod` (not the `Deployment`+ephemeral-consumer machinery the other
variants need, since there is no consumer here) with resources matched to
production's `natsDataPlane.latestKv.resources` (no "unequal budgets"), in namespace
`thingsflow-fresh`, always torn down via `trap ... EXIT`. Cross-checked against
production data-plane health before and after every run.

Smoke-tested first at 5,000 messages (`writes=5000` matched the `last_seq` delta
exactly, confirming the read-back methodology) before any full-scale run.

### Results (4 independent measurement rounds, 2026-08-13T024715Z–T025541Z)

| round | messages | nats_kv rate | nats_jetstream rate | jetstream vs kv |
|---|---:|---:|---:|---:|
| 1 | 500,000 | 20,000.0 msg/s (25s) | 20,833.3 msg/s (24s) | +4.2% |
| 2 | 500,000 | 17,241.4 msg/s (29s) | 20,833.3 msg/s (24s) | +20.8% |
| 3 | 500,000 | 17,241.4 msg/s (29s) | 18,518.5 msg/s (27s) | +7.4% |
| 4 (high-precision) | 2,000,000 | **21,505.4 msg/s (93s)** | **25,974.0 msg/s (77s)** | **+20.8%** |

Rounds 1-3 (25-29s wall-clock) have meaningful measurement noise from this timing
method's 1-second resolution — at that duration, ±1s of jitter is a ±3-4% relative
error, comparable in magnitude to the effect being measured. Round 4 was run
specifically to resolve this: 2,000,000 messages gives 77-93s runtimes, where the
same ±1s jitter is only ~1% relative error — a much higher-confidence measurement.

**`nats_jetstream` output was faster than `nats_kv` output in all 4 independent
rounds — never once slower or equal.** The high-precision round shows a clean
**+20.8%** advantage, consistent with round 2's low-precision result and well above
this finding's own ≥10% GO threshold. Every round completed with zero errors, zero
data loss (every write confirmed via the `last_seq` delta matching the requested
`count` exactly), and production data-plane health OK before and after every run.

### Verdict: GO for rung 1 — with an explicit scope caveat

**This is a genuine GO**, per this finding's own objective criterion (≥10% higher
throughput, no errors, no new failure mode) — the first round of this entire Phase 2
effort (three prior attempts: HTTP-ingest in Phase 1, then two bypass-ingest
publisher attempts) to actually produce a valid, uncontaminated comparison between
the two output mechanisms.

**What this proves:** at the output stage in isolation, with data available as fast
as the pipeline can consume it (removing every upstream constraint), `nats_jetstream`
sustains meaningfully higher throughput than `nats_kv` — reproducibly, across 4
independent trials, with the most precise measurement showing +20.8%.

**What this does NOT prove, and why it still matters for Phase 3:** this design
uses a single trivial pipeline stage (parse-free, since `input.generate` produces the
final shape directly) — NOT the production pipeline's full 5-stage
parse/unarchive/fan-out sequence, and NOT real ingest traffic (HTTP or MQTT). Phase
1's own strongest evidence (`benchmarks/FINDING-twin-state-localization.md`) already
points at the `unarchive` fan-out stage as the likely serial bottleneck in the FULL
production pipeline (pre-split's ~7,700-10,400 msg/s vs. the full pipeline's
measured ~3,900 msg/s ceiling) — a bottleneck this output-isolation test does not
touch at all. It is possible that swapping the output in production removes a real
~20% constraint that is currently masked by the larger `unarchive` bottleneck, in
which case the full-pipeline improvement from rung 1 alone could be smaller than
20%, or even negligible if `unarchive` is capping throughput well below where the
output's own headroom would matter. This is exactly what Phase 3's fair-ramp
verification (`benchmarks/FINDING-twin-state.md`'s 5 load levels, full pipeline,
real ingest) is the correct and necessary test for — this finding's GO verdict is
sufficient evidence to APPLY rung 1 (satisfying Phase 2's diagnose-gated
requirement), not a substitute for verifying its end-to-end production impact.

**Recommendation:** proceed to apply rung 1 (the `output.nats_jetstream` swap) to
`k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml` and deploy to `thingsflow-fresh`
— i.e., re-attempt Plan 02-02's contract with this GO verdict as the gating input —
then let Phase 3 determine the actual end-to-end throughput impact against the real
≥8,000 msg/s target.

## Phase 2 Deployment Decision — Rung 1 APPLIED (2026-08-13T0307Z)

**Decision: rung 1 applied to production.** User explicitly confirmed proceeding
("Yes, apply rung 1 now") given the redesigned diagnostic's GO verdict above,
superseding the earlier BLOCKED decision recorded further up this document (that
decision was correct given the evidence available at the time — see that section
for the full BLOCKED rationale, preserved for audit trail).

**Rung applied:** `output.nats_kv` → `output.nats_jetstream`, publishing to
`$KV.${NATS_KV_BUCKET}.${! metadata("kv_key") }`, byte-identical to the
already-live-tested output block from `configs/jetstream-output.yaml` (Phase 1) and
`configs/generate-nats-jetstream.yaml` (this section's redesigned diagnostic). Only
`k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml`'s `output:` block changed —
`http:`, `input:`, and `pipeline:` are byte-identical to the pre-deployment state
(confirmed via `git diff`). `values.yaml` and all chart templates are untouched.

**Naming correction made live, before deploying:** the original plan assumed a
Helm release named `thingsflow-fresh` — this is wrong. The actual release is named
`thingsflow` (confirmed via `helm list -n thingsflow-fresh`); `thingsflow-fresh` is
only the namespace. The Deployment is `thingsflow-nats-latest-kv`, not
`thingsflow-fresh-nats-latest-kv`. Verified live before issuing any mutating
command, per this project's established "verify, don't assume" discipline.

**Deployment command and outcome:**
```
$ helm upgrade thingsflow k8s/helm/thingsflow -n thingsflow-fresh --reuse-values
Release "thingsflow" has been upgraded. Happy Helming!
REVISION: 2
STATUS: deployed

$ kubectl -n thingsflow-fresh rollout status deploy/thingsflow-nats-latest-kv --timeout=180s
Waiting for deployment "thingsflow-nats-latest-kv" rollout to finish: 1 old replicas are pending termination...
deployment "thingsflow-nats-latest-kv" successfully rolled out
```
Pod logs confirm: `Input type nats_jetstream is now active`, `Output type
nats_jetstream is now active`, no errors, no crash loops.

**Sanity check (light, real HTTP ingest path — NOT the full fair-ramp verification,
which is explicitly Phase 3's job):**
- Drove a small controlled burst via `loadgen2`: 20 devices, 200 msg/s offered,
  10s duration → 1,800 messages, 0 failed, `client kept its schedule`, p99 latency
  4.1ms.
- Post-burst `thingsflow-latest-kv-durable` consumer state:
  `num_pending=0`, `num_ack_pending=0`, `num_redelivered=0` — no backlog, no
  redelivery/retry pressure.
- Spot-checked `nats kv ls twin_state` — the burst's own keys
  (`DEVICE.<tenant>.<device>.telemetry.lg2_rung1-sanity-*`) are present in the
  bucket, confirming writes actually landed via the new `nats_jetstream` output
  path, not just that the pod started without crashing.

**What this does and does not confirm:** this sanity check confirms the deployment
is functionally correct and healthy under light load — it does NOT confirm the
original ~3,900 msg/s collapse is resolved, since (per this document's own
"What this does NOT prove" section above) the redesigned diagnostic's GO verdict
isolates the output stage only, not the full production pipeline's `unarchive`
fan-out stage that Phase 1's evidence flags as the likely larger bottleneck.

**Handoff to Phase 3 ("Verify and Pin"):** rung 1 is live on `thingsflow-fresh`.
Phase 3 should fair-ramp-verify against `benchmarks/FINDING-twin-state.md`'s
original 5 load levels (958 → 15,333 msg/s, MQTT and HTTP) to determine the actual
end-to-end throughput impact and whether ≥8,000 msg/s sustained is achieved. If
Phase 3 finds the collapse persists near its original ~3,900 msg/s point despite
rung 1, that would be strong evidence the `unarchive` fan-out stage — not the
output — is the dominant bottleneck, and this milestone's diagnose-gated fallback
(no config-reachable cause beyond rung 1) may apply.
