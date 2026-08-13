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
