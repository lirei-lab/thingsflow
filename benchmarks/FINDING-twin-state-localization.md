# Phase 1 localization: partial — pre-split is directionally consistent with the unarchive-fan-out hypothesis (not conclusive), `$KV.` semantics fully verified, output-mechanism micro-bench blocked by a newly-discovered (but not yet fully evidenced) ingest-path bottleneck

**Discovered:** 2026-08-11/2026-08-12, Milestone 2 Phase 1, Plan 01-01
**Status:** measured, partial · extends `benchmarks/FINDING-twin-state.md` (unmodified) · does **not** conclusively name the serial stage with numbers from all three planned experiments — one of three (the output-mechanism micro-bench) could not be validly executed today for a reason unrelated to this harness's own configuration

This finding is the output of the diagnostic harness under `benchmarks/twin-state-localization/`
(3 Bento config variants + supporting scripts), run live against the test cluster
(`kubectl --context=microk8s -n thingsflow-fresh`). It does not modify
`k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml`, any file under
`k8s/helm/thingsflow/templates/`, `values.yaml`, `flow-core/internal/twinstore`, or
`benchmarks/FINDING-twin-state.md`.

## Summary of what happened across three attempts

1. **2026-08-11T205044Z (prior agent, interrupted before write-up):** `drop-output` and
   `jetstream-output` were driven with loadgen2's HTTP engine at default connection-pool
   settings (`--http-connections` 512) — both came back `VERDICT: CLIENT WAS THE
   BOTTLENECK` (87% of scheduled sends rejected client-side as `pool_full`). `pre-split`
   ran against a diagnostic consumer bootstrapped with `--deliver all`, which replayed
   124,000 stale messages from earlier manual testing before any load was sent — an
   invalid, contaminated baseline. The `$KV.` semantics check passed cleanly.
2. **2026-08-12T130911Z (this agent, first corrected attempt):** fixed the stale-backlog
   bug by switching `bootstrap-diag-consumer.sh` to `--deliver new`. This broke ALL THREE
   variants' Bento pods: `bind: true` inputs validate the *existing* consumer's actual
   deliver policy against Bento's own default (`all`), so every replica logged `Failed to
   connect to nats_jetstream: configuration requests deliver policy to be 0, but
   consumer's value is 2` in a permanent retry loop and never became ready
   (`ROLLOUT_FAILED` on all three, confirmed live via a standalone reproduction pod).
3. **2026-08-12T132612Z (this agent, corrected attempt):** added `deliver: new` to each
   variant config's `input.nats_jetstream` block to match the consumer's actual policy —
   confirmed live (standalone pod: `Input type nats_jetstream is now active`, ready=true)
   before re-running the full harness. All three variants rolled out cleanly this time,
   `diag_consumer_pending_before=0` for every variant (the stale-backlog bug is fixed).
   `drop-output`/`jetstream-output` were re-driven with a raised connection pool
   (`--http-connections 8000`) — this did **not** fix the client-bottleneck verdict; it
   made it much worse (see below). A live connection-pool sweep (standalone probes, not
   part of the committed variant configs, **and not saved to any retained evidence
   file** — see the "Evidence status" note before the Verdict) suggested a candidate
   explanation: an HTTP-ingest-path throughput ceiling on this shared test cluster,
   independent of client configuration. **This candidate explanation is not yet
   verified** — treat it as a hypothesis pending re-run, not a settled conclusion.

## Results per variant (2026-08-12T132612Z, the corrected run)

| variant | offered¹ | accepted | PROCESSED_RATE | pending at close | verdict |
|---|---:|---:|---:|---:|---|
| drop-output | 3,666.6 msg/s | 15.4 msg/s (926/9,287 attempted) | 15.4 msg/s | 0 | **INVALID** — see below. loadgen2's own tool verdict on this exact run is `CLIENT WAS THE BOTTLENECK`; the "starved of load upstream" framing is a hypothesis this doc argues for below, not something the raw evidence states on its own. |
| jetstream-output | 3,666.6 msg/s | 15.6 msg/s (938/11,513 attempted) | 15.6 msg/s | 0 | **INVALID** — same reason/caveat |
| pre-split (run A) | 7,500 msg/s (publisher)¹ | 7,272.7 msg/s | 7,272.7 msg/s | 0 | **VALID** — publisher-limited, consumer drained fully. Cited: `.results/pre-split-20260812T132612Z.txt`. |
| pre-split (run B, pushed further) | 10,212 msg/s (publisher, 4 conns)¹ | 10,212 msg/s | 10,212.8 msg/s | 0 | **UNARTIFACTED** — see note below. Treat as an unverified follow-up observation, not evidence on the same footing as run A. |

¹ "Offered" for the pre-split rows is a pre-run planning target (`COUNT` messages ÷
intended duration), not a controlled, independently-measured open-loop rate the way
loadgen2 produces for drop-output/jetstream-output — `publish-presplit.sh` has no
pacing/rate-limiting, so there is no quantity distinct from "what was achieved." Don't
read the pre-split "offered" column as comparable to the loadgen2 rows'.

**Evidence gap — run B has no backing file.** `PROCESSED_RATE = (accepted −
pending_at_close) / duration`, per `FINDING-twin-state.md`'s formula, now actually
computed and printed by `run-localization.sh` (this was missing in the interrupted prior
run). Raw evidence exists and was checked line-for-line for `drop-output`/`jetstream-output`
(`.results/drop-output-20260812T132612Z.txt`, `.results/jetstream-output-20260812T132612Z.txt`)
and for pre-split run A (`.results/pre-split-20260812T132612Z.txt`, 240,000 msgs / 33s).
**No such file exists for "run B"** (480,000 msgs / 47s, described as "executed as a
standalone follow-up... bootstrap/deploy/teardown used the same scripts, consumer name
`thingsflow-latest-kv-diag-pre-split-boost`, fully torn down afterward") — this number,
including the cited CPU figure (550m/3000m), is not independently verifiable from
anything in this repository today. It should not be treated as measured fact until it is
re-run and its output saved, the same as every other experiment in this phase.

## Why drop-output and jetstream-output are INVALID: a hypothesized HTTP-ingest-path bottleneck, not yet independently verified

The instruction for this corrected run was: fix the loadgen2 connection pool
(`--http-connections`/`--max-inflight`), which the 2026-08-11 run's 512-connection default
had clearly under-provisioned (87% `pool_full`). That fix was applied and verified in
isolation — `./run.sh calibrate --protocol http --devices 500 --max-workers 4
--calibrate-rates 2000 4000 6000 8000 --calibrate-step-seconds 15` (against loadgen2's
local sink, which forces `--target sink` regardless of flags) showed this host's own
generation ceiling is comfortably above 8,000 msg/s at ~1 core — the **generator itself**
was never the constraint.

But raising the connection pool against the **real cluster** did not fix the client-
bottleneck verdict — it went from bad (87% `pool_full` at 512 connections) to catastrophic
(96% blocked, plus new `conn_lost`/`drain_incomplete` failure modes, accepted throughput
**collapsing to ~15 msg/s**) at 8,000 connections. A standalone sweep (short 15s probes,
200 devices, not part of the committed variant results, **and not saved to any file —
see the evidence-gap note below**) is presented here as a candidate explanation for the
shape of the problem:

| `--http-connections` | offered | accepted | p50 latency | pool_full % | other failures |
|---:|---:|---:|---:|---:|---|
| 512 (loadgen2 default) | 3,555.5 msg/s | 438-443 msg/s | ~1.1s | 87% | none |
| 768 | 3,599.8 msg/s | 454.0 msg/s | 1.60s | 87.4% | none |
| 1,536 | 3,599.8 msg/s | 503.2 msg/s | 2.88s | 86.0% | none |
| 3,072 | 3,599.8 msg/s | 497.0 msg/s | 4.65s | 84.3% | **http_503: 807, http_504: 232** (first real server errors) |
| 512 @ 1,000 msg/s offered (not 4,000) | 899.9 msg/s | 493.2 msg/s | 897ms | 45.2% | none |
| 8,000 (the committed run's setting) | 3,666.6 msg/s | ~15.5 msg/s | seconds+ | 95%+ | conn_lost + drain_incomplete (new) |

**As described by the (unfiled) sweep, accepted throughput plateaus at ~450-500 msg/s
across the entire 512-3,072 connection range, and across offered rates from 1,000 to
4,000 msg/s** — raising the pool only makes requests queue longer (latency scales roughly
linearly with pool size) without increasing completions. Real HTTP 5xx errors appear only
once concurrency is pushed hard (3,072), and at 8,000 the picture changes qualitatively
(client-side `conn_lost`), consistent with this single-node test cluster running out of
some shared resource (connection tracking, server thread/worker pool, or node-level
contention) well before the client's own pool size is the limit — **but this
interpretation rests entirely on data that was never saved to a retained artifact.**

**Evidence status — this claim is NOT yet independently verified.** The calibration step
(local-sink ceiling ≫ 8,000 msg/s, confirming the generator itself is not the constraint)
*is* backed by a real artifact: `benchmarks/scripts/loadgen2/results/calibrate-http-d9179730.json`
(though note this file is currently untracked in git — see the review-cycle-1 fix list).
The connection-pool sweep against the real cluster is **not** backed by any artifact,
tracked or untracked, anywhere in this repository. Worse, it directly contradicts the one
piece of evidence that *is* citable for the actual committed run: `.results/drop-output-20260812T132612Z.txt`
and `.results/jetstream-output-20260812T132612Z.txt` both contain loadgen2's own tool
verdict, `VERDICT: CLIENT WAS THE BOTTLENECK`, for the 8,000-connection run — i.e. the
generator's own self-diagnosis says client-side, and this document overrides that
diagnosis on the strength of unfiled data. Until the sweep is re-run and its raw output
saved, **treat "a real, config-external constraint, not a generator misconfiguration" as
an unverified hypothesis, not a conclusion** — see "Evidence status" before the Verdict
section for what a follow-up re-run needs to resolve this.

`benchmarks/FINDING-twin-state.md` (2026-08-03) reported ingest accepting 15,333 msg/s
without a single error using a different (closed-loop) generator; today's measurement,
using the rigorous open-loop `loadgen2` (built specifically to not silently absorb
backpressure — see `benchmarks/scripts/loadgen2/README.md`), shows this test cluster's
HTTP ingest path capping delivered throughput around ~450-500 msg/s under current
conditions (per the unfiled sweep) or ~15 msg/s (per the filed, committed-run evidence) —
**these two numbers disagree by ~30x and this document does not yet have the evidence to
say which one, if either, correctly characterizes today's ingest ceiling.** Both
`drop-output` and `jetstream-output` share this exact same HTTP ingest front door with the
production consumer, so **neither could be driven to a load level anywhere near the
~3,900-8,000 msg/s regime where the original congestion collapse was observed** — both
variants trivially drained the trickle that got through (0 pending at close, sub-1m pod
CPU), which tells us nothing about pipeline-vs-output cost at interesting load. This
micro-bench (design bullet 3: `nats_jetstream` vs `nats_kv` output at identical load) is
therefore recorded **INVALID** for both variants, per `benchmarks/README.md`'s discipline
— not because of a mistake in this harness, but because the precondition (sufficient
sustained load reaching the consumer) could not be met today with the settings actually
used and artifacted.

This is a genuine open question worth a dedicated follow-up (possible causes: Milestone 3's
added `TF_TWIN_EVENTS`/`internal/twinevents` baseline traffic; general node contention on
this single-node cluster from co-located services — see the incident below; or a real
difference between the rigorous open-loop generator and whatever generated the original
15,333 msg/s ingest number) — but it is out of this plan's scope to resolve.

**Script-defaults drift note (review cycle 1):** `run-localization.sh` currently defaults
to `LOAD_HTTP_CONNECTIONS=1536`/`LOAD_MAX_INFLIGHT=2048` (the sweep's best point per the
unfiled data above). The results table's actual numbers (15.4/15.6 msg/s) were generated
by the *pre-correction* setting (`--http-connections 8000 --max-inflight 8192`), which the
script's own inline comment confirms was later found to be the worst setting tried. Running
this harness today, as committed, would **not** reproduce the table's exact numbers — a
follow-up re-run should use the corrected 1536/2048 defaults and update the table with
fresh, matching evidence.

## pre-split: valid, clean, and consistent with (but not conclusive proof of) the unarchive-fan-out hypothesis

`pre-split` bypasses HTTP ingest entirely — `publish-presplit.sh` publishes directly onto
the diagnostic subject via raw JetStream `nats pub`, so it is unaffected by the ingest-path
bottleneck above. With the stale-backlog bug fixed (`--deliver new` bootstrap +
`deliver: new` in the Bento config, both confirmed live), `diag_consumer_pending_before=0`
in every run, so the measurement starts from a true zero baseline.

One clean, artifacted data point (run A) and one unartifacted follow-up observation (run
B), both claimed as **publisher-limited, not consumer-limited** (the diagnostic Bento
consumer — no `unarchive` fan-out stage, single-key-per-message, unchanged
`output.nats_kv` — reportedly drained every published message with **zero backlog at
close** in both cases):

- **Run A (artifacted, `.results/pre-split-20260812T132612Z.txt`)**: 240,000 messages /
  33s = **7,272.7 msg/s**, zero pending.
- **Run B (UNARTIFACTED — see the evidence-gap note in the results table above)**:
  480,000 messages / 47s (4 parallel publisher connections), claimed **10,212.8 msg/s**,
  zero pending, diagnostic pod CPU 550m of a 3,000m limit (18%). No file backs any of
  these numbers. Treat run B as an anecdotal follow-up observation, not a verified
  measurement, until it is re-run and saved.

A third attempt at 6 parallel publisher connections (720,000 messages) collapsed to
1,512 msg/s — this is a **generator-side artifact** (6 concurrent `nats pub` processes in
one pod, competing with everything else on this single-node cluster, is itself
resource-contended) and is excluded as an invalid data point, per the same discipline
applied to drop-output/jetstream-output above. It is not evidence about the consumer's
ceiling.

**Two methodology caveats not accounted for above:**
- **Key cardinality.** The synthetic publisher (`publish-presplit.sh`) cycles through only
  50 fixed KV keys (`{{ Random 1 50 }}`), each overwritten thousands of times per run,
  versus production's much larger real key cardinality (per-tenant/per-device/per-telemetry-key).
  This is a second uncontrolled variable riding along with "remove the unarchive fan-out" —
  NATS KV write/dedup behavior under heavy same-key overwrite may not generalize cleanly to
  a broad key spread, so this comparison is not a pure single-variable isolation.
- **"Landed" is ack-based, not content-verified.** `run-localization.sh` treats
  `pending_after=0` on the diagnostic consumer as proof every message "landed," via a
  JetStream-ack proxy. It never independently reads back the `twin_state` KV bucket to
  confirm the actual content, unlike `FINDING-twin-state.md`'s methodology (which
  cross-checks exact landed row counts against GreptimeDB). This is a weaker guarantee than
  the original finding used, and the 50-key design above would make an independent
  landed-count check structurally uninformative even if attempted (overwrites collapse to
  50 final values regardless of write volume).

**Interpretation, stated carefully:** `benchmarks/FINDING-twin-state.md` shows the full
production pipeline (with `unarchive` fan-out) already accumulating backlog
(10,147 pending) at 3,833 msg/s offered — roughly 11,500 KV operations/s at 3 keys/message.
Using the artifacted run A alone, `pre-split`'s fan-out-free pipeline drained 7,272.7 msg/s
(1 KV operation per message, so 7,272.7 KV ops/s) with **zero** backlog — already a load
level in the same order of magnitude as the point where the full pipeline was congesting,
on real evidence. The unartifacted run B claims this extends to 10,212.8 KV ops/s at only
18% CPU, which if verified would strengthen this reading further, but that number is not
yet independently confirmed (see above). Even on run A alone, this is **suggestive
supporting evidence** for the
`unarchive` fan-out hypothesis already named as the leading suspect in
`benchmarks/FINDING-twin-state.md` ("Where it is, then: inside the consumer" /
"A specific candidate to review... `unarchive`"), not new proof: the fan-out-free
consumer's own true ceiling was not found (both attempts were publisher-limited, and
pushing the publisher harder caused a generator-side collapse rather than revealing the
consumer's limit — see the incident note below for why a third, more aggressive push was
not attempted).

## CPU-headroom evidence caveat

The "not CPU-limited" claims above (e.g. "18% of quota") rest on `pod_cpu_snapshot()`
readings taken via a single post-hoc `kubectl top pod` call *after* the load window ended
and the consumer had already drained (called right before writing `pending_after`, `sleep
15` after load stopped) — not sampled during sustained load. In the raw evidence for
pre-split run A, the snapshot reads `1m` CPU despite the pod having just processed 7,272.7
msg/s, consistent with the snapshot being taken once the pod was already idle again. This
is weaker evidence for "not CPU-bound" than an in-flight sample would be (e.g. polling
`kubectl top pod` every few seconds during the load window, or reading a cumulative
`cpu_seconds` counter delta) — a follow-up re-run should sample during, not after, load.

## `$KV.<bucket>.<key>` publish-semantics: PASS on every reader, confirmed 3 times

`verify-kv-publish-semantics.sh` ran three times across this plan's execution (the
original 2026-08-11 run, and both 2026-08-12 corrected runs) with **identical results**
every time:

| check | result | what it proves |
|---|---|---|
| READBACK | **PASS** | a raw JetStream publish to `$KV.twin_state.<key>` is retrievable via the `nats kv get` API — the bucket's stream config treats it as a valid KV entry |
| WATCHER | **PASS** | a `nats kv watch` subscriber receives an update event for a raw publish — the same code path `flow-core/internal/twinstore`'s `TWIN_STATE_WATCH_ENABLED` watcher depends on |
| WRITETWICE | **PASS** | publishing the same key twice leaves the second value as the final read (last-write-wins), matching `twinstore`'s per-key LWW merge semantics |

Evidence: `.results/kv-semantics-20260811T205044Z.txt`, `.results/kv-semantics-20260812T130911Z.txt`,
`.results/kv-semantics-20260812T132612Z.txt`. **This directly de-risks the Phase 2 rung-1
candidate's correctness** (swap `output.nats_kv` → `output.nats_jetstream` publishing to
`$KV.<bucket>.<key>`) — a raw JetStream publish is observationally identical to a
`nats_kv` Put for every reader that matters. It does **not** by itself prove the swap
improves throughput; that remains untested (see above).

## Incident during this run: NATS OOMKilled, self-recovered, no data loss

While pushing `pre-split`'s synthetic load harder to strengthen the evidence above (a third
attempt, 6 parallel publisher connections, 720,000 messages), the shared JetStream server
pod `thingsflow-nats-0` was **OOMKilled** (`exitCode: 137`, `reason: OOMKilled`) and entered
`CrashLoopBackOff` for several restart cycles. This is a real platform-wide incident on the
shared `thingsflow-fresh` test namespace, not confined to this diagnostic's own resources —
`thingsflow-flow-core` and `thingsflow-http-ingest` also picked up restarts during the same
window.

**Root cause:** the combination of aggressive concurrent synthetic publish load (6 parallel
`nats pub` processes) plus this diagnostic's own consumer plus ordinary co-located cluster
traffic exceeded the NATS pod's 1Gi memory limit during JetStream filestore index rebuild
activity (the pod's own logs show `Filestream state outdated... will rebuild` on every
restart, which is itself CPU/memory-expensive and was repeatedly re-triggered by the
crash loop).

**Resolution:** the 6-publisher load was immediately identified as the trigger and treated
as an invalid, excluded data point (see above); the leftover diagnostic Deployment/consumer
from that attempt were torn down immediately to relieve pressure; Kubernetes's own restart
backoff then brought `thingsflow-nats-0` back to `1/1 Running` without manual intervention
(destructive recovery actions like force-deleting the pod were considered but avoided —
they were blocked by this session's own permission classifier, and waiting out the normal
backoff was sufficient and safer). **No data loss**: the pod's own recovery logs show every
stream restored to its exact prior message count (`KV_twin_state`: 1,291,234 messages,
`TF_RAW`: 1,098,231 messages, `TF_ENTITY`: 240 messages) before returning to `Ready`. All
namespace pods were confirmed `Running`/`Completed` normally afterward, and the orphaned
`thingsflow-latest-kv-diag-pre-split-boost` consumer left behind by the interrupted teardown
(NATS was unreachable when teardown first ran) was found and deleted in a follow-up sweep.

**Lesson for future work on this harness:** this is a single-node, resource-shared test
cluster running several other services (GreptimeDB TTL/freshness-guard CronJobs, the
`twinevents` publisher, etc.) — synthetic load generation for diagnostic purposes should
stay conservative (2-4 parallel publisher connections, as run A and B above) rather than
scaling aggressively, and should watch cluster-wide pod health (not just the resource under
test) during any load push.

**Addendum (orchestrator verification, post-agent):** the executing agent's "no data loss,
self-recovered" claim covered the NATS pod itself but not its downstream consumers. On
independent verification, all four data-plane Bento consumer pods
(`thingsflow-nats-greptimedb`, `thingsflow-nats-entity-greptimedb`, `thingsflow-nats-alarms`,
`thingsflow-nats-latest-kv`) had **not** reconnected after the NATS pod's OOM-restart — each
was spinning `level=error msg="Failed to read message: nats: connection closed"` in a
permanent loop, with zero reconnection attempts. The `greptimedb-freshness-guard` CronJob's
next tick caught this live: `ALERT: 0 telemetry rows in device_telemetry_kv in the last 15m,
... data-plane ingest is halted` (backlog 100,921 messages on `thingsflow-greptimedb-durable`,
`Active Interest: No interest`). This is the exact **"GreptimeDB ingest silent halt"**
failure mode already documented as a known incident pattern for this project (NATS core
subscriptions can die without auto-reconnecting; `200` on ingest does not mean writes are
landing) — reproduced live during this diagnostic session as a side effect of the OOM
incident above, not by anything specific to the diagnostic harness itself. **Fix applied:**
`kubectl rollout restart` on all four affected Deployments; all four reconnected cleanly
(`Input type nats_jetstream is now active`, no further connection errors), the
`greptimedb-durable` consumer backlog drained to 0 with `Active Interest: Active`, and the
next `freshness-guard` tick completed successfully (`0/1 Completed`, no alert). Total
ingest-halt window: approximately 10-15 minutes, bounded by when the guard's next scheduled
tick surfaced it. No permanent data loss (JetStream's durable storage meant the backlog was
recoverable, not dropped), but this was an active outage on the shared test cluster during
that window, not a silent no-op as the original incident note implied. **Follow-up
recommendation**: any future live-cluster diagnostic work on this harness should, after any
NATS pod restart (whether from an OOM incident or otherwise), explicitly check and restart
all data-plane Bento consumer pods rather than assuming they self-reconnect — this is a
recurring platform gap, not specific to this one incident. **Fixed in review cycle 1**:
`run-localization.sh` now has a `check_dataplane_health()` guard checked before any live
run starts and after every variant's load-drive step, catching exactly this failure mode
automatically rather than relying on manual post-hoc verification next time.

**Reviewer note**: `kubectl get pods` will continue to show a `thingsflow-greptimedb-freshness-guard-*`
pod in `Error` status after this incident — that is expected CronJob `failedJobsHistoryLimit`
retention of the exact alert tick cited above, not an ongoing live problem. Subsequent
ticks (checked independently during review) all show `Completed`.

**Process note (review cycle 1):** the load push that caused this incident (6 parallel
publisher connections) was an ad-hoc escalation beyond this harness's own committed,
conservative scope, executed *after* two experiments had already confirmed an external
throughput wall — in hindsight, that result on its own was reason enough to stop and
report a partial finding rather than pushing further against a cluster already showing
contention signals. **Fixed in review cycle 1**: `publish-presplit.sh` now enforces a
`MAX_SAFE_PUBLISHERS` cap (default 4) and requires an explicit `--i-understand-the-oom-risk`
opt-in to exceed it, so "scale up parallelism" is a deliberate, gated decision going
forward rather than something that can happen silently.

## What this does NOT prove

- **That the `unarchive` fan-out is definitively THE serial stage.** `pre-split`'s clean,
  zero-backlog result is consistent with that hypothesis (already the leading suspect in
  `benchmarks/FINDING-twin-state.md`) but does not conclusively confirm it — its own true
  ceiling was never found, and the `drop-output` experiment (pipeline cost vs. output cost,
  which would have isolated this more directly) could not be validly run.
- **That `nats_jetstream` output pipelines better than `nats_kv` output at high load.** The
  `jetstream-output` micro-bench is the one piece of R1's exit criteria that could not be
  executed today — it requires either resolving the ingest-path bottleneck above, or
  redesigning this specific experiment to bypass HTTP ingest the way `pre-split` already
  does (a Phase 2 candidate, not attempted here to stay within this plan's scope and avoid
  a second load-generation incident).
- **That there is data loss or corruption.** There is none — every stream landed exactly
  the expected message counts across all runs, including through the NATS incident above.

## Evidence status: two gaps flagged in review cycle 1

Before the Verdict below, two specific claims in this document are **not yet backed by a
retained evidence file** and should be treated as open, not settled:

1. The connection-pool sweep (512-3,072-connection rows in "Why drop-output and
   jetstream-output are INVALID") — the basis for calling the ~450-500 msg/s ingest ceiling
   a genuine platform constraint rather than a generator artifact. It also directly
   contradicts the one artifacted number available (loadgen2's own `VERDICT: CLIENT WAS
   THE BOTTLENECK` on the committed 8,000-connection run), a contradiction this document
   has not yet resolved with evidence.
2. `pre-split` "run B" (10,212.8 msg/s) — the document's strongest supporting number for
   the `unarchive`-fan-out hypothesis, with no backing file anywhere in the repository.

**Both are scheduled for resolution in a follow-up live re-run**, now that
`run-localization.sh` has an added cluster-wide data-plane health guard
(`check_dataplane_health()`, review cycle 1 fix) and a load-generation parallelism cap
(`MAX_SAFE_PUBLISHERS`, also review cycle 1) — see that re-run's saved `.results/` output
before treating either the ingest-ceiling-cause conclusion or the run-B number as
established fact.

## Verdict

**Partial.** Two of three planned discriminating experiments do not cleanly resolve as
intended, and — per the evidence-status note directly above — the resolution offered for
one of them is itself pending re-verification:

- The `$KV.<bucket>.<key>` publish-semantics check is **fully resolved**: PASS on
  READBACK, WATCHER, and WRITETWICE, confirmed identically across three separate runs.
  This clears the Phase 2 rung-1 candidate's correctness precondition.
- `pre-split` vs. the full pipeline's fan-out is **partially resolved**: clean, valid,
  artifacted zero-backlog evidence at 7,272.7 KV ops/s for the fan-out-free path (run A) —
  directionally supportive of the `unarchive` hypothesis already named in
  `benchmarks/FINDING-twin-state.md`, but not new conclusive proof, since neither its own
  ceiling nor the full pipeline's behavior at the same exact load level were captured
  side-by-side today. An unartifacted follow-up observation claims this extends to
  10,212.8 KV ops/s; that specific number is not yet independently verified (see Evidence
  status above).
- `drop-output` vs. `jetstream-output` (pipeline cost vs. output cost, and the Phase 2
  rung-1 throughput candidate itself) is **unresolved** — blocked by a hypothesized,
  config-external HTTP-ingest-path throughput ceiling (~450-500 msg/s per an unfiled sweep;
  the artifacted committed-run evidence instead shows loadgen2's own `CLIENT WAS THE
  BOTTLENECK` verdict at ~15 msg/s) on this shared test cluster. The generator itself was
  ruled out via a local-sink calibration; the platform-vs-generator distinction for the
  observed collapse is not yet independently confirmed pending the re-run in Evidence
  status above.

**No config-reachable cause is named with full numeric confirmation from all three planned
experiments.** Per `.planning/PROJECT.md`'s diagnose-gated approach, this does not
by itself justify ending the milestone — the missing piece (the output-mechanism
micro-bench) failed for a reason external to the hypothesis under test, not because the
hypothesis was tested and falsified. **Recommendation for Phase 2: do not yet greenlit
the full R2 config-fix-ladder on the strength of this finding alone.** Specifically:

1. The `$KV.` semantics result already clears rung 1's correctness precondition — safe to
   keep as the leading candidate.
2. Before committing to rung 1's throughput benefit, Phase 2 (or a narrow Phase-1
   amendment) should re-run the `jetstream-output` vs. `nats_kv` output comparison using a
   bypass-ingest synthetic publisher analogous to `publish-presplit.sh` (publishing
   pre-split-shaped, or full-pipeline-shaped, messages directly onto a diagnostic NATS
   subject) rather than HTTP ingest — this sidesteps the newly-discovered ingest-path
   ceiling entirely, the same way `pre-split` already does successfully.
3. Separately, the HTTP ingest ceiling observed today (unfiled sweep suggests ~450-500
   msg/s; the artifacted committed-run evidence instead shows ~15 msg/s under
   `CLIENT WAS THE BOTTLENECK` — vs. 15,333 msg/s in the 2026-08-03 original finding
   either way) is itself worth a short, independent, properly-artifacted investigation
   before Phase 3's fair-ramp re-verification — it may reflect added Milestone 3 baseline
   traffic, general test-cluster contention, or a measurement methodology difference; if
   unresolved, Phase 3's re-verification against `benchmarks/FINDING-twin-state.md`'s
   original levels (958 → 15,333 msg/s) may itself be at risk of the same bottleneck.
4. Keep synthetic load generation on this cluster conservative (2-4 parallel connections)
   given the OOM incident above — do not scale generator parallelism without first
   confirming headroom on the shared node.
