# The latest-values writer suffers congestion collapse above ~3,900 msg/s

**Discovered:** 2026-08-03, fair ramp v4
**Status:** measured · **serialization localized to the Bento consumer** · no fix applied

## The fact

Real throughput of the twin state writer, computed as
`(accepted − pending at level close) / duration`:

| offered | KV processes | pending at close | KV CPU/pod | history |
|---:|---:|---:|---:|---|
| 958 msg/s | 958/s | 0 | 352 m | complete |
| 1,917 msg/s | 1,917/s | 0 | 507 m | complete |
| 3,833 msg/s | 3,777/s | 10,147 | 717 m | complete, exactly 2,070,000 rows |
| 7,667 msg/s | **3,964/s** | 666,456 | 622 m | complete, exactly 4,140,000 rows |
| 15,333 msg/s | **2,914/s** | 2,235,393 | 509 m | complete, exactly 8,280,000 rows |

Two things, and the second is the serious one:

1. **It saturates above ~3,900 msg/s** (some 11,700 KV operations/s at 3 keys per
   message).
2. **Beyond that point it does not plateau: it goes backwards.** Doubling the load from
   7,667 to 15,333 msg/s makes throughput **drop by 26 %**. That is congestion collapse,
   not saturation: the system does *less* work the more it is asked to do.

**It is not a lack of CPU, and the CPU itself proves it.** The replicas run at 36 % of
their 2,000 m quota with no throttling, and — the revealing part — **their CPU falls
together with the throughput** (717 → 622 → 509 m). Less CPU doing less work with more
offered load means the process is *waiting*, not computing.

Across all five levels the history landed **complete and verified**, with the rows
matching exactly what was accepted.

## Why it matters in production

The twin state is what the UI shows as each device's **latest value**. A lag there leaves
the operator looking at stale readings while the history is perfect. It is the worst kind
of failure: **nothing looks broken**. The history charts are complete, there are no errors
in the logs, ingest answers 200, and the big number on the screen has not moved for
minutes.

And because it collapses instead of degrading gracefully, the problem **gets worse exactly
when the load is highest** — the moment when an operator most needs to see fresh data.

No earlier benchmark detected it, because all of them verified landing **only against
GreptimeDB**. The level came out "clean" with the history complete and the twin state
behind.

## Confirmed on two independent entry paths

The same ceiling appears coming in over MQTT and over HTTP, which use different brokers,
authentication and ingest routes. All they share is the consumer:

| offered | MQTT processes | MQTT cpu/pod | HTTP processes | HTTP cpu/pod |
|---:|---:|---:|---:|---:|
| 958/s | 958/s | 352 m | 958/s | 326 m |
| 1,917/s | 1,917/s | 507 m | 1,917/s | 340 m |
| 3,833/s | 3,777/s ✗ | 717 m | **3,833/s ✓** | 507 m |
| 7,667/s | 3,964/s ✗ | 622 m | 3,516/s ✗ | 515 m |

**Hypothesis ruled out: that it was specific to the MQTT path.** At 3,833 msg/s the HTTP
path keeps up exactly and the MQTT one falls 56 msg/s short, but at 7,667 **both collapse**
to an equivalent throughput (3,964 and 3,516/s). The ceiling belongs to the KV writer, not
to the path.

The MQTT path is consistently 20-40 % more expensive per message in the consumer, which
leaves it just on the wrong side of the limit at 3,833. That fits with the mapping
decoding the JWT from `from_username` (base64url + `parse_json`) on every MQTT message,
whereas over HTTP that field comes in empty and the decode is skipped. **But that does not
explain the collapse**: the history writer pays exactly the same decode and sustains
≥16,000 msg/s.

## What has already been ruled out

**Hypothesis: contention in NATS.** If the writer is blocked on round trips, it should
degrade when NATS is busier. **The data contradict it:**

| offered | NATS CPU | KV CPU/pod | KV throughput |
|---:|---:|---:|---:|
| 3,833/s | 1,934 m | 717 m | 3,777/s |
| 15,333/s | **1,646 m** | 509 m | **2,914/s** |

NATS consumes *less* CPU at the level where the KV collapses, and it is at 27 % of its
6,000 m quota. It is not saturated. **Hypothesis ruled out.**

**Also ruled out:** consumer CPU, its quota (36 %), CFS throttling (zero), and the replica
count (there are 3 and all of them are underused).

## The structural difference from the writer that does scale

| | history | latest values |
|---|---|---|
| Bento output | `http_client` | `nats_kv` |
| batching | `batching: count 1000, period 500ms` | **none** |
| write unit | 1 request = 1,000 data points | 1 operation = 1 key |
| CPU/pod at 15,333 msg/s | **733 m and rising** | 509 m and falling |

The history is the **only component that scales with load**, and it is precisely the one
that batches. The KV does one operation per key: each message fans out to
`DEVICE.<tenant>.<device>.telemetry.<key>`.

`max_in_flight: 1024` is configured and *should* pipeline them in parallel. **It does not
achieve that**, and the next section measures it.

## The declared concurrency is 1024. The effective one is 1.4

Test run: the same 8,000 msg/s level, changing **only** `max_in_flight`.

| `max_in_flight` | KV processes | pending at close | CPU/pod |
|---:|---:|---:|---:|
| 1024 | 4,058/s | 649,558 | 587 m |
| **1** | **2,936/s** | 851,492 | 480 m |

Dropping the concurrency from 1024 to 1 costs **only 28 %** of throughput. That rules out
both extreme hypotheses at once:

- **It is not that the setting does nothing.** There is a measurable difference, so the
  concurrency is applied.
- **But it does not work as declared.** If 1024 operations were genuinely parallel, going
  to 1 should sink the throughput by orders of magnitude, not by 28 %.

**Effective concurrency = 4,058 / 2,936 ≈ 1.38×.** 1024 is declared and less than 1.4 is
obtained. That is where the bottleneck is: something serializes the writes inside the
consumer.

### Ruled out: the KV store. Measured directly

`nats bench --kv` against a clean bucket, **with our pipeline out of the way**:

| concurrent publishers | throughput |
|---:|---:|
| 1 | 4,634 ops/s |
| 8 | **31,568 ops/s** (6.8×, almost linear) |

**The KV bucket parallelizes well.** It holds ~31,500 operations/s while our pipeline
manages about 12,000 (4,058 msg/s × 3 keys). The store is not the bottleneck, and the
hypothesis that a single stream serialized the writes is **ruled out**.

### Where it is, then: inside the consumer

The figure that settles it: **each Bento pod delivers about 4,000 ops/s — almost exactly
what ONE serial publisher gives** (4,634/s). With `max_in_flight: 1024` declared.

That is, each replica behaves as if it emitted the writes **one at a time**. That fits
everything measured: that raising the declared concurrency barely helps (+38 %), that
widening the consumer window does nothing, and that adding replicas scales sublinearly.

A specific candidate to review in the pipeline: `unarchive` turns one message into N (one
per telemetry key), and those N appear to be dispatched serially within the same batch
despite the global `max_in_flight`.

### Other tests run and their results

| test | result | conclusion |
|---|---|---|
| `max_in_flight` 1024 → 1 | 4,058 → 2,936/s (−28 %) | the effective concurrency is ~1.4, not 1024 |
| replicas 3 → 6 | 4,058 → 5,085/s (+25 %) | sublinear; per pod it falls from 1,353 to 847/s |
| `maxAckPending` 1024 → 8192 | 4,058 → 4,101/s | **no effect**; the consumer window does not bind |
| direct KV, 1 → 8 clients | 4,634 → 31,568 ops/s | the store is not the bottleneck |

### What NOT to do yet

Replacing the one-key-per-entry model with a one-document-per-device model would reduce
the operations to the number of messages instead of the number of keys, and would probably
resolve the symptom. **But it changes the read contract** of everything that consumes twin
state, and doing it before confirming the cause would mean blindly fixing something that
may yet be resolved with a pipeline adjustment.

## What must NOT be concluded

- **That ThingsFlow "only handles 3,900 msg/s".** The history sustains ≥16,000 msg/s
  verified with zero loss, and ingest accepts 15,333/s without a single error. What
  collapses is one specific route.
- **That there is data loss.** There is none. The history is complete across all five
  levels. What degrades is the *freshness* of the latest value, not its persistence: the
  messages remain in the stream and are processed later.
- **That it is fixed with more replicas or more CPU.** They are at 36 % of their quota and
  their consumption *falls* as the problem gets worse.

## Post-Fix: rung 1 measured end-to-end — the collapse persists

**Measured:** 2026-08-13, fair ramp re-run (Milestone 2, Phase 3, plan 03-01)
**Change under test:** rung 1 — `output.nats_kv` → `output.nats_jetstream` publishing to
`$KV.${NATS_KV_BUCKET}.${! metadata("kv_key") }` in
`k8s/helm/thingsflow/files/bento-nats-latest-kv.yaml`, applied to `thingsflow-fresh`
on 2026-08-13. Only the `output:` block changed. See
`benchmarks/FINDING-twin-state-rung1-verification.md` for the GO verdict that
authorised it, and for that finding's own explicit caveat — its +20.8% was measured
with the **output stage in isolation** (an in-process unthrottled generator feeding a
trivial one-stage pipeline), **not** the full 5-stage production pipeline with real
ingest. This section is the test of that caveat.

**Verdict up front: ≥8,000 msg/s sustained is NOT achieved, on either protocol.**
Rung 1 produced no end-to-end improvement distinguishable from run-to-run variance.

### Results — MQTT (primary sweep)

| offered | KV processes | pending at close | KV CPU/pod | history |
|---:|---:|---:|---:|---|
| 898 msg/s | 898/s | 0 | 281 m | complete, exactly 323,316 rows |
| 1,797 msg/s | 1,797/s | 0 | 357 m | complete, exactly 646,980 rows |
| 3,593 msg/s | 3,593/s | 0 | 485 m | complete, exactly 1,293,636 rows |
| 7,188 msg/s | **3,856/s** | 399,819 | 546 m | complete, exactly 2,587,608 rows |
| 14,373 msg/s | **2,626/s** | 1,409,857 | 353 m | complete, exactly 5,174,880 rows |

### Results — HTTP (primary sweep)

| offered | KV processes | pending at close | KV CPU/pod | history |
|---:|---:|---:|---:|---|
| 898 msg/s | 898/s | 0 | 193 m | complete, exactly 323,316 rows |
| 1,797 msg/s | 1,797/s | 0 | 275 m | complete, exactly 646,980 rows |
| 3,593 msg/s | 3,593/s | 0 | 371 m | complete, exactly 1,293,636 rows |
| 7,188 msg/s | **3,374/s** | 457,658 | 442 m | complete, exactly 2,587,608 rows |
| 12,226 msg/s ✗ | **2,205/s** | 1,202,663 | 354 m | complete, exactly 4,401,597 rows |

✗ The top HTTP level did not reach its offered rate: the generator itself failed
257,761 sends with `pool_full` and was marked `generator-limited`. **This is not new
and not a property of the fix** — the original 2026-08-03 run hit the same wall on the
same path (`thingsflow-http-16000`: 547,115 errors, also INVALID), which is why the
original finding's own two-entry-path table stops at 7,667 msg/s for HTTP.

`processed` uses the original finding's identical formula,
`(accepted − pending at level close) / duration`. History landing was verified against
GreptimeDB at every level and matched the expected row count **exactly, with zero loss,
on all ten** — the same "history perfect, freshness behind" signature the original
documented. The lagging consumer was `thingsflow-latest-kv-durable` in every case.

### Level validity, stated honestly

Per `benchmarks/README.md`'s rule, the three top levels are **INVALID as sustained
levels** — not because the measurement failed, but because a consumer did not keep up,
which is precisely the defect this document exists to record. Their verdicts, verbatim
from `benchmarks/results-fair/verdicts.tsv`:

```
thingsflow-mqtt-7667    INVALID  lagging consumer: thingsflow-latest-kv-durable with 399819 pending
thingsflow-mqtt-15333   INVALID  lagging consumer: thingsflow-latest-kv-durable with 1409857 pending
thingsflow-http-7667    INVALID  lagging consumer: thingsflow-latest-kv-durable with 457658 pending
thingsflow-http-15333   INVALID  errors=257761;generator-limited(blocked=257761);lagging consumer: ... 1202663 pending
```

The six lower levels came out **CLEAN**. Every one of the ten levels passed the
CPU-throttling gate (`GATE OK — no limit was binding during the measured window`), so
the observed ceiling is the platform's and not the cage's. The generator was never the
bottleneck on MQTT (peak 1.9 of 16 cores).

### Reproduced: each INVALID level was re-run once

| level | primary sweep | retry | agreement |
|---|---:|---:|---|
| mqtt 7,188 | 3,856.0/s (399,819 pending) | 3,856.3/s (399,774 pending) | within 0.01% |
| mqtt 14,373 | 2,625.9/s (1,409,857) | 3,034.3/s (1,360,839) | +15.6% run-to-run |
| http 7,188 | 3,374.0/s (457,658) | 3,758.2/s (411,551) | +11.4% run-to-run |
| http 12,2xx | 2,204.5/s (1,202,663) | 2,451.3/s (1,169,131) | +11.2% run-to-run |

Every retry reproduced the same verdict and the same failure mode. The saturated levels
carry real run-to-run variance (±10-16%), which is why the verdict below is stated
against the **best** observation across both runs, not the worst.

### The ≥8,000 msg/s verdict, per protocol

The criterion can only be decided at the top offered level, since processed ≤ offered.

| protocol | best processed at the top level | target | verdict |
|---|---:|---:|---|
| **MQTT** | **3,034 msg/s** | 8,000 msg/s | **NOT ACHIEVED** — 37.9% of target |
| **HTTP** | **2,451 msg/s** | 8,000 msg/s | **NOT ACHIEVED** — 30.6% of target |

Measured change against the original finding's number at the corresponding level
(original MQTT 2,914/s at 15,333 offered; original HTTP 2,084/s at 12,294 offered):

- **MQTT: −9.9% (primary sweep) to +4.1% (retry); mean of the two runs −2.9%.**
- **HTTP: +5.8% (primary sweep) to +17.6% (retry); mean +11.7%** — but both the
  original and this run are `generator-limited` at that level, so neither side of that
  comparison is a clean platform measurement.

**There is no improvement here that survives the run-to-run variance.** The peak
processed rate observed anywhere in this campaign — across 8 saturated observations on
2 protocols — is **3,856 msg/s**, against the original finding's 3,964 msg/s. The
ceiling has not moved.

The original's two diagnostic signatures both reproduce intact:

1. **It still goes backwards.** MQTT falls from 3,856/s at 7,188 offered to 2,626/s at
   14,373 offered. Doubling the load still makes throughput *drop*. Congestion collapse,
   not saturation.
2. **CPU still falls with throughput.** MQTT 546 m → 353 m and HTTP 442 m → 354 m as
   offered load doubles and processed rate falls. Less CPU doing less work under more
   load: the consumer is still *waiting*, not computing — at 18-27% of its 2,000 m quota,
   with zero CFS throttling measured.

### TF_TWIN_EVENTS baseline traffic: isolated, measured at exactly zero

`PROJECT.md` requires the Milestone 3 twin-event journal to be isolated **or accounted
for**. It is isolated, and this is measured rather than assumed:

- **Idle baseline** (3 non-contiguous 60 s samples, ~3 min apart, before any load):
  `last_seq` 0 → 0 on all three. **Range 0–0 events/60 s.**
- **Per level during the ramp**: `last_seq` was bracketed before and after each of the
  ten levels. **Delta = 0 at every level, on both protocols.** Across the entire
  42-minute run the stream's `last_seq` never left 0.

So the comparison against the original (which predates the journal entirely) needs no
adjustment: the journal contributed no traffic at all. This is consistent with
`flow-core/internal/twinevents/publisher.go`'s `Publish()` firing only on control-plane
twin-API saves — `loadgen2`'s device provisioning does not reach it.

### Rung 1 stayed deployed throughout

The premise was re-checked at start, midpoint and end, plus 8 automated checks every
5 minutes during the run. All confirmed `Output type nats_jetstream is now active` on
**all three** `thingsflow-nats-latest-kv` replicas, release `thingsflow` revision 4:

| checkpoint | time (UTC) | result |
|---|---|---|
| START | 2026-08-13T16:14:50Z | PASS — 3/3 replicas, deploy 3/3/3, live ConfigMap carries the rung-1 `output:` block |
| MIDPOINT (after the 5 MQTT levels) | 2026-08-13T16:46:25Z | PASS — 3/3 replicas, helm rev 4 |
| END | 2026-08-13T17:06:42Z | PASS — 3/3 replicas, helm rev 4 |

No drift was detected at any point.

### What this measurement is, and its limits

- **The cage was restored, but not byte-exactly.** `benchmarks/profiles/fair-thingsflow.yaml`
  was applied before this run (latestKv 3 × 2,000 m, nats 6,000 m, rmqtt 3 × 2,000 m raft,
  http-ingest 3 × 3,000 m, greptimedb 6,000 m, flow-core 3,000 m), matching the resource
  shape the original finding describes. Four deviations were unavoidable and are recorded
  for honesty: `images.flowCore` was pinned to the running image (the chart has no such
  key, so the profile's `""` would have broken flow-core), and the NATS PVC (20Gi vs 10Gi),
  postgres PVC (10Gi vs 8Gi) and `storageClassName` were left as deployed because
  `volumeClaimTemplates` are immutable — the first upgrade was rejected server-side. The
  two PVCs are *larger* than the profile asks, and none of the four touches the CPU cage
  the comparison depends on.
- **The profile is a reconstruction, not a documented guarantee.** This finding's original
  text never names `fair-thingsflow.yaml`; the link comes from `benchmarks/README.md`'s
  reproduction procedure. It is the best available reconstruction of the original
  measurement conditions.
- **The offered levels are ~6.3% below the original's.** The original's 958/1,917/3,833/
  7,667/15,333 figures are *achieved* rates produced by nominal `RATES="1000 2000 4000
  8000 16000"` at `DURATION=180` (achieved = nominal × (180−7.5)/180). This run used the
  nominal rates 958…15,333 at the harness's current default `DURATION=120`, giving
  achieved = nominal × (120−7.5)/120 — 898…14,373. Because the system exhibits congestion
  collapse, offered→processed is non-monotonic, so a lower offered level is not
  conservative in a single direction; the shortfall against 8,000 msg/s (a factor of
  2.6-3.3×) is far too large for this 6.3% gap to explain.
- **Ambient load.** An unrelated job (`ecvxlayer.experiment_capacity`) held ~1.6-2.2 of
  the node's 16 cores throughout. The throttle gate passed at every level and the
  generator was never the bottleneck on MQTT, so this did not cap the result.

### What this means: `unarchive`, not the output, is the binding constraint

Rung 1 was the last config-reachable rung. Phase 2's isolated micro-benchmark was right
that `nats_jetstream` is a faster output mechanism (+20.8% with every upstream constraint
removed) — and this measurement shows that **at the production pipeline's operating point
that advantage is invisible, because the output was never what was binding.**

That is exactly the outcome `benchmarks/FINDING-twin-state-localization.md` (Phase 1)
predicted. Its pre-split variant — the same consumer with the `unarchive` fan-out removed
— reached 7,700-10,400 msg/s, against the full pipeline's ~3,900 msg/s. This run puts the
full pipeline at 3,856 msg/s with the faster output already in place. **The gap between
those two numbers is the `unarchive` fan-out stage, and it is still there.**

That also settles this document's own open question. The original section above named
`unarchive` as "a specific candidate to review" and measured effective concurrency at
≈1.4 against a declared `max_in_flight: 1024`. Swapping the output did not move that
number, which is what would be expected if the serialization lives upstream of the output,
in the per-key fan-out, rather than in the write itself.

### Consequences for the milestone

Per `PROJECT.md`, the config-only fix ladder is now **exhausted**: rung 1 is applied and
measured, and rung 2 was never evidence-supported (Phase 2 declined it). The remaining
candidate — replacing the one-key-per-entry model with a **one document per device** model,
which would cut operations from the number of keys to the number of messages — is the
option this document's own "What NOT to do yet" section deliberately reserved, and it is
**explicitly out of scope for this milestone** because it changes the read contract of
every twin-state consumer (`twinstore`, the WebSocket plane, the UI).

The honest position is therefore: **the cause is now localized with much higher
confidence than before, and the fix for it is a scoped piece of future work, not a
configuration change.** Nothing in this run contradicts the original finding; it
strengthens it.

### What must NOT be concluded from this section

- **That rung 1 was a mistake or should be reverted.** It is a strictly faster output
  mechanism, measured four independent times, and it costs nothing. It is simply masked
  by a larger upstream bottleneck. This run gives no evidence for a rollback.
- **That the platform lost throughput.** It did not. Ingest still accepted 14,373 msg/s
  on MQTT with zero errors, and the history still landed complete and exact — 5,174,880
  rows at the top level — at every one of the ten levels.
- **That ≥8,000 msg/s is unreachable.** It was not reached *by a configuration change to
  the output stage*. Phase 1's pre-split evidence (7,700-10,400 msg/s with the fan-out
  removed) is direct evidence that the target is reachable by addressing `unarchive`.

### CPU floor re-pin: measured, but NOT applied — the pinned condition could not be reproduced

The re-pin of `tools/python/test_data_plane_cpu_headroom.py` was measured and then
**deliberately not applied.** The measurement itself was clean:

**Reference load** (identical to the one the pinned values already use): 3,000 msg/s
nominal MQTT, 500 devices, `DURATION=120 WARMUP=45 SETTLE=30`, ramp 15, qos 1, drained
and KV-reset beforehand via the harness's own `wait_drained`/`tf_reset_state`. Achieved
2,812 msg/s offered, 337,500 accepted, **0 failed**, landing verified exact
(1,012,500 / 1,012,500 rows). Generator at 0.48 of 16 cores — not the bottleneck.

**`throttle-gate.py check` → `GATE OK`, rc=0: 47 of 47 containers, zero throttled
periods.** Nothing was binding.

| component | pinned value | replicas then | measured now (mean/pod) | replicas now | aggregate now |
|---|---:|---:|---:|---:|---:|
| `nats` | 1,432 m | 1 | **923 m** | 1 | 923 m |
| `latestKv` | 1,632 m | 1 | 314 m | 3 | 942 m |
| `greptimedb` | 725 m | 1 | 188 m | 3 | 564 m |
| `alarms` | 724 m | 1 | 170 m | 3 | 511 m |

**Why it was not applied.** `MEASURED_DRAW_M` and `floors` are asserted against
`k8s/helm/thingsflow/values.yaml`, whose shipped default is **1 replica** per consumer
(`latestKv` at a 3,000 m limit). They were originally measured under exactly that
condition. This run necessarily happened under the **benchmark cage** — the fair profile's
**3 replicas at 2,000 m** — which the ramp above required and which this plan is not
authorised to change. Every consumer figure above is therefore one-of-three-replicas
carrying roughly a third of the load, not the single shipped replica carrying all of it.

Applying the numbers mechanically would have set `MEASURED_DRAW_M["latestKv"]` to 314 m
and `floors["latestKv"]` to the deployed 2,000 m. Both are **lower** than the current
pins, and the combination would allow `values.yaml`'s `latestKv` limit to fall from
3,000 m to 2,000 m while the test still passed — at a limit this very file documents as
throttling **15.1% of CFS periods** at this exact load. That is precisely the silent
drift the test exists to prevent, so the pins were left untouched and the shortfall is
recorded here instead. `tools/python/test_data_plane_cpu_headroom.py` is unmodified and
still passes.

**What the measurement does support**, stated without over-reaching: `nats` is the one
component whose replica count is the same now as when it was pinned (1), and it drew
**923 m against a pinned 1,432 m** at a 6% lower achieved rate — consistent with rung 1
making the KV write path cheaper for the broker, which is what Phase 2's isolated
benchmark predicted. The aggregate `latestKv` draw of 942 m against a pinned 1,632 m
points the same way. Neither is a substitute for a same-topology re-measurement.

**To complete the re-pin**, a future plan needs the consumers at `values.yaml`'s own
1-replica topology and limits for the duration of a single 3,000 msg/s run — a Helm
release change, and therefore its own authorisation.
