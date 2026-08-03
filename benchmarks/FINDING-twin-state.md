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
