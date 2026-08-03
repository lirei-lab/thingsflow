# How we measure, and the thirteen defects we found while measuring

This document is the useful residue of four attempts to measure ThingsFlow. None of the
first three produced publishable data, and in every case the reason was the same: **the
instrumentation lied in a different way each time, and always silently.**

It is published because the catalog of ways to fool yourself while measuring is worth
more than any table of results.

---

## The three conditions for a level to count

A load level is only considered valid if all three hold at the same time:

1. **Zero errors and zero loss.** It is not enough for ingest to return 200: we count the
   rows that **landed in the store**, and they must match the accepted count exactly.
2. **The generator was not the bottleneck.** If the client ran out of connections or ran
   out of headroom, the ceiling measured is the client's, not the platform's.
3. **No CPU limit bound during the window.** We read the kernel's real CFS throttling per
   container. A level throttled by its own limit measures the cage.

And a fourth one we added late, after its absence cost us a finding:

4. **No consumer fell behind.** Verifying only the history store lets a level pass as
   "clean" when another route of the same flow is accumulating lag.

---

## The defects we found

### In the instrumentation itself

| Defect | How it showed up |
|---|---|
| **Averaging CPU by dividing by `nr_periods`** | `nr_periods` only advances when the cgroup has runnable tasks, so a bursty container comes out inflated. Within a single window the derived "durations" ranged from 0.2 s to 177.9 s when they should be identical. A physical impossibility gave it away: a consumer spending more CPU under eight times less load |
| **An empty run marked CLEAN** | Zero errors because there were no attempts, zero loss because there was nothing to lose, and the remaining guards skipped over null values. Ten non-existent levels recorded as valid |
| **Contamination between levels** | A consumer was dragging 1,823,148 messages over from the previous level and burned 2,282 m draining them. The level measured on top of it reported 64 % more CPU than it was due |
| **The bench inflating its own state** | The key prefix carried a per-run stamp, so each level wrote 6,000 new KV keys instead of overwriting. After six runs: 35,999 entries where there should have been 6,000. It could manufacture false "lagging consumer" verdicts |
| **A parser reading the wrong field** | It looked for the first `accepted` key at any level of the JSON and found a one-second bucket: it computed the verdict over 125 messages instead of 32,500 |
| **The report misdescribing its own method** | The string that names the landing verification was hardwired to a single store, so a run against a different one came out labeled as counted where it was not |
| **`landed = None` collapsed with "no loss"** | An unverified run passed as valid |
| **Repeating a level added the previous level's rows** | 33,744 expected came out as 67,488 |
| **The client dying at a fixed point** | The server closes the HTTP connection after ~100 requests; the pool did not replenish. With 512 connections the client died at **exactly 51,200 requests** and reported it as platform saturation |
| **A guard that skipped over itself** | `curl -w '%{http_code}'` prints `000` on failure **and also** exits with an error, so `$(curl ... \|\| echo 000)` concatenated into `"000000"` — different from `"000"`, and the check passed |
| **A gate that penalized a threading model** | Counting throttled periods in absolute terms penalizes the JVM, whose GC bursts exceed the quota in some 100 ms period even when the average sits at 43 %. It was invalidating runs that had landed everything with no loss |

### In configuration, not in code

| Defect | Consequence |
|---|---|
| **A YAML key that Helm silently ignores** | The profile wrote `resources.nats` and the chart reads `nats.resources`. Helm gives no warning: NATS ran an entire ramp with its default value, and that ceiling was published as a platform limit. **An override that does not apply is worse than not setting one**: it produces a measurement that looks configured and is not |
| **An isolation script clobbering Helm** | It saved the replica count in an annotation before scaling to zero, and on restore wrote the old value back, undoing any change made in between. Invisible to the key detector, because the key existed and did arrive |

---

## The tools that remained

All of them were born from a specific defect, and all of them are tested **in both
directions** — a check that has only ever been seen green is not evidence of anything.

- **`scripts/throttle-gate.py`** — reads the kernel's CFS throttling per container between
  two instants and classifies the severity. Tested against a pod throttled on purpose:
  `253/253 periods, 100 %, exit=1`.
- **`scripts/verify-effective-limits.py`** — requires every container to have at least 3×
  headroom over its consumption, **reading it from the deployed cluster**. The only thing
  that does not lie is what the kubelet actually applied.
- **`scripts/check-values-keys.py`** — detects overrides that never reach the chart. It is
  a heuristic and it says so: it gives false positives when the template dumps a subtree
  with `toYaml`.
- **`scripts/fair-ramp.sh`** — the ramp: waits until the platform genuinely responds (not
  until its pods are `Running`), isolates each level from the previous one, and records
  the verdict together with its reason.

---

## What cannot be equalized, and is therefore declared

- **Single 16-core node.** No store can be genuinely replicated. The figures **do not
  represent a production deployment with high availability**.
- **`flow-core` pinned to 1 replica** by design (in-memory login throttle and synchronous
  audit writer). It is not on the hot telemetry path, so it does not bound the measured
  throughput, but it is a real limitation and not a test-bench decision.
- **GreptimeDB runs with `sync_write = false`** (verified in the live process's
  `/config`). We do not `fsync` per write. Part of the low latency is durability given up,
  and saying so matters. Nuance: the acknowledgement to the device happens after
  persisting into JetStream, which is file-backed, so the risk window is between the WAL
  and the disk, not the whole message.

---

## On comparing with other platforms

It was attempted and **withdrawn**. Publishing performance figures for another company's
product, measured by us, on our infrastructure, and with its storage backend chosen by us,
is not defensible no matter how clean the methodology ends up being.

The detail that settled the decision: when we audited that comparison, **almost all the
asymmetries we found favored us**. It has an innocent explanation — we instrument the
system we know far better — and that is precisely why the result was not publishable.
