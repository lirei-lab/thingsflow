# ADR 0003 — NATS consumer liveness: progress, not backlog

- **Status:** Accepted
- **Date:** 2026-08-20
- **Context owners:** data plane (NATS/Bento), platform ops
- **Relates to:** `k8s/helm/thingsflow/templates/nats-consumer-guard{,-scripts}.yaml`,
  `greptimedb-freshness-guard.yaml`, `nats-stream-guard-scripts.yaml`, D-07

## Context

On 2026-08-19 a rolling kubelet restart across all four nodes of the production
cluster restarted NATS. Three Bento consumers — `latest-kv`, `alarms` and
`entity-greptimedb` — were left holding dead subscriptions. All three kept
reporting `Running 1/1`. The halt was found only because a human ran
`nats consumer info` by hand; `entity-greptimedb` had been stopped for roughly
seven hours by then.

This is a repeat, not a novelty. `benchmarks/FINDING-twin-state-localization.md`
records the same shape after an earlier NATS OOM-restart: four consumer pods
spinning on `level=error msg="Failed to read message: nats: connection closed"`
**with zero reconnection attempts**, a backlog of 100,921 messages, and
`Active Interest: No interest`.

### Why nothing caught it

**Kubernetes could not.** Every data-plane Deployment already sets
`livenessProbe: /ping` and `readinessProbe: /ready`
(`nats-data-plane-bento.yaml`). Both stayed green. `/ready` is satisfied by a
`nats.Conn` handle that is dead at the *subscription* level; `/ping` only pings
Bento's own HTTP server. Bento 1.8.1 cannot self-report this failure — that is
an observation from the field evidence above, not an assumption: if the input
had surfaced the dead subscription, the reader would have reconnected and
`/ready` would have flipped the pods to `0/1`. Neither happened.

**The existing guard could not.** `greptimedb-freshness-guard` already contains
the right instinct — *"what separates the two is whether there is WORK
WAITING"* — but it probes only the greptimedb durable (the one consumer that did
**not** stall) and uses backlog as a *secondary* signal qualifying a GreptimeDB
freshness query. When `latest-kv` dies, GreptimeDB keeps receiving rows from a
different consumer, so that guard exits 0, green, correctly, and uselessly.

## Decision

Add a dedicated `nats-consumer-guard` CronJob covering **every** JetStream
durable, following the established convention (D-07: the failed Job **is** the
alert; no Prometheus, no ServiceMonitor, no push-gateway).

### Backlog depth is the wrong signal — this is the load-bearing decision

The obvious design is "alert when backlog exceeds a threshold, or grows between
two samples." It is wrong, and it fails in the direction that makes a guard
worthless.

`latest-kv` runs `max_ack_pending: 1` as a deliberate single-writer
serialization mechanism, not a throughput knob: the doc-merge must complete a
whole `cache get → merge → cache set` before the next delivery. Under load it
therefore holds a large — and frequently *growing* — backlog **while working
perfectly**. Measured against a real NATS 2.10.26 server: a sustained backlog of
6,729 messages while the ack floor advanced 61,179 → 100,526 in the same window.
A depth-based rule reports that healthy consumer as dead on every tick. A
`>=`-based "not shrinking" rule is worse still: a saturated-but-healthy consumer
whose arrival rate equals its drain rate holds a *flat* non-zero backlog, which
is the definition of a working queue at capacity.

Two signals are used instead:

1. **`push_bound`** — all durables are push consumers created with an explicit
   `--target` and `--deliver-group`, so the server tracks whether anything is
   subscribed to the deliver subject *right now*. It flips within milliseconds
   of the subscription dying, needs one sample, and cannot false-positive on a
   serialized consumer. This is the `Active Interest: No interest` from the
   incident. It is **necessary but not sufficient**: with more than one replica
   in a deliver group, one dead pod still leaves it `true` — and
   `thingsflow-nats-greptimedb` runs two replicas in production.

2. **`ack_floor.consumer_seq` movement** across two samples — what actually
   separates "serialized but progressing" from "dead". Deliberately
   `consumer_seq`, **not** `stream_seq`: TF_RAW is `discard old` with a
   `max_bytes` cap, so retention deleting messages out from under a *stalled*
   consumer drags its stream-side floor forward and would read as false
   progress.

Two samples are required rather than preferred: NATS 2.10.26 exposes no
`ack_floor.last_active` field (verified against the pinned server — `ack_floor`
carries only `consumer_seq` and `stream_seq`), so there is no stateless
"seconds since last ack" available.

An alert requires the fault in **both** samples. A `helm upgrade` rolls the
Bento pods and leaves a brief window with no subscriber; firing on that would
alert on every deployment, which is how operators learn to ignore a guard.

### Alert only — no auto-remediation

The chart contains no RBAC objects at all. Granting a scheduled Job standing
patch authority over the data plane to restart Deployments is a far larger
change than the guard, and it would be reviewed as one by anyone deploying this.
Beyond that, a guard that fixes the problem and exits 0 destroys the alert
surface D-07 is built on, and during a real NATS outage a self-remediating tick
would restart every consumer every five minutes — a stateless CronJob has no
memory with which to rate-limit itself, so a recoverable outage becomes a
self-inflicted one.

### Which consumers are checked

Live enumeration (`stream ls` → `consumer ls` → `consumer info`) **union** a
render-injected expected set. Live enumeration cannot drift from reality but is
blind by construction to a consumer that *vanished*; the expected set covers
exactly that. A generic Helm `range` over `.Values.natsDataPlane` would be
wrong — that map mixes scalars with the consumer sub-maps, and real enablement
depends on `historyStore`, which the sub-maps do not carry — so the expected set
mirrors the same gates the Bento Deployments use, plus the `alarm-materializer`
durable, which is created by the Go client, lives under a different values key,
and whose Deployment has no probes at all.

## Consequences

- An orphaned durable (component disabled, consumer left behind) now alerts
  forever, correctly: on a capped, discard-old stream an unattended consumer is
  not inert. The `thingsflow-questdb-durable` orphan documented in `values.yaml`
  (grew to 106k pending, never draining) is therefore removed at the source —
  `templates/nats.yaml` gained an `else` branch that `consumer rm`s it when
  `questdb.enabled` is false — rather than being added to an ignore list.
- `monitoring.consumerGuard.ignore` exists as an explicit, reviewable escape
  hatch, never a silent skip.
- The guard cannot see the WebSocket journal fan-out
  (`flow-core/internal/ws/journal.go`): that is a *core* NATS subscription with
  its own resubscribe loop and creates no JetStream consumer. Stated here so
  "every consumer is guarded" is not read as "every subscription is guarded."
- **Every overlay shipped `latestKv.replicas: 2`**, contradicting the
  single-writer invariant documented in `values.yaml` and
  `files/bento-nats-latest-kv.yaml`. This was not one stale file: the cluster,
  pilot and demo overlays all carried it, including the
  `values-*.example.yaml` templates new deployments copy from, so every fresh
  cluster inherited it. Production happened to be running 1, which is why it was
  a landmine for the next clean upgrade rather than a live fault. All are now 1,
  pinned by a test over every tracked values file.

  Whether 2 would in fact be safe is **not settled** — `max_ack_pending` is a
  property of the shared consumer, so the server withholds message N+1 until N
  is acked regardless of which pod acked it, and Bento acks after the cache set.
  That argument is plausible and untested; scaling out belongs to the
  sharded-merge work already open as a milestone criterion, with a measurement.

## Verification

Against a real NATS 2.10.26 + nats-box 0.16.0, using the script extracted from
the **rendered** chart, all four decision branches:

| Scenario | Observed | Verdict |
|---|---|---|
| No subscriber, backlog 93,000 | `push_bound false` | ALERT |
| Subscriber alive, never acking (wedged / dead replica in a group) | `push_bound true`, backlog 4,000, ack floor frozen at 0 | ALERT |
| Expected durable absent | unreadable in both samples | ALERT |
| Serialized `max_ack_pending: 1` under sustained load | backlog 10,974, ack floor 117,109 → 149,520 | **OK** |
| Whole platform healthy under load | all five bound and progressing | OK, exit 0 |

A silent defect was found this way and fixed before shipping: the list-splitting
helper used `printf '%s'` without a trailing newline, so `while read` dropped the
**last** element of every comma-separated list — and the last element of the
rendered expected set is the `alarm-materializer` durable, the one consumer with
no Kubernetes probes of any kind.

### In-cluster reproduction

A **single clean NATS pod restart did not reproduce the stall**: every consumer
reconnected and the guard correctly reported health. That is itself worth
recording — it is why the failure looked intermittent and hard to pin down.

Scaling the NATS StatefulSet to zero for four minutes — long enough to exhaust
the client's reconnect attempts, which is what a node-level disruption produces
and a pod restart does not — reproduced it exactly:

- All five consumers came back `push_bound false`.
- Every Bento pod stayed `Running 1/1` with its restart count **unchanged**, so
  Kubernetes reported the data plane as healthy throughout.
- The guard failed its Job and named all five durables.

`kubectl rollout restart` on the five Deployments restored every subscription
and the next tick returned to `exit 0`, confirming the remedy in the operations
playbook.

The reproduction recipe matters as much as the guard: it is the only known way
to exercise this failure on demand, and a pod restart — the obvious thing to
try — does not do it.
