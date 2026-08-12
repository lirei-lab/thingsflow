#!/usr/bin/env bash
# Synthetic publisher for the Phase 1 `pre-split` diagnostic variant. Publishes
# pre-split (already one-key-per-message) JSON directly onto the diagnostic-only
# subject tf.ingest.http.raw.diag-presplit.>, at the fastest rate a small pool of
# persistent nats-box connections can sustain, and prints the ACTUAL elapsed
# publish rate achieved (messages / wall-clock seconds) so run-localization.sh can
# flag a generator-limited run per benchmarks/README.md's discipline ("the
# generator was not the limit" is one of the five conditions a level must meet).
#
# Design note (verified live against this cluster's pinned natsio/nats-box:0.16.0
# before writing this script, per the plan's instruction to verify --help first):
#   - `nats pub --help` documents template macros (Count/TimeStamp/Unix/UnixNano/
#     Time/ID/Random(min,max)) for the BODY and header values ONLY — the help text
#     explicitly says "Body and Header values ... may use Go templates", subject
#     templating is not documented.
#   - CONFIRMED LIVE: templating the SUBJECT itself (e.g.
#     "test.presplit.perf.{{ Random 1 50 }}") hung indefinitely (no progress
#     output, no error, no completion within 30s for even 100 messages) — this
#     is the "does not support these template macros" case the plan told this
#     script to fall back from. Templating the BODY only, against a FIXED
#     subject, worked and reached ~4,500 msg/s from a single connection, ~10,900
#     msg/s from 2 parallel connections in one pod (measured live).
#   - This is exactly the plan's documented fallback: "a small pool (not
#     one-per-message) of persistent `nats pub --count` processes run in
#     parallel via &/wait, each handling a distinct sub-range, still avoiding
#     per-message process spawn." The fixed subject is safe because the
#     pre-split.yaml consumer's filter subject
#     (tf.ingest.http.raw.diag-presplit.>) is a wildcard subtree — the exact
#     leaf name under it does not matter, only that it stays under that subtree
#     (never real device traffic).
#
# Env vars (all optional, sane defaults matching the diagnostic rate range near
# the ~3,900 msg/s ceiling from benchmarks/FINDING-twin-state.md):
#   COUNT        total messages to publish (default 240000 => ~4,000 msg/s * 60s
#                at the measured 2-connection throughput)
#   PUBLISHERS   number of parallel nats-box connections in the pool (default 2)
#   SUBJECT      fixed diagnostic subject (default tf.ingest.http.raw.diag-presplit.load)
#   NATS_URL     (default nats://thingsflow-nats:4222)
#   NAMESPACE, KUBECTL_CONTEXT, NATS_IMAGE — cluster access, same defaults as the
#                other scripts in this directory.
#
# Prints, to stdout, a single machine-parseable summary line:
#   PUBLISH_RESULT count=<n> elapsed_s=<n> rate_msg_s=<n>
set -uo pipefail

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"
SUBJECT="${SUBJECT:-tf.ingest.http.raw.diag-presplit.load}"
COUNT="${COUNT:-240000}"
PUBLISHERS="${PUBLISHERS:-2}"
POD_TIMEOUT="${POD_TIMEOUT:-600}"

kc() {
  kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"
}

# Split COUNT as evenly as possible across PUBLISHERS (integer division; any
# remainder goes to the last publisher so the total is exact).
PER_PUB=$(( COUNT / PUBLISHERS ))
REMAINDER=$(( COUNT - PER_PUB * PUBLISHERS ))

echo "-- publish-presplit: subject=$SUBJECT count=$COUNT publishers=$PUBLISHERS (per-pub=$PER_PUB, +$REMAINDER on last)" >&2

# Build the in-pod script: PUBLISHERS background `nats pub --count` processes,
# each with its OWN persistent connection (not one process per message — a
# fresh connect/handshake per message tops out at low hundreds of msg/s and
# cannot reach the diagnostic range, per benchmarks/scripts/run-nats-benchmark.sh's
# precedent of using nats's own multi-publisher primitives instead of a
# process-per-message loop). Body templating gives each message a distinct
# kv_key (spread across 50 diagnostic keys) and a real Unix timestamp; value is
# the per-connection message counter.
POD_SCRIPT="START=\$(date +%s)
"
for i in $(seq 1 "$PUBLISHERS"); do
  this_count="$PER_PUB"
  if [[ "$i" -eq "$PUBLISHERS" ]]; then
    this_count=$(( PER_PUB + REMAINDER ))
  fi
  POD_SCRIPT+="nats --server '$NATS_URL' pub '$SUBJECT' '{\"kv_key\":\"DEVICE.diag-tenant.diag-device.telemetry.presplit_k{{ Random 1 50 }}\",\"ts\":{{Unix}},\"value\":{{Count}}}' --count $this_count >/tmp/pub$i.log 2>&1 &
P$i=\$!
"
done
POD_SCRIPT+="
"
for i in $(seq 1 "$PUBLISHERS"); do
  POD_SCRIPT+="wait \$P$i
"
done
POD_SCRIPT+="END=\$(date +%s)
ELAPSED=\$(( END - START ))
[ \"\$ELAPSED\" -lt 1 ] && ELAPSED=1
echo \"PUBLISH_RESULT count=$COUNT elapsed_s=\$ELAPSED rate_msg_s=\$(( $COUNT / ELAPSED ))\"
"

PODNAME="diag-presplit-pub-$RANDOM"
OUT="$(kc run "$PODNAME" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
  sh -c "$POD_SCRIPT" < /dev/null 2>/dev/null)"

echo "$OUT" >&2

RESULT_LINE="$(printf '%s\n' "$OUT" | grep '^PUBLISH_RESULT' || true)"
if [[ -z "$RESULT_LINE" ]]; then
  echo "ERROR: publish-presplit.sh did not observe a PUBLISH_RESULT line from the pod (pod may have failed or timed out)" >&2
  exit 1
fi

echo "$RESULT_LINE"
