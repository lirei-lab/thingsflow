#!/usr/bin/env bash
# Deletes the ephemeral diagnostic JetStream consumer created by
# bootstrap-diag-consumer.sh. Idempotent: exits 0 even if the consumer does not
# exist (e.g. bootstrap failed before creating it, or teardown already ran).
#
# Usage: teardown-diag-consumer.sh <variant-name>
set -uo pipefail  # deliberately NOT -e: this script must always report success on
                   # a best-effort cleanup, never abort a caller's trap sequence.

VARIANT="${1:?usage: teardown-diag-consumer.sh <variant-name>}"

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"
STREAM="${STREAM:-TF_RAW}"

CONSUMER_NAME="thingsflow-latest-kv-diag-${VARIANT}"

kc() {
  kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"
}

run_nbox() {  # run_nbox <sh -c command string>
  local podname="diag-teardown-$RANDOM"
  kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "$1"
}

# Finding 2 (Phase 1 review cycle 1): the orphaned-consumer incident during
# this phase's own execution happened because the old version of this check
# gated the delete decision purely on `consumer info`'s exit code, treating
# ANY failure (including "NATS unreachable") as "consumer does not exist,
# nothing to do" — teardown ran while NATS was mid-OOM-restart, silently
# reported success, and left thingsflow-latest-kv-diag-pre-split-boost
# orphaned. This distinguishes a genuine "not found" from any other failure
# mode and aborts loudly on the latter instead of assuming cleanup succeeded.
check_consumer_state() {  # -> stdout: "FOUND"|"NOT_FOUND"|"AMBIGUOUS|<detail>"
  # Review cycle 2 fix: this function runs inside a subshell whenever it's
  # invoked via command substitution ($(...)) at the call site, so a global
  # variable assignment made here (the old CHECK_DETAIL="$out") never
  # propagates back to the caller — every AMBIGUOUS abort message printed
  # "Detail: " with nothing after it. Encode state and detail together on
  # stdout, delimited, and split at the call site instead.
  local podname="diag-check-$RANDOM"
  local out
  out="$(kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "nats --server '$NATS_URL' consumer info '$STREAM' '$CONSUMER_NAME' 2>&1; echo NATS_EXIT=\$?" 2>&1)"
  local nats_exit
  nats_exit="$(printf '%s\n' "$out" | sed -n 's/^NATS_EXIT=//p' | tail -1)"
  if [[ "$nats_exit" == "0" ]]; then
    echo "FOUND"; return 0
  fi
  # Review cycle 3 fix: the pinned nats-box:0.16.0 CLI does NOT print any of
  # the previously-assumed "consumer not found" style text for a nonexistent
  # consumer — even with a stream and an exact consumer name supplied, a
  # missing consumer makes it fall into an interactive picker that then fails
  # non-interactively with a misleading, generic message. Empirically
  # confirmed live (reproduced twice, independently) — this is what "does not
  # exist" actually looks like on this CLI version, not a real ambiguous
  # failure:
  #   nats: error: could not select Consumer: cannot pick a Consumer without
  #   a terminal and no Consumer name supplied
  if printf '%s\n' "$out" | grep -qiE 'consumer not found|no such (consumer|stream)|nats: error: consumer|cannot pick a consumer without a terminal'; then
    echo "NOT_FOUND"; return 0
  fi
  echo "AMBIGUOUS|$out"; return 0
}

echo "-- tearing down diagnostic consumer: $CONSUMER_NAME (stream=$STREAM)" >&2

state_line="$(check_consumer_state)"
state="${state_line%%|*}"
CHECK_DETAIL="${state_line#*|}"
case "$state" in
  FOUND)
    if run_nbox "nats --server '$NATS_URL' consumer rm '$STREAM' '$CONSUMER_NAME' -f" >/dev/null 2>&1; then
      echo "-- deleted $CONSUMER_NAME" >&2
    else
      echo "AMBIGUOUS_STATE: consumer rm failed for $CONSUMER_NAME after confirming it exists — verify manually" >&2
      exit 1
    fi
    ;;
  NOT_FOUND)
    echo "-- $CONSUMER_NAME does not exist, nothing to do" >&2
    ;;
  AMBIGUOUS)
    echo "AMBIGUOUS_STATE: could not determine whether $CONSUMER_NAME exists — treating as a FAILED teardown, not a no-op." >&2
    echo "  Detail: $CHECK_DETAIL" >&2
    echo "  Verify manually: kubectl --context=$KUBECTL_CONTEXT -n $NAMESPACE run diag-verify-\$RANDOM --rm -i --restart=Never --image=$NATS_IMAGE --command -- nats --server '$NATS_URL' consumer info '$STREAM' '$CONSUMER_NAME'" >&2
    exit 1
    ;;
esac

exit 0
