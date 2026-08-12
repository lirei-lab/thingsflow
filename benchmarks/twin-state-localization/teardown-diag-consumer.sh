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

echo "-- tearing down diagnostic consumer: $CONSUMER_NAME (stream=$STREAM)" >&2

if run_nbox "nats --server '$NATS_URL' consumer info '$STREAM' '$CONSUMER_NAME' >/dev/null 2>&1"; then
  run_nbox "nats --server '$NATS_URL' consumer rm '$STREAM' '$CONSUMER_NAME' -f" || true
  echo "-- deleted $CONSUMER_NAME" >&2
else
  echo "-- $CONSUMER_NAME does not exist, nothing to do" >&2
fi

exit 0
