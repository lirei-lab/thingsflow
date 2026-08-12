#!/usr/bin/env bash
# Creates an ephemeral, uniquely-named JetStream consumer for a Phase 1 diagnostic
# variant, WITHOUT disturbing the production thingsflow-latest-kv-durable consumer.
#
# Usage: bootstrap-diag-consumer.sh <variant-name> <filter-subject>
#
# Creates durable consumer "thingsflow-latest-kv-diag-<variant-name>" on stream
# TF_RAW (deliver all, ack explicit, mirroring the production consumer's
# MaxDeliver=500 / AckWait=30s from k8s/helm/thingsflow/templates/nats.yaml's
# converge_consumer bootstrap pattern / values.yaml natsDataPlane.latestKv).
#
# WR-05 (see k8s/helm/thingsflow/templates/nats.yaml lines ~369-377): the pinned
# nats-box:0.16.0 CLI's `consumer add --deliver-group` WITHOUT --target hangs
# waiting on stdin in a non-interactive script ("could not request delivery
# target: cannot prompt for user input without a terminal"). Both flags are
# passed together below.
#
# Idempotent: if a consumer with this name already exists (leftover from a prior
# interrupted run), it is deleted and recreated — mirrors converge_consumer's
# delete-and-recreate pattern rather than failing on a name collision.
#
# Deliver policy: `new`, not `all`. The pre-split variant's diagnostic subtree
# (tf.ingest.http.raw.diag-presplit.>) is a subject on the SAME TF_RAW stream
# real device traffic and prior interrupted-run leftovers also land on — a
# `--deliver all` consumer replays the stream's ENTIRE historical backlog on
# that subject before it sees a single message from the current run, which is
# exactly the contamination that invalidated the 2026-08-11T205044Z run (124,000
# Unprocessed Messages already queued before any load was sent, see
# .results/pre-split-20260811T205044Z.txt). `--deliver new` only delivers
# messages published AFTER the consumer is created, so a freshly bootstrapped
# consumer always starts at a true zero baseline regardless of what happened on
# the stream earlier. This is safe for drop-output/jetstream-output too (filter
# tf.ingest.>, which also carries real ingest traffic): those variants only care
# about messages published during their own measurement window, never about
# historical backlog.
#
# Prints the created consumer name to stdout on success. Exits non-zero on failure.
set -euo pipefail

VARIANT="${1:?usage: bootstrap-diag-consumer.sh <variant-name> <filter-subject>}"
FILTER_SUBJECT="${2:?usage: bootstrap-diag-consumer.sh <variant-name> <filter-subject>}"

NAMESPACE="${NAMESPACE:-thingsflow-fresh}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-microk8s}"
NATS_URL="${NATS_URL:-nats://thingsflow-nats:4222}"
NATS_IMAGE="${NATS_IMAGE:-natsio/nats-box:0.16.0}"
STREAM="${STREAM:-TF_RAW}"
MAX_DELIVER="${MAX_DELIVER:-500}"
ACK_WAIT="${ACK_WAIT:-30s}"
MAX_ACK_PENDING="${MAX_ACK_PENDING:-1024}"

CONSUMER_NAME="thingsflow-latest-kv-diag-${VARIANT}"
DELIVER_GROUP="thingsflow-nats-latest-kv-diag-${VARIANT}"
DELIVER_TARGET="tf.deliver.latest-kv-diag-${VARIANT}"

kc() {
  kubectl --context="$KUBECTL_CONTEXT" -n "$NAMESPACE" "$@"
}

run_nbox() {  # run_nbox <sh -c command string>
  local podname="diag-bootstrap-$RANDOM"
  kc run "$podname" --rm -i --restart=Never --image="$NATS_IMAGE" --command -- \
    sh -c "$1"
}

echo "-- bootstrapping diagnostic consumer: $CONSUMER_NAME (stream=$STREAM filter=$FILTER_SUBJECT)" >&2

# Idempotent delete-and-recreate: check for a leftover consumer from an interrupted
# prior run before creating (mirrors nats.yaml's converge_consumer pattern).
if run_nbox "nats --server '$NATS_URL' consumer info '$STREAM' '$CONSUMER_NAME' >/dev/null 2>&1"; then
  echo "-- found leftover consumer $CONSUMER_NAME, deleting before recreate" >&2
  run_nbox "nats --server '$NATS_URL' consumer rm '$STREAM' '$CONSUMER_NAME' -f" || true
fi

if ! run_nbox "nats --server '$NATS_URL' consumer add '$STREAM' '$CONSUMER_NAME' \
      --filter '$FILTER_SUBJECT' \
      --ack explicit \
      --deliver new \
      --replay instant \
      --max-deliver '$MAX_DELIVER' \
      --wait '$ACK_WAIT' \
      --max-pending '$MAX_ACK_PENDING' \
      --target '$DELIVER_TARGET' \
      --deliver-group '$DELIVER_GROUP' \
      --defaults"; then
  echo "ERROR: failed to create diagnostic consumer $CONSUMER_NAME" >&2
  exit 1
fi

echo "$CONSUMER_NAME"
