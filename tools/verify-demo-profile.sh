#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${1:-thingsflow}"
RELEASE="${RELEASE:-thingsflow}"
KUBECTL="${KUBECTL:-kubectl}"

need() {
  command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }
}
need "${KUBECTL%% *}"

kubectl_cmd() {
  # KUBECTL may include arguments, for example: KUBECTL="kubectl --kubeconfig /path/to/kubeconfig".
  # Intentional word splitting lets callers provide those arguments.
  # shellcheck disable=SC2086
  $KUBECTL "$@"
}

psql_query() {
  kubectl_cmd -n "$NAMESPACE" exec "${RELEASE}-postgres-0" -- \
    psql -U postgres -d thingsboard -Atc "$1"
}

expect_eq() {
  local name="$1" actual="$2" expected="$3"
  if [ "$actual" != "$expected" ]; then
    echo "FAIL $name: got $actual, expected $expected" >&2
    exit 1
  fi
  echo "OK $name: $actual"
}

expect_ge() {
  local name="$1" actual="$2" minimum="$3"
  if [ "$actual" -lt "$minimum" ]; then
    echo "FAIL $name: got $actual, expected >= $minimum" >&2
    exit 1
  fi
  echo "OK $name: $actual"
}

DASHBOARDS="$(psql_query "SELECT count(*) FROM dashboard WHERE title IN ('Thermostats','SCADA Process Demo','Smart Building Office Demo','Firmware','Software');")"
DEVICES="$(psql_query "SELECT count(*) FROM device WHERE name LIKE '%Demo%';")"
ASSETS="$(psql_query "SELECT count(*) FROM asset WHERE name='Demo Building';")"
RELATIONS="$(psql_query "SELECT count(*) FROM relation r JOIN asset a ON a.id=r.from_id WHERE a.name='Demo Building';")"

# Latest values live in the NATS JetStream KV bucket `twin_state` — Postgres
# ts_kv_latest is the legacy ThingsBoard table this platform stopped writing
# to in v1.0 (Postgres keeps only api_usage_state counters), so counting
# there always yields 0 on a healthy install. Read the bucket through a
# throwaway nats-box pod instead. NATS auth is optional for this check: demo
# installs can run without the auth Secret, so fall back to an
# unauthenticated URL when it is absent.
nats_url() {
  local user pass
  user="$(kubectl_cmd -n "$NAMESPACE" get secret "${RELEASE}-nats-auth" -o jsonpath='{.data.username}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  pass="$(kubectl_cmd -n "$NAMESPACE" get secret "${RELEASE}-nats-auth" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [ -n "$user" ] && [ -n "$pass" ]; then
    printf 'nats://%s:%s@%s-nats:4222' "$user" "$pass" "$RELEASE"
  else
    printf 'nats://%s-nats:4222' "$RELEASE"
  fi
}

DEMO_DEVICE_IDS="$(psql_query "SELECT id FROM device WHERE name LIKE '%Demo%';")"
KV_KEYS="$(kubectl_cmd -n "$NAMESPACE" run "verify-demo-kv-$$" --rm -i --restart=Never \
  --image=natsio/nats-box:0.16.0 -- \
  nats -s "$(nats_url)" kv ls twin_state 2>/dev/null || true)"
DEVICES_WITH_LATEST=0
for id in $DEMO_DEVICE_IDS; do
  # twin_state keys look like DEVICE.<tenant>.<device>.telemetry.<key>;
  # stray kubectl chatter in KV_KEYS can never match this fixed pattern.
  if printf '%s\n' "$KV_KEYS" | grep -Fq ".${id}.telemetry."; then
    DEVICES_WITH_LATEST=$((DEVICES_WITH_LATEST + 1))
  fi
done
SIM_READY="$(kubectl_cmd -n "$NAMESPACE" get deploy "${RELEASE}-demo-simulator" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
SIM_READY="${SIM_READY:-0}"

expect_eq "demo dashboards" "$DASHBOARDS" "5"
expect_eq "demo devices" "$DEVICES" "18"
expect_eq "demo asset" "$ASSETS" "1"
expect_eq "demo relations" "$RELATIONS" "18"
expect_eq "demo devices with latest telemetry" "$DEVICES_WITH_LATEST" "18"
expect_ge "demo-simulator ready replicas" "$SIM_READY" "1"

echo "OK ThingsFlow demo profile is ready for the ThingsBoard-compatible UI."
