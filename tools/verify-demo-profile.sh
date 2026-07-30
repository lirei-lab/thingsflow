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
DEVICES_WITH_LATEST="$(psql_query "SELECT count(*) FROM (SELECT d.id FROM device d JOIN ts_kv_latest l ON l.entity_id=d.id WHERE d.name LIKE '%Demo%' GROUP BY d.id HAVING count(l.key) >= 1) s;")"
SIM_READY="$(kubectl_cmd -n "$NAMESPACE" get deploy "${RELEASE}-demo-simulator" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
SIM_READY="${SIM_READY:-0}"

expect_eq "demo dashboards" "$DASHBOARDS" "5"
expect_eq "demo devices" "$DEVICES" "18"
expect_eq "demo asset" "$ASSETS" "1"
expect_eq "demo relations" "$RELATIONS" "18"
expect_eq "demo devices with latest telemetry" "$DEVICES_WITH_LATEST" "18"
expect_ge "demo-simulator ready replicas" "$SIM_READY" "1"

echo "OK ThingsFlow demo profile is ready for the ThingsBoard-compatible UI."
