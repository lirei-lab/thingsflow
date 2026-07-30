#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose.flow-test.yml}"

echo "== ThingsFlow local verification =="

echo "-> compose config"
docker compose -f "$COMPOSE_FILE" config --quiet

echo "-> flow-core tests"
( cd flow-core && go test ./... )

echo "-> native RMQTT edge smoke"
bash tools/verify-native-rmqtt-edge-local.sh

echo "LOCAL VERIFICATION PASSED"
