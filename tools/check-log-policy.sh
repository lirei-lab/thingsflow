#!/usr/bin/env bash
#
# Static guardrail for pilot logging discipline. This does not prove the
# whole logging policy, but it catches the easy regression: adding normal
# INFO logs directly in telemetry-rate paths.
#
# Usage:
#   bash tools/check-log-policy.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

hot_files=(
  flow-core/main.go
  flow-core/postgres.go
  flow-core/internal/telemetry/writer.go
)

status=0
for file in "${hot_files[@]}"; do
  if grep -nE 'log\.Printf\("(Telemetria ignorada|Telemetry received|Device CONNECTED|Device DISCONNECTED|telemetry received|rule decision skipped)' "$file"; then
    echo "ERROR: noisy log pattern found in hot path: $file" >&2
    status=1
  fi
done

if grep -RInE '(log\.Printf|slog\.(Info|Warn|Error)).*(accessToken|refreshToken|credentialsId|jwtToken|deviceToken)' flow-core --exclude='*_test.go'; then
  echo "ERROR: possible secret token value logging found" >&2
  status=1
fi

if [ "$status" -eq 0 ]; then
  echo "log policy check passed"
fi
exit "$status"
