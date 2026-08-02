#!/usr/bin/env bash
# Samples NATS JetStream stream/consumer depth into a CSV, for use alongside
# instrumented-bench.py. Detects growing consumer lag (backpressure) during a run.
#
# Usage: nats-depth-sampler.sh <out.csv> [interval_seconds] [monitor_url]
set -uo pipefail

OUT="${1:?usage: nats-depth-sampler.sh <out.csv> [interval] [monitor_url]}"
INTERVAL="${2:-5}"
URL="${3:-http://127.0.0.1:18222}"

echo "ts,stream,messages,bytes,consumer,num_pending,num_ack_pending,num_redelivered" >"$OUT"

while true; do
  TS=$(date +%s)
  curl -s --max-time 5 "$URL/jsz?streams=1&consumers=1" 2>/dev/null | TS="$TS" python3 -c '
import json, os, sys
ts = os.environ["TS"]
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for acc in d.get("account_details", []) or []:
    for s in acc.get("stream_detail", []) or []:
        st = s.get("state", {})
        cons = s.get("consumer_detail", []) or []
        if not cons:
            print(f'"'"'{ts},{s["name"]},{st.get("messages",0)},{st.get("bytes",0)},,,,'"'"')
        for c in cons:
            print(f'"'"'{ts},{s["name"]},{st.get("messages",0)},{st.get("bytes",0)},{c.get("name")},{c.get("num_pending",0)},{c.get("num_ack_pending",0)},{c.get("num_redelivered",0)}'"'"')
' >>"$OUT"
  sleep "$INTERVAL"
done
