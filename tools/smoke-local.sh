#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper: run the compact native RMQTT edge smoke.
bash tools/verify-native-rmqtt-edge-local.sh
