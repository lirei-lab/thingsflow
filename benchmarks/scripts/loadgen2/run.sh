#!/usr/bin/env bash
# loadgen2 launcher: bootstraps a local venv (uv, falling back to python -m venv)
# and forwards every argument to loadgen2.py.
#
#   ./run.sh calibrate --protocol mqtt --devices 200
#   ./run.sh run --target thingsflow --protocol http --devices 100 --rate 500 \
#                --duration 60 --verify-landed --cleanup
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$HERE/.venv"
PY="$VENV/bin/python"

if [[ ! -x "$PY" ]]; then
  echo "[loadgen2] creating venv at $VENV" >&2
  if command -v uv >/dev/null 2>&1; then
    uv venv "$VENV" --python 3.10 >&2
    VIRTUAL_ENV="$VENV" uv pip install --link-mode=copy -r "$HERE/requirements.txt" >&2
  else
    python3 -m venv "$VENV" >&2
    "$PY" -m pip install -q -r "$HERE/requirements.txt" >&2
  fi
fi

if [[ "${1:-}" == "--selftest" ]]; then
  exec "$PY" "$HERE/selftest.py"
fi

exec "$PY" "$HERE/loadgen2.py" "$@"
