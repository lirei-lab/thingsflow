#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Generate operator-owned Kubernetes Secrets for a ThingsFlow pilot.

The generated file is written with mode 0600 and is not applied unless
APPLY=true is set.

Environment variables:
  NAMESPACE                  Kubernetes namespace. Default: thingsflow
  OUT                        Output manifest path. Default: /tmp/thingsflow-pilot-secrets.yaml
  APPLY                      Apply the manifest with kubectl. Default: false
  KUBECONFIG                 Optional kubeconfig used when APPLY=true
  NATS_USERNAME              NATS application username. Default: thingsflow
  INCLUDE_OIDC               Generate thingsflow-oidc. Default: true
  OIDC_CLIENT_SECRET         Optional fixed OIDC client secret
  OIDC_STATE_SIGNING_KEY     Optional fixed OIDC state signing key

Backup Secret generation is opt-in. Set all of these variables to include it:
  BACKUP_ENDPOINT
  BACKUP_BUCKET
  BACKUP_REGION
  BACKUP_ACCESS_KEY
  BACKUP_SECRET_KEY

Example:
  NAMESPACE=thingsflow OUT=/tmp/thingsflow-pilot-secrets.yaml tools/generate-pilot-secrets.sh

  BACKUP_ENDPOINT=https://s3.example.com \
  BACKUP_BUCKET=thingsflow-backups \
  BACKUP_REGION=us-east-1 \
  BACKUP_ACCESS_KEY=... \
  BACKUP_SECRET_KEY=... \
  tools/generate-pilot-secrets.sh
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

b64_file() {
  base64 < "$1" | tr -d '\n'
}

b64_text() {
  printf '%s' "$1" | base64 | tr -d '\n'
}

yaml_escape() {
  printf '%s' "$1" | sed "s/'/''/g"
}

secret_string() {
  printf "'%s'" "$(yaml_escape "$1")"
}

need openssl
need base64
need sed

NAMESPACE="${NAMESPACE:-thingsflow}"
OUT="${OUT:-/tmp/thingsflow-pilot-secrets.yaml}"
APPLY="${APPLY:-false}"
NATS_USERNAME="${NATS_USERNAME:-thingsflow}"
INCLUDE_OIDC="${INCLUDE_OIDC:-true}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
umask 077

device_key="$tmpdir/device-jwt-es256.pem"
openssl ecparam -name prime256v1 -genkey -noout -out "$device_key"

platform_jwt_key="$(openssl rand -base64 48 | tr -d '\n')"
device_private_b64="$(b64_file "$device_key")"
empty_previous_jwks_b64="$(b64_text '{"keys":[]}')"
postgres_password="$(openssl rand -hex 32)"
nats_password="$(openssl rand -hex 32)"
oidc_client_secret="${OIDC_CLIENT_SECRET:-$(openssl rand -hex 32)}"
oidc_state_signing_key="${OIDC_STATE_SIGNING_KEY:-$(openssl rand -base64 48 | tr -d '\n')}"

backup_complete=false
if [[ -n "${BACKUP_ENDPOINT:-}" || -n "${BACKUP_BUCKET:-}" || -n "${BACKUP_REGION:-}" || -n "${BACKUP_ACCESS_KEY:-}" || -n "${BACKUP_SECRET_KEY:-}" ]]; then
  if [[ -z "${BACKUP_ENDPOINT:-}" || -z "${BACKUP_BUCKET:-}" || -z "${BACKUP_REGION:-}" || -z "${BACKUP_ACCESS_KEY:-}" || -z "${BACKUP_SECRET_KEY:-}" ]]; then
    echo "backup Secret requires BACKUP_ENDPOINT, BACKUP_BUCKET, BACKUP_REGION, BACKUP_ACCESS_KEY, and BACKUP_SECRET_KEY" >&2
    exit 1
  fi
  backup_complete=true
fi

if [[ "$OUT" != "-" ]]; then
  mkdir -p "$(dirname "$OUT")"
  exec > "$OUT"
fi

cat <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: thingsflow-platform-keys
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  jwt-token-signing-key: $(secret_string "$platform_jwt_key")
---
apiVersion: v1
kind: Secret
metadata:
  name: thingsflow-device-jwt
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  device-jwt-es256-private-key-pem-b64: $(secret_string "$device_private_b64")
  previous-public-jwks-b64: $(secret_string "$empty_previous_jwks_b64")
---
apiVersion: v1
kind: Secret
metadata:
  name: thingsflow-postgres
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  password: $(secret_string "$postgres_password")
---
apiVersion: v1
kind: Secret
metadata:
  name: thingsflow-nats-auth
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  username: $(secret_string "$NATS_USERNAME")
  password: $(secret_string "$nats_password")
EOF

if [[ "$INCLUDE_OIDC" == "true" ]]; then
  cat <<EOF
---
apiVersion: v1
kind: Secret
metadata:
  name: thingsflow-oidc
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  client-secret: $(secret_string "$oidc_client_secret")
  state-signing-key: $(secret_string "$oidc_state_signing_key")
EOF
fi

if [[ "$backup_complete" == "true" ]]; then
  cat <<EOF
---
apiVersion: v1
kind: Secret
metadata:
  name: thingsflow-backup-s3
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  endpoint: $(secret_string "$BACKUP_ENDPOINT")
  bucket: $(secret_string "$BACKUP_BUCKET")
  region: $(secret_string "$BACKUP_REGION")
  access-key: $(secret_string "$BACKUP_ACCESS_KEY")
  secret-key: $(secret_string "$BACKUP_SECRET_KEY")
EOF
fi

if [[ "$OUT" != "-" ]]; then
  chmod 0600 "$OUT"
  echo "Wrote $OUT with mode 0600" >&2
  if [[ "$backup_complete" != "true" ]]; then
    echo "Backup Secret was not generated. Provide BACKUP_* variables or set backup.enabled=false in the private pilot overlay." >&2
  fi
fi

if [[ "$APPLY" == "true" ]]; then
  need kubectl
  if [[ "$OUT" == "-" ]]; then
    echo "APPLY=true requires OUT to be a file path, not '-'" >&2
    exit 1
  fi
  if [[ -n "${KUBECONFIG:-}" ]]; then
    kubectl --kubeconfig "$KUBECONFIG" -n "$NAMESPACE" apply -f "$OUT"
  else
    kubectl -n "$NAMESPACE" apply -f "$OUT"
  fi
fi
