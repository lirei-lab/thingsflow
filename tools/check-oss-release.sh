#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_file() {
  [[ -f "$1" ]] || fail "required public release file missing: $1"
}

require_file LICENSE
require_file NOTICE
require_file README.md
require_file SECURITY.md
require_file MAINTAINERS
require_file docs/INSTALL.md
require_file docs/RELEASE.md
require_file docs/SECURITY.md
require_file k8s/helm/thingsflow/Chart.yaml
require_file k8s/helm/thingsflow/values.yaml
require_file k8s/helm/thingsflow/values-pilot.example.yaml

tracked_blocklist=(
  "cluster.yaml"
  "docker/.env"
  "docker/loadgen-data/thingsflow-broker-ca.crt"
)
for path in "${tracked_blocklist[@]}"; do
  if git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
    fail "$path is tracked but must stay local/private"
  fi
done

# NOTE: the secret-filename rule deliberately skips Helm chart templates. A
# `templates/secrets.yaml` is a template that RENDERS a Secret from values — it holds
# {{ }} placeholders, not credentials — so matching on the filename alone produced a
# false positive that failed this gate. Literal credentials are still caught by the
# private-pattern content scan below.
# (The `\.py0^tools/target/` fragment below was a typo for `\.py$|^tools/target/`,
# which silently disabled both of those rules.)
{
  # tools/verify-*.py are documented public verifiers (OPERATIONS, DEVICE_SDK,
  # RELEASE all reference them); the rule targets stray private scripts at tools/ root.
  git ls-files | grep -E '(^cluster\.yaml$|(^|/)kubeconfig($|\.)|^docker/loadgen-data/|__pycache__/|^tools/[^/]+\.py$|^tools/target/|^tools/src/|^tools/pom\.xml$)' \
    | grep -vE '^tools/verify-[^/]+\.py$' || true
  git ls-files | grep -E '(^|/)(secret|secrets|.*\.secret)\.ya?ml$' | grep -vE '(^|/)templates/' || true
} | sort -u >/tmp/thingsflow-oss-tracked-blocklist.txt
if [ -s /tmp/thingsflow-oss-tracked-blocklist.txt ]; then
  echo "Tracked private/runtime files:" >&2
  cat /tmp/thingsflow-oss-tracked-blocklist.txt >&2
  fail "tracked private/runtime files found"
fi

# The public tree must not carry deployment-specific endpoints, public IPs,
# credentials, or private registry names. Use git grep so ignored runtime dirs
# are not scanned.
private_pattern='(cloud\.lirei|lirei\.io|registry\.cloud|localhost:5000|132\.209\.38\.|tailf6b6d|NovaST[0-9]*|lirei-harbor)'
if git grep -n -I -E "$private_pattern" -- . ':!tools/check-oss-release.sh' >/tmp/thingsflow-oss-private-patterns.txt; then
  echo "Private cluster patterns in public tree:" >&2
  cat /tmp/thingsflow-oss-private-patterns.txt >&2
  fail "public tree contains private cluster configuration"
fi

if grep -RIn 'SECURITY_FINAL.md' README.md SECURITY.md docs .github >/tmp/thingsflow-oss-stale-security.txt; then
  echo "Stale security-doc references:" >&2
  cat /tmp/thingsflow-oss-stale-security.txt >&2
  fail "stale SECURITY_FINAL.md references found"
fi

if [[ -d .github/workflows ]]; then
  if grep -RInE 'mvn .*license:format|tools/src/main/python/check_yml_file.py|ThingsBoard Bot' .github/workflows >/tmp/thingsflow-oss-upstream-workflows.txt; then
    echo "Upstream ThingsBoard workflow leftovers:" >&2
    cat /tmp/thingsflow-oss-upstream-workflows.txt >&2
    fail "remove upstream ThingsBoard-only workflows"
  fi
fi


python3 - <<'CHECKPY'
from pathlib import Path
import re
values = Path("k8s/helm/thingsflow/values.yaml").read_text().splitlines()
inside_images = False
bad = []
for line in values:
    if line.startswith("images:"):
        inside_images = True
        continue
    if inside_images and line and not line.startswith(" "):
        inside_images = False
    if not inside_images:
        continue
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        continue
    if re.search(r':[Ll][Aa][Tt][Ee][Ss][Tt](?:[\"\']?\s*(?:#.*)?$|@)', stripped):
        bad.append(line)
if bad:
    print("Public chart images.* must not use moving :latest tags:", flush=True)
    for line in bad:
        print(f"  {line}")
    raise SystemExit(1)
CHECKPY

if command -v helm >/dev/null 2>&1; then
  helm template thingsflow k8s/helm/thingsflow -n thingsflow >/tmp/thingsflow-helm-template.yaml
else
  echo "WARN: helm not found; skipping helm template render" >&2
fi

echo "OK: OSS release gate passed"
