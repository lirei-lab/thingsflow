{{/*
Helm-time fail-fast validations for security-critical values. Runs at
`helm template` / `helm install` / `helm upgrade` and aborts the render
with a readable error before any manifest reaches the cluster.

The flow-core has its own runtime preflight (FLOW_ENV=production fails the
boot if JWT_TOKEN_SIGNING_KEY is missing). This
file catches the *next layer in*: defaults committed to values.yaml
that should never reach a production install.

Invoked from NOTES.txt with `{{ include "thingsflow.validateSecrets" . }}`.
*/}}
{{- define "thingsflow.validateSecrets" -}}
{{- $production := or (.Values.production | default false) (eq .Values.flowCore.env "production") }}
{{- $jwtSecret := .Values.flowCore.jwtTokenSigningKeyExistingSecret | default dict }}
{{- $pgSecret := .Values.postgres.passwordExistingSecret | default dict }}
{{- $deviceJwt := .Values.flowCore.deviceJwt | default dict }}
{{- $deviceJwtSecret := $deviceJwt.privateKeyExistingSecret | default dict }}
{{- $natsAuth := .Values.nats.auth | default dict }}
{{- $natsAuthExisting := $natsAuth.existingSecret | default dict }}
{{- $historyStore := .Values.timeseries.store | default "greptimedb" }}
{{- $oidcClientSecret := .Values.oidc.clientSecretExistingSecret | default dict }}
{{- $oidcStateSecret := .Values.oidc.stateSigningKeyExistingSecret | default dict }}
{{- $testAuthDex := eq (include "thingsflow.testAuthDexEnabled" .) "true" }}

{{- if not (has $historyStore (list "greptimedb" "questdb")) }}
{{- fail "timeseries.store must be either greptimedb or questdb." }}
{{- end }}

{{- /* CLEAN-02 / D-02: greptimedb.retention.* moved to retention.greptimedb.*.
       Fail-fast on the deprecated key (PRESENCE only, RESEARCH Pitfall 5) so an
       operator who set greptimedb.retention.ttl cannot silently lose the TTL
       override. Fires on EVERY install/upgrade/template (outside $production),
       since the rename is a hard break regardless of FLOW_ENV. */}}
{{- if hasKey (.Values.greptimedb | default dict) "retention" }}
{{- fail "greptimedb.retention.* has moved to retention.greptimedb.*. Move greptimedb.retention.ttl -> retention.greptimedb.ttl and greptimedb.retention.schedule -> retention.greptimedb.schedule in your release values, then re-run helm upgrade." }}
{{- end }}

{{- if $production }}

{{- if and (not $pgSecret.name) (eq (.Values.postgres.password | default "") "postgres") }}
{{- fail "postgres.password must not use the dev default in FLOW_ENV=production. Set postgres.passwordExistingSecret.name or provide a private password overlay." }}
{{- end }}

{{- /* C-2/C-3: JWT signing key must be set in production. */}}
{{- if and (not .Values.flowCore.jwtTokenSigningKey) (not $jwtSecret.name) }}
{{- fail "flowCore.jwtTokenSigningKey or flowCore.jwtTokenSigningKeyExistingSecret.name is required in FLOW_ENV=production." }}
{{- end }}
{{- if .Values.flowCore.jwtTokenSigningKey }}
{{- if not (regexMatch "^[A-Za-z0-9+/]+={0,2}$" .Values.flowCore.jwtTokenSigningKey) }}
{{- fail "flowCore.jwtTokenSigningKey must be standard base64. URL-safe base64 or raw random strings are rejected by flow-core." }}
{{- end }}
{{- if lt (len .Values.flowCore.jwtTokenSigningKey) 44 }}
{{- fail "flowCore.jwtTokenSigningKey is too short — use at least 32 random bytes encoded as standard base64." }}
{{- end }}
{{- end }}

{{- /* Device JWT ES256 private key must be stable in production. */}}
{{- if and ($deviceJwt.enabled | default true) (not ($deviceJwt.privateKeyPemBase64 | default "")) (not $deviceJwtSecret.name) }}
{{- fail "flowCore.deviceJwt.privateKeyPemBase64 or flowCore.deviceJwt.privateKeyExistingSecret.name is required in FLOW_ENV=production." }}
{{- end }}

{{- /* CORS sanity: ALLOWED_ORIGIN=* in production with credentials is the documented footgun. */}}
{{- if eq .Values.flowCore.allowedOrigin "*" }}
{{- fail "flowCore.allowedOrigin is '*' — required to be the UI's exact origin in FLOW_ENV=production." }}
{{- end }}

{{- if and (.Values.nats.enabled | default false) (not ($natsAuth.enabled | default false)) (not (.Values.natsDataPlane.natsUrl | default "")) }}
{{- fail "nats.auth.enabled=true is required in FLOW_ENV=production when the built-in NATS event plane is enabled. Use an external authenticated natsDataPlane.natsUrl only if you disable the built-in NATS service." }}
{{- end }}

{{- if $testAuthDex }}
{{- fail "testAuth.enabled=true is for automated demos/tests only and must stay disabled in FLOW_ENV=production." }}
{{- end }}

{{- end }}

{{- if and .Values.oidc.enabled (not $testAuthDex) }}
{{- if not .Values.oidc.clientId }}
{{- fail "oidc.clientId is required when oidc.enabled=true." }}
{{- end }}
{{- if and (not .Values.oidc.clientSecret) (not $oidcClientSecret.name) }}
{{- fail "oidc.clientSecret or oidc.clientSecretExistingSecret.name is required when oidc.enabled=true." }}
{{- end }}
{{- if and (not .Values.oidc.stateSigningKey) (not $oidcStateSecret.name) }}
{{- fail "oidc.stateSigningKey or oidc.stateSigningKeyExistingSecret.name is required when oidc.enabled=true." }}
{{- end }}
{{- if and (not .Values.oidc.issuer) (or (not .Values.oidc.authorizationUrl) (not .Values.oidc.tokenUrl) (not .Values.oidc.jwksUrl)) }}
{{- fail "oidc.issuer or all explicit OIDC endpoint URLs are required when oidc.enabled=true." }}
{{- end }}
{{- end }}

{{- if $testAuthDex }}
{{- $dex := .Values.testAuth.dex | default dict }}
{{- if not ($dex.clientId | default "") }}
{{- fail "testAuth.dex.clientId is required when testAuth.enabled=true." }}
{{- end }}
{{- if not ($dex.clientSecret | default "") }}
{{- fail "testAuth.dex.clientSecret is required when testAuth.enabled=true." }}
{{- end }}
{{- if lt (len ($dex.stateSigningKey | default "")) 32 }}
{{- fail "testAuth.dex.stateSigningKey must be at least 32 bytes because flow-core signs OIDC state with it." }}
{{- end }}
{{- if not (($dex.user | default dict).passwordBcryptHash | default "") }}
{{- fail "testAuth.dex.user.passwordBcryptHash is required when testAuth.enabled=true." }}
{{- end }}
{{- end }}

{{- if and ($natsAuth.enabled | default false) (not $natsAuthExisting.name) (not ($natsAuth.password | default "")) }}
{{- fail "nats.auth.password or nats.auth.existingSecret.name is required when nats.auth.enabled=true." }}
{{- end }}

{{- end -}}
