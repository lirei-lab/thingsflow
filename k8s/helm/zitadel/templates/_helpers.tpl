{{- define "zitadel.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "zitadel.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "zitadel.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "zitadel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "zitadel.selectorLabels" -}}
app.kubernetes.io/name: {{ include "zitadel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "zitadel.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "zitadel.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "zitadel.masterkeySecretName" -}}
{{- $existing := .Values.masterkey.existingSecret | default dict -}}
{{- if $existing.name -}}
{{- $existing.name -}}
{{- else -}}
{{- printf "%s-masterkey" (include "zitadel.fullname" .) -}}
{{- end -}}
{{- end }}

{{- define "zitadel.masterkeySecretKey" -}}
{{- $existing := .Values.masterkey.existingSecret | default dict -}}
{{- $existing.key | default "masterkey" -}}
{{- end }}

{{- define "zitadel.secretConfigSecretName" -}}
{{- $existing := .Values.secretConfig.existingSecret | default dict -}}
{{- if $existing.name -}}
{{- $existing.name -}}
{{- else -}}
{{- printf "%s-secret-config" (include "zitadel.fullname" .) -}}
{{- end -}}
{{- end }}

{{- define "zitadel.secretConfigSecretKey" -}}
{{- $existing := .Values.secretConfig.existingSecret | default dict -}}
{{- $existing.key | default "config-yaml" -}}
{{- end }}

{{- define "zitadel.probeHost" -}}
{{- .Values.probes.hostHeader | default .Values.config.externalDomain -}}
{{- end }}

{{- define "zitadel.hookAnnotations" -}}
{{- if .Values.jobs.useHelmHooks }}
"helm.sh/hook": pre-install,pre-upgrade
"helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
{{- end }}
{{- end }}

{{- define "zitadel.validate" -}}
{{- $mk := .Values.masterkey | default dict -}}
{{- $mkSecret := $mk.existingSecret | default dict -}}
{{- if and (not ($mk.value | default "")) (not ($mkSecret.name | default "")) }}
{{- fail "masterkey.value or masterkey.existingSecret.name is required." }}
{{- end }}
{{- if and ($mk.value | default "") (ne (len $mk.value) 32) }}
{{- fail "masterkey.value must be exactly 32 characters. Use an existing Secret for production." }}
{{- end }}
{{- $secretExisting := .Values.secretConfig.existingSecret | default dict -}}
{{- if not ($secretExisting.name | default "") }}
{{- $pg := .Values.config.database.postgres -}}
{{- if not ($pg.user.password | default "") }}
{{- fail "config.database.postgres.user.password or secretConfig.existingSecret.name is required." }}
{{- end }}
{{- if not ($pg.admin.password | default "") }}
{{- fail "config.database.postgres.admin.password or secretConfig.existingSecret.name is required." }}
{{- end }}
{{- end }}
{{- if and (.Values.config.firstInstance.enabled | default true) (not (.Values.config.firstInstance.org.human.password | default "")) }}
{{- fail "config.firstInstance.org.human.password is required when firstInstance.enabled=true." }}
{{- end }}
{{- end }}
