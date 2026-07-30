package main

// Backwards-compat shim: telemetry read path moved to internal/telemetry.
// questdbPG var aliases telemetry.PG so existing callers
// (entity_query_handler.go, websockets.go) keep compiling.

import (
	"database/sql"
	"net/http"

	"flow-core/internal/telemetry"
)

// questdbPG points at the same *sql.DB managed by internal/telemetry.
// Set by InitQuestDBReader after the pool is open.
var questdbPG *sql.DB

// InitQuestDBReader — delegate then sync the alias.
func InitQuestDBReader() {
	telemetry.InitReader()
	questdbPG = telemetry.PG
}

// HandleTelemetryKeys / HandleTelemetryValues — delegate to the
// equivalents in internal/telemetry.
func HandleTelemetryKeys(w http.ResponseWriter, r *http.Request) {
	telemetry.HandleTelemetryKeys(w, r)
}
func HandleTelemetryValues(w http.ResponseWriter, r *http.Request) {
	telemetry.HandleTelemetryValues(w, r)
}

// Helpers used by websockets.go — keep the lowercase names alive.
func isValidColumnName(name string) bool { return telemetry.IsValidColumnName(name) }
func pgValueToString(boolV *bool, strV *string, longV *int64, dblV *float64, jsonV *string) string {
	return telemetry.PgValueToString(boolV, strV, longV, dblV, jsonV)
}
