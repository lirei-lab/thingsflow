package telemetry

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"flow-core/internal/httputil"
)

// HandleDeleteTimeseries — DELETE /api/plugins/telemetry/{entityType}/{entityId}/timeseries/delete
//
// Off unless TELEMETRY_DELETE_ENABLED=true, and that default is deliberate.
//
// This is the only path in the product that removes stored measurements, and it
// is reachable from a button in the UI. The telemetry store has no backup, so a
// mis-scoped delete — wrong device picked from a list, or the "all keys, all
// time" shape the UI offers — is unrecoverable. An operator should have to turn
// that on knowingly, in the same way RPC has to be turned on before commands can
// reach a breaker.
//
// When disabled the answer is an explicit 501 naming the flag, not a silent
// success: a delete that reports 200 and keeps the data is the worst outcome,
// because the operator stops looking.
func HandleDeleteTimeseries(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodDelete {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !deleteEnabled() {
		httputil.WriteError(w, http.StatusNotImplemented,
			"Telemetry deletion is disabled. The timeseries store has no backup; "+
				"set TELEMETRY_DELETE_ENABLED=true to allow it.")
		return
	}

	entityType, entityID, ok := parseTelemetryTarget(r.URL.Path)
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest,
			"Expected /api/plugins/telemetry/{entityType}/{entityId}/timeseries/delete")
		return
	}
	if !strings.EqualFold(entityType, "DEVICE") {
		httputil.WriteError(w, http.StatusBadRequest,
			"Only DEVICE telemetry can be deleted")
		return
	}

	q := r.URL.Query()
	keys := splitKeys(q.Get("keys"))
	if len(keys) == 0 {
		// Refusing the unscoped form is the point: TB's "delete all data for this
		// entity" is exactly the shape that cannot be undone here.
		httputil.WriteError(w, http.StatusBadRequest,
			"keys is required: deleting every key for an entity is not permitted")
		return
	}
	startTs, err1 := strconv.ParseInt(q.Get("startTs"), 10, 64)
	endTs, err2 := strconv.ParseInt(q.Get("endTs"), 10, 64)
	if err1 != nil || err2 != nil || endTs <= startTs {
		httputil.WriteError(w, http.StatusBadRequest,
			"startTs and endTs are required and endTs must be greater than startTs")
		return
	}

	tenantID, _ := claims["tenantId"].(string)
	if tenantID == "" {
		// No tenant in the token ⇒ we cannot scope the delete to the caller's
		// data. Refuse rather than run a tenant-blind DELETE that could remove
		// another tenant's measurements.
		httputil.WriteError(w, http.StatusForbidden, "Tenant scope required to delete telemetry")
		return
	}
	if PG == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "Telemetry store is unavailable")
		return
	}

	tsColumn := telemetryKVTimestampColumn()
	deleted := int64(0)
	for _, key := range keys {
		// tenant_id predicate is mandatory: without it an authenticated tenant A
		// could delete tenant B's device telemetry by supplying B's device UUID.
		res, err := PG.Exec(
			"DELETE FROM device_telemetry_kv WHERE device_id = $1 AND telemetry_key = $2 "+
				"AND "+tsColumn+" >= $3 AND "+tsColumn+" <= $4 AND tenant_id = $5",
			entityID, key, startTs, endTs, tenantID)
		if err != nil {
			slog.Error("telemetry_delete_failed",
				slog.String("device_id", entityID), slog.String("key", key),
				slog.String("error", err.Error()))
			httputil.WriteError(w, http.StatusInternalServerError, "Delete failed for key "+key)
			return
		}
		if n, err := res.RowsAffected(); err == nil {
			deleted += n
		}
	}

	// Deliberate deletion of measurements is an audit-grade event; log it with
	// enough detail to reconstruct exactly what was removed and by whom.
	user, _ := claims["sub"].(string)
	slog.Warn("telemetry_deleted",
		slog.String("tenant_id", tenantID),
		slog.String("device_id", entityID),
		slog.String("user", user),
		slog.String("keys", strings.Join(keys, ",")),
		slog.Int64("start_ts", startTs),
		slog.Int64("end_ts", endTs),
		slog.Int64("rows", deleted))

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": deleted,
		"keys":    keys,
	})
}

func deleteEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("TELEMETRY_DELETE_ENABLED")), "true")
}

// parseTelemetryTarget pulls (entityType, entityId) out of a telemetry path.
func parseTelemetryTarget(path string) (string, string, bool) {
	rest := strings.TrimPrefix(path, "/api/plugins/telemetry/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func splitKeys(raw string) []string {
	out := []string{}
	for _, k := range strings.Split(raw, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return out
}
