package telemetry

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
	"flow-core/internal/twinstore"
)

var PG *sql.DB

// tsdbEnv reads a TSDB connection setting, preferring the backend-neutral
// TSDB_PG_* name and falling back to the legacy QUESTDB_PG_* one.
//
// The QUESTDB_* names predate GreptimeDB becoming the default store, and they
// are now actively misleading: a GreptimeDB-only deployment still carries
// QUESTDB_PG_HOST=<greptime host>, which reads as "QuestDB is in use" when it is
// not deployed at all. The fallback is kept so an existing deployment keeps
// working across the rename — flipping both the image and the ConfigMap
// atomically is not something we can guarantee.
func tsdbEnv(suffix, fallback string) string {
	if v := strings.TrimSpace(os.Getenv("TSDB_PG_" + suffix)); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("QUESTDB_PG_" + suffix)); v != "" {
		log.Printf("WARN: QUESTDB_PG_%s is deprecated, use TSDB_PG_%s (the name is backend-neutral; QuestDB need not be involved)", suffix, suffix)
		return v
	}
	return fallback
}

// InitReader opens a persistent connection pool to the historical telemetry
// store's PostgreSQL-compatible protocol. GreptimeDB is the default history
// store; QuestDB remains selectable via TELEMETRY_HISTORY_STORE=questdb.
func InitReader() {
	store := historyStore()
	dsn := strings.TrimSpace(os.Getenv("TSDB_PG_DSN"))
	defaultHost := "questdb"
	defaultPort := "8812"
	defaultUser := "admin"
	defaultPass := "quest"
	defaultDB := "qdb"
	if store == "greptimedb" {
		defaultHost = "greptimedb"
		defaultPort = "4003"
		defaultUser = ""
		defaultPass = ""
		defaultDB = "public"
	}
	host := tsdbEnv("HOST", defaultHost)
	port := tsdbEnv("PORT", defaultPort)
	if dsn == "" {
		user := tsdbEnv("USER", defaultUser)
		pass := tsdbEnv("PASS", defaultPass)
		dbname := tsdbEnv("DB", defaultDB)
		if user == "" && pass == "" {
			dsn = fmt.Sprintf("postgres://%s:%s/%s?sslmode=disable", host, port, dbname)
		} else {
			dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, dbname)
		}
	}

	var err error
	PG, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("WARN: Failed to open %s PG connection: %v", store, err)
		return
	}

	PG.SetMaxOpenConns(5)
	PG.SetMaxIdleConns(2)
	PG.SetConnMaxLifetime(5 * time.Minute)

	if err = PG.Ping(); err != nil {
		log.Printf("WARN: Failed to ping %s PG at %s:%s: %v (queries will fail)", store, host, port, err)
		return
	}

	log.Printf("%s SQL reader connected at %s:%s", store, host, port)
	if store == "questdb" {
		ensureTTL()
	}
}

// HandleTelemetryKeys responds with the list of timeseries key names for the
// requested entity. For DEVICE entities we prefer the native telemetry history
// table; for API_USAGE_STATE / ASSET / etc. we fall back to ts_kv in Postgres.
// GET /api/plugins/telemetry/{entityType}/{entityId}/keys/timeseries
func HandleTelemetryKeys(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	var entityType, entityId string
	for i, p := range parts {
		if p == "telemetry" && i+2 < len(parts) {
			entityType = parts[i+1]
			entityId = parts[i+2]
			break
		}
	}

	// Deny-by-default: every telemetry read must be scoped to the caller's
	// tenant. An empty tenant claim (missing/invalid token) must NOT fall
	// through to a query with no tenant predicate — that is the IDOR we are
	// closing. 401 rather than run an unscoped scan.
	tenantID := tenantIDFromRequest(r)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	keys := []string{}
	seen := map[string]bool{}
	addKeys := func(values []string) {
		for _, key := range values {
			key = strings.TrimSpace(key)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			keys = append(keys, key)
		}
	}

	var twinErr error
	if store := twinstore.Global(); store != nil && tenantID != "" && entityId != "" {
		kvKeys, err := store.GetTelemetryKeys(r.Context(), tenantID, entityType, entityId)
		if err == nil {
			addKeys(kvKeys)
		} else if !errors.Is(err, twinstore.ErrNotFound) {
			twinErr = err
		}
	}

	useHistory := strings.EqualFold(entityType, "DEVICE") && PG != nil
	if useHistory {
		if kvKeys, ok := QueryQuestDBKVKeys(tenantID, entityId); ok {
			addKeys(kvKeys)
		} else if historyStore() == "questdb" {
			rows, err := PG.Query(`SELECT "column" FROM table_columns('device_telemetry')
				WHERE type NOT IN ('SYMBOL', 'TIMESTAMP')`)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var col string
					if err := rows.Scan(&col); err == nil {
						addKeys([]string{col})
					}
				}
			} else {
				log.Printf("WARN: telemetry history keys query failed: %v", err)
			}
		}
	}

	if len(keys) == 0 && natsTwinStateAuthoritative(entityType) && twinErr != nil {
		http.Error(w, "NATS twin state unavailable", http.StatusServiceUnavailable)
		return
	}

	// Merge in the non-device keys for this entity (covers api_usage_state, assets,
	// and any device key written via the bridge fallback). When the read backend is
	// flipped to greptime, list DISTINCT telemetry_key from entity_telemetry_kv
	// (tenant not threaded here — best-effort via entity_id); else keep the unchanged
	// Postgres ts_kv_latest/key_dictionary join.
	if usageReadBackend() == "greptime" && PG != nil && entityId != "" {
		rows, err := PG.Query(entityKVDistinctKeys(entityType, entityId, ""))
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var k string
				if err := rows.Scan(&k); err == nil {
					addKeys([]string{k})
				}
			}
		}
	} else if dbpkg.Pool != nil && entityId != "" {
		rows, err := dbpkg.Pool.Query(
			`SELECT DISTINCT k.key FROM ts_kv_latest t
			 JOIN key_dictionary k ON k.key_id = t.key
			 WHERE t.entity_id = $1`, entityId)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var k string
				if err := rows.Scan(&k); err == nil {
					addKeys([]string{k})
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(keys)
}

// HandleTelemetryValues responds with historical timeseries data.
// GET /api/plugins/telemetry/{entityType}/{entityId}/values/timeseries?key=...&startTs=...&endTs=...&limit=...&agg=...&interval=...&orderBy=...
//
// Response format matches ThingsBoard's expected JSON:
//
//	{
//	  "temperature": [{"ts": 1234567890, "value": "23.5"}, ...],
//	  "humidity":    [{"ts": 1234567890, "value": "55.2"}, ...]
//	}
func HandleTelemetryValues(w http.ResponseWriter, r *http.Request) {
	// Parse entity type+id from path: /api/plugins/telemetry/{entityType}/{entityId}/values/timeseries
	parts := strings.Split(r.URL.Path, "/")
	var entityType, entityId string
	for i, p := range parts {
		if p == "telemetry" && i+2 < len(parts) {
			entityType = parts[i+1]
			entityId = parts[i+2]
			break
		}
	}
	if entityId == "" {
		http.Error(w, "Missing entityId", http.StatusBadRequest)
		return
	}

	// Deny-by-default tenant scoping (see HandleTelemetryKeys). Refuse an
	// empty tenant claim with 401 before any store read so no device read
	// runs without an `AND tenant_id = <session tenant>` predicate.
	tenantID := tenantIDFromRequest(r)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	// Parse query parameters
	keys := TimeseriesKeysFromQuery(r.URL.Query())
	startTsStr := r.URL.Query().Get("startTs")
	endTsStr := r.URL.Query().Get("endTs")
	limitStr := r.URL.Query().Get("limit")
	agg := r.URL.Query().Get("agg")
	intervalStr := r.URL.Query().Get("interval")
	orderBy := r.URL.Query().Get("orderBy")

	// Defaults
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > 10000 {
		limit = 10000
	}

	// If no keys specified, return latest values for all keys
	if len(keys) == 0 {
		strict := r.URL.Query().Get("useStrictDataTypes") == "true"
		if serveLatestFromTwinState(w, r, entityType, entityId, nil, strict) {
			return
		}
		handleLatestValues(w, entityId, strict)
		return
	}
	if startTsStr == "" && endTsStr == "" && agg == "" {
		strict := r.URL.Query().Get("useStrictDataTypes") == "true"
		if serveLatestFromTwinState(w, r, entityType, entityId, keys, strict) {
			return
		}
	}

	// Historical reads still use the existing durable stores. NATS KV is the
	// authoritative hot/latest state, not a history table.
	if !strings.EqualFold(entityType, "DEVICE") || PG == nil {
		// Non-device entities (api_usage_state, asset, …): serve from GreptimeDB
		// entity_telemetry_kv when the read backend is flipped, else the unchanged
		// Postgres ts_kv path. Default postgres ⇒ zero behavior change.
		if usageReadBackend() == "greptime" && PG != nil {
			handleTelemetryValuesFromGreptime(w, r, entityType, entityId)
			return
		}
		handleTelemetryValuesFromPostgres(w, r, entityId)
		return
	}

	// Time range (epoch millis)
	var startTs, endTs int64
	if startTsStr != "" {
		startTs, _ = strconv.ParseInt(startTsStr, 10, 64)
	}
	if endTsStr != "" {
		endTs, _ = strconv.ParseInt(endTsStr, 10, 64)
	}
	if endTs == 0 {
		endTs = time.Now().UnixMilli()
	}
	if startTs == 0 {
		startTs = endTs - 3600000
	}

	strict := r.URL.Query().Get("useStrictDataTypes") == "true"
	if kvResult, ok := QueryQuestDBKVTimeseries(tenantID, entityId, keys, startTs, endTs, limit, orderBy, agg, intervalStr, strict); ok {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(kvResult)
		return
	}
	if historyStore() != "questdb" {
		handleTelemetryValuesFromPostgres(w, r, entityId)
		return
	}

	// Convert from epoch millis to history-store timestamp strings.
	startTime := time.UnixMilli(startTs).UTC().Format("2006-01-02T15:04:05.000000Z")
	endTime := time.UnixMilli(endTs).UTC().Format("2006-01-02T15:04:05.000000Z")

	result := make(map[string][]map[string]interface{})

	for _, key := range keys {
		// Validate key name to prevent SQL injection (only alphanumeric and underscore)
		if !IsValidColumnName(key) {
			continue
		}

		var query string
		var orderClause string
		if orderBy == "ASC" {
			orderClause = "ASC"
		} else {
			orderClause = "DESC"
		}

		if agg != "" && agg != "NONE" && intervalStr != "" {
			// Aggregated query using QuestDB's SAMPLE BY.
			intervalMs, _ := strconv.ParseInt(intervalStr, 10, 64)
			sampleBy := msToSampleBy(intervalMs)

			aggFunc := mapAggFunction(agg)
			query = fmt.Sprintf(
				`SELECT timestamp, %s(%s) as val FROM device_telemetry 
				 WHERE device_id = '%s' AND timestamp >= '%s' AND timestamp <= '%s' AND %s IS NOT NULL
				 SAMPLE BY %s
				 ORDER BY timestamp %s
				 LIMIT %d`,
				aggFunc, key, entityId, startTime, endTime, key, sampleBy, orderClause, limit)
		} else {
			// Raw values query
			query = fmt.Sprintf(
				`SELECT timestamp, %s FROM device_telemetry 
				 WHERE device_id = '%s' AND timestamp >= '%s' AND timestamp <= '%s' AND %s IS NOT NULL
				 ORDER BY timestamp %s
				 LIMIT %d`,
				key, entityId, startTime, endTime, key, orderClause, limit)
		}

		rows, err := PG.Query(query)
		if err != nil {
			log.Printf("WARN: QuestDB wide-table values query failed for key '%s': %v", key, err)
			result[key] = []map[string]interface{}{}
			continue
		}

		dataPoints := []map[string]interface{}{}
		for rows.Next() {
			var ts time.Time
			var val interface{}
			if err := rows.Scan(&ts, &val); err != nil {
				continue
			}

			// Convert value to string (TB UI expects string values)
			valStr := fmt.Sprintf("%v", val)

			dataPoints = append(dataPoints, map[string]interface{}{
				"ts":    ts.UnixMilli(),
				"value": valStr,
			})
		}
		rows.Close()

		result[key] = dataPoints
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func serveLatestFromTwinState(w http.ResponseWriter, r *http.Request, entityType, entityID string, keys []string, strict bool) bool {
	store := twinstore.Global()
	authoritative := natsTwinStateAuthoritative(entityType)
	if store == nil {
		if authoritative {
			http.Error(w, "NATS twin state unavailable", http.StatusServiceUnavailable)
			return true
		}
		return false
	}
	tenantID := tenantIDFromRequest(r)
	if tenantID == "" {
		if authoritative {
			http.Error(w, "tenantId required for NATS twin state", http.StatusForbidden)
			return true
		}
		return false
	}
	latest, err := store.GetLatestTelemetry(r.Context(), tenantID, entityType, entityID, keys)
	if err != nil {
		if authoritative {
			if errors.Is(err, twinstore.ErrNotFound) {
				latest = map[string]twinstore.Value{}
			} else {
				http.Error(w, "NATS twin state unavailable", http.StatusServiceUnavailable)
				return true
			}
		} else {
			return false
		}
	}
	// Twin-state KV yielded no value (e.g. the latest-KV pipeline is not
	// populating the bucket). Fall back to the history KV last-value so device
	// "current value" widgets aren't blank. KV stays the fast path when populated.
	if len(latest) == 0 && strings.EqualFold(entityType, "DEVICE") && PG != nil {
		if fb, ok := deviceLatestFallback(tenantID, entityID, keys, strict); ok {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(fb)
			return true
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(twinstore.LatestAsTimeseries(latest, strict))
	return true
}

func natsTwinStateAuthoritative(entityType string) bool {
	return strings.EqualFold(entityType, "DEVICE") && strings.EqualFold(strings.TrimSpace(getEnv("TWIN_STATE_STORE", "")), "nats")
}

func tenantIDFromRequest(r *http.Request) string {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		return ""
	}
	tenantID, _ := claims["tenantId"].(string)
	return tenantID
}

// TimeseriesKeysFromQuery accepts both ThingsBoard styles:
// repeated singular `key=a&key=b` and comma-joined `keys=a,b`.
func TimeseriesKeysFromQuery(values url.Values) []string {
	seen := map[string]bool{}
	keys := []string{}
	add := func(raw string) {
		for _, part := range strings.Split(raw, ",") {
			key := strings.TrimSpace(part)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			keys = append(keys, key)
		}
	}
	for _, key := range values["key"] {
		add(key)
	}
	for _, key := range values["keys"] {
		add(key)
	}
	return keys
}

// QuestDBKVValueToTyped converts values emitted by the NATS history KV
// materializer. In non-strict mode it preserves ThingsBoard's legacy string
// response shape.
func QuestDBKVValueToTyped(kind, raw string, strict bool) interface{} {
	if !strict {
		return raw
	}
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "number":
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			return f
		}
	case "boolean", "bool":
		if b, err := strconv.ParseBool(raw); err == nil {
			return b
		}
	case "json", "object", "array":
		var parsed interface{}
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			return parsed
		}
	}
	return raw
}

// QueryQuestDBKVKeys returns telemetry keys from the compact history KV table
// written by the NATS data plane. ok=false means the table is not available and
// callers should fall back to the legacy wide table.
func QueryQuestDBKVKeys(tenantID, entityId string) ([]string, bool) {
	if PG == nil || entityId == "" {
		return nil, false
	}
	rows, err := PG.Query(fmt.Sprintf(
		`SELECT DISTINCT telemetry_key FROM device_telemetry_kv WHERE device_id = %s%s`,
		sqlLiteral(entityId), deviceTenantPredicate(tenantID),
	))
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	keys := []string{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err == nil && key != "" {
			keys = append(keys, key)
		}
	}
	return keys, true
}

// QueryQuestDBKVTimeseries reads historical points from the compact
// device_telemetry_kv table used by the NATS materializer. ok=false means the
// table/query is unavailable and the caller may fall back to the wide table.
func QueryQuestDBKVTimeseries(tenantID, entityId string, keys []string, startTs, endTs int64, limit int, orderBy, agg, intervalStr string, strict bool) (map[string][]map[string]interface{}, bool) {
	result := map[string][]map[string]interface{}{}
	if PG == nil || entityId == "" {
		return result, false
	}
	if len(keys) == 0 {
		var ok bool
		keys, ok = QueryQuestDBKVKeys(tenantID, entityId)
		if !ok {
			return result, false
		}
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}
	if strings.ToUpper(orderBy) != "ASC" {
		orderBy = "DESC"
	} else {
		orderBy = "ASC"
	}
	startTime := time.UnixMilli(startTs).UTC().Format("2006-01-02T15:04:05.000000Z")
	endTime := time.UnixMilli(endTs).UTC().Format("2006-01-02T15:04:05.000000Z")

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}

		query := questDBKVRawQuery(tenantID, entityId, key, startTime, endTime, orderBy, limit)
		if agg != "" && !strings.EqualFold(agg, "NONE") && intervalStr != "" {
			if intervalMs, err := strconv.ParseInt(intervalStr, 10, 64); err == nil && intervalMs > 0 {
				// Server-side interval aggregation. QuestDB uses SAMPLE BY; GreptimeDB
				// (the default store) has no SAMPLE BY, so bucket via date_bin(). Without
				// this, GreptimeDB dashboards silently got raw points (capped at limit) and
				// no interval rollup — breaking energy aggregates (hourly/daily kWh, demand).
				if historyStore() == "questdb" {
					query = questDBKVAggQuery(tenantID, entityId, key, startTime, endTime, orderBy, limit, mapAggFunction(agg), msToSampleBy(intervalMs))
				} else {
					query = greptimeKVAggQuery(tenantID, entityId, key, startTime, endTime, orderBy, limit, mapAggFunction(agg), intervalMs)
				}
			}
		}

		rows, err := PG.Query(query)
		if err != nil {
			log.Printf("WARN: telemetry history KV values query failed for key '%s': %v", key, err)
			return result, false
		}
		points := []map[string]interface{}{}
		for rows.Next() {
			var ts time.Time
			var raw string
			var kind string
			if err := rows.Scan(&ts, &raw, &kind); err != nil {
				continue
			}
			points = append(points, map[string]interface{}{
				"ts":    ts.UnixMilli(),
				"value": QuestDBKVValueToTyped(kind, raw, strict),
			})
		}
		rows.Close()
		result[key] = points
	}
	return result, true
}

// deviceTenantPredicate renders the mandatory `AND tenant_id = <lit>` isolation
// clause for the device_telemetry_kv reads. device_telemetry_kv carries a
// tenant_id column (written by the Bento pipeline as a line-protocol tag), so
// EVERY device read must be scoped to the requesting session's tenant — a bare
// device_id predicate is an IDOR (tenant A reads tenant B's device by UUID).
// The predicate is rendered unconditionally: an empty tenantID yields
// `tenant_id = ”`, which matches no rows (fail-closed) rather than dumping
// every tenant's telemetry. HTTP handlers reject an empty tenant with 401
// before reaching here; this is the defence-in-depth second layer.
func deviceTenantPredicate(tenantID string) string {
	return " AND tenant_id = " + sqlLiteral(tenantID)
}

func questDBKVRawQuery(tenantID, entityId, key, startTime, endTime, orderBy string, limit int) string {
	tsColumn := telemetryKVTimestampColumn()
	return fmt.Sprintf(
		`SELECT %s AS timestamp, value_string, value_kind FROM device_telemetry_kv
		 WHERE device_id = %s AND telemetry_key = %s%s
		   AND %s >= %s AND %s <= %s
		 ORDER BY %s %s
		 LIMIT %d`,
		tsColumn, sqlLiteral(entityId), sqlLiteral(key), deviceTenantPredicate(tenantID),
		tsColumn, sqlLiteral(startTime), tsColumn, sqlLiteral(endTime),
		tsColumn, orderBy, limit)
}

func questDBKVAggQuery(tenantID, entityId, key, startTime, endTime, orderBy string, limit int, aggFunc, sampleBy string) string {
	return fmt.Sprintf(
		`SELECT timestamp, cast(%s(cast(value_string AS DOUBLE)) AS VARCHAR) AS value_string, 'number' AS value_kind
		   FROM device_telemetry_kv
		  WHERE device_id = %s AND telemetry_key = %s AND value_kind = 'number'%s
		    AND timestamp >= %s AND timestamp <= %s
		  SAMPLE BY %s
		  ORDER BY timestamp %s
		  LIMIT %d`,
		aggFunc, sqlLiteral(entityId), sqlLiteral(key), deviceTenantPredicate(tenantID), sqlLiteral(startTime), sqlLiteral(endTime), sampleBy, orderBy, limit)
}

// greptimeKVAggQuery builds a server-side aggregated KV timeseries query for
// GreptimeDB. GreptimeDB has no QuestDB SAMPLE BY, so numeric samples are bucketed
// into interval-wide windows with date_bin() and the aggregate is applied per bucket.
// intervalMs is the TB `interval` query param (bucket width in epoch millis). The
// projection matches questDBKVRawQuery's shape — (bucket_ts, value_string, value_kind)
// — so the caller scans aggregated and raw rows identically. Only value_kind='number'
// rows are aggregatable; the cast-to-DOUBLE mirrors the QuestDB agg path.
func greptimeKVAggQuery(tenantID, entityId, key, startTime, endTime, orderBy string, limit int, aggFunc string, intervalMs int64) string {
	tsColumn := telemetryKVTimestampColumn()
	bin := fmt.Sprintf("date_bin('%d milliseconds'::INTERVAL, %s)", intervalMs, tsColumn)
	return fmt.Sprintf(
		`SELECT %s AS timestamp, CAST(%s(CAST(value_string AS DOUBLE)) AS STRING) AS value_string, 'number' AS value_kind
		   FROM device_telemetry_kv
		  WHERE device_id = %s AND telemetry_key = %s AND value_kind = 'number'%s
		    AND %s >= %s AND %s <= %s
		  GROUP BY %s
		  ORDER BY timestamp %s
		  LIMIT %d`,
		bin, aggFunc, sqlLiteral(entityId), sqlLiteral(key), deviceTenantPredicate(tenantID),
		tsColumn, sqlLiteral(startTime), tsColumn, sqlLiteral(endTime),
		bin, orderBy, limit)
}

// DeviceKVLatest returns the latest (ts_millis, typed value) for one telemetry
// key of a device from the history KV table (device_telemetry_kv). It is the
// resilience fallback used when the NATS twin-state KV yields no value — e.g.
// the latest-KV pipeline is not populating the bucket — so dashboard "current
// value" widgets still resolve. Reads the configured history store (GreptimeDB
// default) over its pg-wire connection; key is escaped as a literal (not a
// column), so arbitrary telemetry key names are safe.
func DeviceKVLatest(tenantID, entityId, key string, strict bool) (int64, interface{}, bool) {
	if PG == nil || entityId == "" || strings.TrimSpace(key) == "" {
		return 0, nil, false
	}
	tsCol := telemetryKVTimestampColumn()
	q := fmt.Sprintf(
		`SELECT %s, value_string, value_kind FROM device_telemetry_kv
		  WHERE device_id = %s AND telemetry_key = %s%s
		  ORDER BY %s DESC
		  LIMIT 1`,
		tsCol, sqlLiteral(entityId), sqlLiteral(key), deviceTenantPredicate(tenantID), tsCol)
	var ts time.Time
	var raw, kind string
	if err := PG.QueryRow(q).Scan(&ts, &raw, &kind); err != nil {
		return 0, nil, false
	}
	return ts.UnixMilli(), QuestDBKVValueToTyped(kind, raw, strict), true
}

// deviceLatestFallback builds the TB latest-timeseries response shape
// (key -> [{ts,value}]) from the history KV last-value, for one or more keys.
// When keys is empty it first discovers the device's keys. Returns found=false
// if nothing resolves, so the caller can keep the (empty) twin-state response.
func deviceLatestFallback(tenantID, entityId string, keys []string, strict bool) (map[string][]map[string]interface{}, bool) {
	if PG == nil || entityId == "" {
		return nil, false
	}
	if len(keys) == 0 {
		discovered, ok := QueryQuestDBKVKeys(tenantID, entityId)
		if !ok {
			return nil, false
		}
		keys = discovered
	}
	out := map[string][]map[string]interface{}{}
	found := false
	for _, key := range keys {
		if ts, value, ok := DeviceKVLatest(tenantID, entityId, key, strict); ok {
			out[key] = []map[string]interface{}{{"ts": ts, "value": value}}
			found = true
		}
	}
	return out, found
}

// usageReadBackend reports which store serves the non-device (entity) telemetry
// read path. Default "postgres" — the existing ts_kv/ts_kv_latest path — so the
// GreptimeDB cutover is a safe blue/green config flip (set USAGE_READ_BACKEND=greptime
// only AFTER entity_telemetry_kv is confirmed accruing rows). Single source of truth
// for the flip; callers gate on usageReadBackend() == "greptime".
func usageReadBackend() string {
	return strings.ToLower(strings.TrimSpace(getEnv("USAGE_READ_BACKEND", "postgres")))
}

// UsageReadBackend is the exported accessor for the non-device read-backend flip, used by
// sibling packages (entityquery, ws) to gate their latest-value branches identically.
func UsageReadBackend() string {
	return usageReadBackend()
}

// entityKVRawQuery builds a raw history read against entity_telemetry_kv keyed on
// entity_id + entity_type (the generalized sibling of questDBKVRawQuery, which keys on
// device_id). entity_type/entity_id/key/times come from the request path, so EVERY value
// is wrapped with sqlLiteral (single-quote doubling) — the same injection guard the device
// reader uses. IsValidColumnName is deliberately NOT used on these: they are SQL literals,
// not column names. When tenantId != "" the caller has resolved the session tenant and an
// `AND tenant_id = <lit>` predicate is rendered (mandatory second-layer isolation); callers
// that do not thread the tenant pass "" and rely on entity_id (the requesting tenant's own
// api_usage_state.id) as the scoping layer.
func entityKVRawQuery(entityType, entityId, tenantId, key, startTime, endTime, orderBy string, limit int) string {
	tsColumn := telemetryKVTimestampColumn()
	tenantPredicate := ""
	if tenantId != "" {
		tenantPredicate = " AND tenant_id = " + sqlLiteral(tenantId)
	}
	return fmt.Sprintf(
		`SELECT %s AS timestamp, value_string, value_kind FROM entity_telemetry_kv
		 WHERE entity_id = %s AND entity_type = %s AND telemetry_key = %s
		   AND %s >= %s AND %s <= %s%s
		 ORDER BY %s %s
		 LIMIT %d`,
		tsColumn, sqlLiteral(entityId), sqlLiteral(entityType), sqlLiteral(key),
		tsColumn, sqlLiteral(startTime), tsColumn, sqlLiteral(endTime), tenantPredicate,
		tsColumn, orderBy, limit)
}

// entityKVAggQuery is the entity_telemetry_kv analogue of greptimeKVAggQuery: server-side
// interval aggregation via date_bin() bucketing for non-device (api_usage_state) telemetry,
// so entity-scoped dashboard widgets get proper interval rollups instead of raw points.
// intervalMs is the TB `interval` param; only value_kind='number' rows are aggregatable.
func entityKVAggQuery(entityType, entityId, tenantId, key, startTime, endTime, orderBy string, limit int, aggFunc string, intervalMs int64) string {
	tsColumn := telemetryKVTimestampColumn()
	tenantPredicate := ""
	if tenantId != "" {
		tenantPredicate = " AND tenant_id = " + sqlLiteral(tenantId)
	}
	bin := fmt.Sprintf("date_bin('%d milliseconds'::INTERVAL, %s)", intervalMs, tsColumn)
	return fmt.Sprintf(
		`SELECT %s AS timestamp, CAST(%s(CAST(value_string AS DOUBLE)) AS STRING) AS value_string, 'number' AS value_kind
		   FROM entity_telemetry_kv
		  WHERE entity_id = %s AND entity_type = %s AND telemetry_key = %s AND value_kind = 'number'
		    AND %s >= %s AND %s <= %s%s
		  GROUP BY %s
		  ORDER BY timestamp %s
		  LIMIT %d`,
		bin, aggFunc, sqlLiteral(entityId), sqlLiteral(entityType), sqlLiteral(key),
		tsColumn, sqlLiteral(startTime), tsColumn, sqlLiteral(endTime), tenantPredicate,
		bin, orderBy, limit)
}

// entityKVLatestQuery builds the last-point read against entity_telemetry_kv (replaces the
// ts_kv_latest read for non-device entities). Same sqlLiteral escaping + mandatory tenant_id
// predicate rule as entityKVRawQuery when tenantId != "".
func entityKVLatestQuery(entityType, entityId, tenantId, key string) string {
	tsColumn := telemetryKVTimestampColumn()
	tenantPredicate := ""
	if tenantId != "" {
		tenantPredicate = " AND tenant_id = " + sqlLiteral(tenantId)
	}
	return fmt.Sprintf(
		`SELECT %s AS timestamp, value_string, value_kind FROM entity_telemetry_kv
		 WHERE entity_id = %s AND entity_type = %s AND telemetry_key = %s%s
		 ORDER BY %s DESC LIMIT 1`,
		tsColumn, sqlLiteral(entityId), sqlLiteral(entityType), sqlLiteral(key), tenantPredicate, tsColumn)
}

// entityKVDistinctKeys lists the telemetry keys present for an entity in
// entity_telemetry_kv (replaces the ts_kv_latest/key_dictionary join). Same tenant_id rule.
func entityKVDistinctKeys(entityType, entityId, tenantId string) string {
	tenantPredicate := ""
	if tenantId != "" {
		tenantPredicate = " AND tenant_id = " + sqlLiteral(tenantId)
	}
	return fmt.Sprintf(
		`SELECT DISTINCT telemetry_key FROM entity_telemetry_kv WHERE entity_id = %s AND entity_type = %s%s`,
		sqlLiteral(entityId), sqlLiteral(entityType), tenantPredicate)
}

func sqlLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func historyStore() string {
	store := strings.ToLower(strings.TrimSpace(getEnv("TELEMETRY_HISTORY_STORE", "greptimedb")))
	if store == "" {
		return "greptimedb"
	}
	return store
}

func telemetryKVTimestampColumn() string {
	column := strings.TrimSpace(os.Getenv("TELEMETRY_KV_TS_COLUMN"))
	if column == "" {
		if historyStore() == "greptimedb" {
			column = "greptime_timestamp"
		} else {
			column = "timestamp"
		}
	}
	if !IsValidColumnName(column) {
		return "timestamp"
	}
	return column
}

// handleLatestValues returns the most recent value for each telemetry
// key. When strict=true (the UI's useStrictDataTypes=true) values keep
// their native JSON type (number/bool/string) — without that flag the
// UI rejects the response as un-parseable and reports "no telemetry".
//
// QuestDB's device_telemetry is a wide single table covering every
// metric the fleet has ever published. Reading "latest row by
// device_id" therefore returns columns the device never publishes —
// for BOOLEAN columns those values come back as false (the column's
// implicit default), which would mislead the UI into showing e.g.
// `compressor_on=false` on a temperature sensor. We suppress booleans
// that equal false and numerics that are zero, since both are the
// most common QuestDB-default value when the device never wrote that
// column. False-positives (a device that legitimately sent 0/false
// looking absent) trade against showing wrong cross-profile defaults
// — the latter is a UX bug the user can spot, the former isn't.
func handleLatestValues(w http.ResponseWriter, entityId string, strict bool) {
	// Get all telemetry column names + their types so we can suppress
	// QuestDB-default values per column kind.
	colRows, err := PG.Query(`SELECT "column", type FROM table_columns('device_telemetry')
		WHERE type NOT IN ('SYMBOL', 'TIMESTAMP')`)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer colRows.Close()

	var columns []string
	colTypes := map[string]string{}
	for colRows.Next() {
		var col, ctype string
		if err := colRows.Scan(&col, &ctype); err == nil {
			columns = append(columns, col)
			colTypes[col] = ctype
		}
	}

	result := make(map[string][]map[string]interface{})

	if len(columns) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
		return
	}

	// Build a single query for the latest row
	colList := strings.Join(columns, ", ")
	query := fmt.Sprintf(
		`SELECT timestamp, %s FROM device_telemetry 
		 WHERE device_id = '%s' 
		 ORDER BY timestamp DESC 
		 LIMIT 1`,
		colList, entityId)

	rows, err := PG.Query(query)
	if err != nil {
		log.Printf("WARN: QuestDB latest values query failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
		return
	}
	defer rows.Close()

	if rows.Next() {
		// Dynamically scan all columns
		values := make([]interface{}, len(columns)+1) // +1 for timestamp
		valuePtrs := make([]interface{}, len(columns)+1)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("WARN: Failed to scan latest values row: %v", err)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(result)
			return
		}

		// Parse timestamp
		var tsMs int64
		if ts, ok := values[0].(time.Time); ok {
			tsMs = ts.UnixMilli()
		}

		// Build result per key, with type-aware filtering and emission.
		for i, col := range columns {
			val := values[i+1]
			if val == nil {
				continue
			}
			ctype := colTypes[col]
			// Suppress QuestDB-default values that are almost certainly
			// "column never written by this device" rather than a real
			// reading. See the function-level doc for the trade-off.
			switch ctype {
			case "BOOLEAN":
				if b, ok := val.(bool); ok && !b {
					continue
				}
			case "BYTE", "SHORT", "INT", "LONG":
				if n, ok := toInt64(val); ok && n == 0 {
					continue
				}
			case "FLOAT", "DOUBLE":
				if f, ok := toFloat64(val); ok && f == 0 {
					continue
				}
			}
			emitted := val
			if !strict {
				// Legacy mode: stringify everything (older UI tabs).
				emitted = fmt.Sprintf("%v", val)
			}
			result[col] = []map[string]interface{}{
				{"ts": tsMs, "value": emitted},
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleTelemetryValuesFromPostgres serves /api/plugins/telemetry/.../values/timeseries
// from the PostgreSQL ts_kv / ts_kv_latest tables. Used for entities that don't
// live in the QuestDB device_telemetry table (api_usage_state, asset, etc.).
func handleTelemetryValuesFromPostgres(w http.ResponseWriter, r *http.Request, entityId string) {
	keys := TimeseriesKeysFromQuery(r.URL.Query())
	startTsStr := r.URL.Query().Get("startTs")
	endTsStr := r.URL.Query().Get("endTs")
	limitStr := r.URL.Query().Get("limit")

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > 10000 {
		limit = 10000
	}

	var startTs, endTs int64
	if startTsStr != "" {
		startTs, _ = strconv.ParseInt(startTsStr, 10, 64)
	}
	if endTsStr != "" {
		endTs, _ = strconv.ParseInt(endTsStr, 10, 64)
	}
	if endTs == 0 {
		endTs = time.Now().UnixMilli()
	}

	result := make(map[string][]map[string]interface{})

	// If no specific keys: return latest of every key for this entity
	if len(keys) == 0 {
		rows, err := dbpkg.Pool.Query(
			`SELECT k.key, t.ts, t.bool_v, t.str_v, t.long_v, t.dbl_v, t.json_v
			 FROM ts_kv_latest t
			 JOIN key_dictionary k ON k.key_id = t.key
			 WHERE t.entity_id = $1`, entityId)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var key string
				var ts int64
				var boolV *bool
				var strV, jsonV *string
				var longV *int64
				var dblV *float64
				if err := rows.Scan(&key, &ts, &boolV, &strV, &longV, &dblV, &jsonV); err != nil {
					continue
				}
				v := PgValueToString(boolV, strV, longV, dblV, jsonV)
				result[key] = []map[string]interface{}{{"ts": ts, "value": v}}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
		return
	}

	for _, key := range keys {
		key = strings.TrimSpace(key)
		keyId := dbpkg.GetOrInsertKeyID(key)
		if keyId == -1 {
			result[key] = []map[string]interface{}{}
			continue
		}
		query := `SELECT ts, bool_v, str_v, long_v, dbl_v, json_v FROM ts_kv
		          WHERE entity_id = $1 AND key = $2`
		args := []interface{}{entityId, keyId}
		argIdx := 3
		if startTs > 0 {
			query += " AND ts >= $" + strconv.Itoa(argIdx)
			args = append(args, startTs)
			argIdx++
		}
		if endTs > 0 {
			query += " AND ts <= $" + strconv.Itoa(argIdx)
			args = append(args, endTs)
			argIdx++
		}
		query += " ORDER BY ts DESC LIMIT $" + strconv.Itoa(argIdx)
		args = append(args, limit)

		rows, err := dbpkg.Pool.Query(query, args...)
		if err != nil {
			log.Printf("WARN ts_kv values query for %s/%s: %v", entityId, key, err)
			result[key] = []map[string]interface{}{}
			continue
		}
		points := []map[string]interface{}{}
		for rows.Next() {
			var ts int64
			var boolV *bool
			var strV, jsonV *string
			var longV *int64
			var dblV *float64
			if err := rows.Scan(&ts, &boolV, &strV, &longV, &dblV, &jsonV); err != nil {
				continue
			}
			points = append(points, map[string]interface{}{
				"ts":    ts,
				"value": PgValueToString(boolV, strV, longV, dblV, jsonV),
			})
		}
		rows.Close()
		result[key] = points
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleTelemetryValuesFromGreptime serves /api/plugins/telemetry/.../values/timeseries
// from GreptimeDB entity_telemetry_kv (keyed on entity_id + entity_type) when
// USAGE_READ_BACKEND=greptime. Latest = last-point query (no ts_kv_latest). Emits the
// exact same {key: [{ts, value}]} shape as the Postgres path.
//
// tenantId is passed "" here: this HTTP-values handler does not thread the session tenant,
// so isolation is best-effort via entity_id (the requesting tenant's own api_usage_state.id,
// which already scopes the row). The mandatory tenant_id predicate is enforced on the
// resolved-tenant paths (entityquery.go fetchLatestTimeseries + ws.go non-device latest).
func handleTelemetryValuesFromGreptime(w http.ResponseWriter, r *http.Request, entityType, entityId string) {
	keys := TimeseriesKeysFromQuery(r.URL.Query())
	startTsStr := r.URL.Query().Get("startTs")
	endTsStr := r.URL.Query().Get("endTs")
	limitStr := r.URL.Query().Get("limit")
	orderBy := r.URL.Query().Get("orderBy")
	agg := r.URL.Query().Get("agg")
	intervalStr := r.URL.Query().Get("interval")
	strict := r.URL.Query().Get("useStrictDataTypes") == "true"

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > 10000 {
		limit = 10000
	}
	if strings.ToUpper(orderBy) == "ASC" {
		orderBy = "ASC"
	} else {
		orderBy = "DESC"
	}

	result := make(map[string][]map[string]interface{})

	// No specific keys: latest value of every key for this entity.
	if len(keys) == 0 {
		rows, err := PG.Query(entityKVDistinctKeys(entityType, entityId, ""))
		if err == nil {
			for rows.Next() {
				var k string
				if err := rows.Scan(&k); err == nil && k != "" {
					keys = append(keys, k)
				}
			}
			rows.Close()
		}
		for _, key := range keys {
			if ts, value, ok := entityKVLatestRow(entityType, entityId, "", key, strict); ok {
				result[key] = []map[string]interface{}{{"ts": ts, "value": value}}
			} else {
				result[key] = []map[string]interface{}{}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
		return
	}

	// Time range (epoch millis) → history-store timestamp strings.
	var startTs, endTs int64
	if startTsStr != "" {
		startTs, _ = strconv.ParseInt(startTsStr, 10, 64)
	}
	if endTsStr != "" {
		endTs, _ = strconv.ParseInt(endTsStr, 10, 64)
	}
	if endTs == 0 {
		endTs = time.Now().UnixMilli()
	}
	if startTs == 0 {
		startTs = endTs - 3600000
	}
	startTime := time.UnixMilli(startTs).UTC().Format("2006-01-02T15:04:05.000000Z")
	endTime := time.UnixMilli(endTs).UTC().Format("2006-01-02T15:04:05.000000Z")

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		query := entityKVRawQuery(entityType, entityId, "", key, startTime, endTime, orderBy, limit)
		if agg != "" && !strings.EqualFold(agg, "NONE") && intervalStr != "" {
			if intervalMs, perr := strconv.ParseInt(intervalStr, 10, 64); perr == nil && intervalMs > 0 {
				query = entityKVAggQuery(entityType, entityId, "", key, startTime, endTime, orderBy, limit, mapAggFunction(agg), intervalMs)
			}
		}
		rows, err := PG.Query(query)
		if err != nil {
			log.Printf("WARN entity_telemetry_kv values query for %s/%s: %v", entityId, key, err)
			result[key] = []map[string]interface{}{}
			continue
		}
		points := []map[string]interface{}{}
		for rows.Next() {
			var ts time.Time
			var raw, kind string
			if err := rows.Scan(&ts, &raw, &kind); err != nil {
				continue
			}
			points = append(points, map[string]interface{}{
				"ts":    ts.UnixMilli(),
				"value": QuestDBKVValueToTyped(kind, raw, strict),
			})
		}
		rows.Close()
		result[key] = points
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// entityKVLatestRow runs entityKVLatestQuery and decodes the single last point. ok=false
// when no row exists or the query fails. tenantId follows the same rule as the builders:
// non-empty renders the mandatory tenant_id predicate.
func entityKVLatestRow(entityType, entityId, tenantId, key string, strict bool) (ts int64, value interface{}, ok bool) {
	if PG == nil {
		return 0, nil, false
	}
	rows, err := PG.Query(entityKVLatestQuery(entityType, entityId, tenantId, key))
	if err != nil {
		log.Printf("WARN entity_telemetry_kv latest query for %s/%s: %v", entityId, key, err)
		return 0, nil, false
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, nil, false
	}
	var t time.Time
	var raw, kind string
	if err := rows.Scan(&t, &raw, &kind); err != nil {
		return 0, nil, false
	}
	return t.UnixMilli(), QuestDBKVValueToTyped(kind, raw, strict), true
}

// EntityKVLatest is the exported last-point lookup for non-device entity latest values,
// called from the resolved-tenant paths (entityquery.go fetchLatestTimeseries + ws.go
// non-device latest). Callers MUST pass the resolved session tenantId so the rendered
// query carries the mandatory `AND tenant_id = <session tenant>` predicate (two-layer
// isolation). Non-strict decode preserves ThingsBoard's legacy string-valued response.
func EntityKVLatest(entityType, entityId, tenantId, key string) (ts int64, value interface{}, ok bool) {
	return entityKVLatestRow(entityType, entityId, tenantId, key, false)
}

func PgValueToString(boolV *bool, strV *string, longV *int64, dblV *float64, jsonV *string) string {
	switch {
	case boolV != nil:
		return fmt.Sprintf("%v", *boolV)
	case strV != nil:
		return *strV
	case longV != nil:
		return strconv.FormatInt(*longV, 10)
	case dblV != nil:
		return strconv.FormatFloat(*dblV, 'f', -1, 64)
	case jsonV != nil:
		return *jsonV
	}
	return ""
}

// PgValueToTyped is the strict-types counterpart to PgValueToString.
// Returns the value with its native JSON type so the TB UI v3.7+
// (which expects useStrictDataTypes semantics on WS subscriptions)
// renders it as a number / boolean / object instead of dropping the
// row as un-parseable. JSON columns are unmarshalled so nested objects
// and arrays come out as proper JSON, not a quoted blob.
func PgValueToTyped(boolV *bool, strV *string, longV *int64, dblV *float64, jsonV *string) interface{} {
	switch {
	case boolV != nil:
		return *boolV
	case strV != nil:
		return *strV
	case longV != nil:
		return *longV
	case dblV != nil:
		return *dblV
	case jsonV != nil:
		var parsed interface{}
		if err := json.Unmarshal([]byte(*jsonV), &parsed); err == nil {
			return parsed
		}
		return *jsonV
	}
	return nil
}

// IsValidColumnName validates column names to prevent SQL injection.
func IsValidColumnName(name string) bool {
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return len(name) > 0 && len(name) < 64
}

// mapAggFunction maps ThingsBoard aggregation types to SQL functions.
func mapAggFunction(agg string) string {
	switch strings.ToUpper(agg) {
	case "AVG":
		return "avg"
	case "MIN":
		return "min"
	case "MAX":
		return "max"
	case "SUM":
		return "sum"
	case "COUNT":
		return "count"
	default:
		return "avg"
	}
}

// msToSampleBy converts milliseconds to QuestDB SAMPLE BY syntax.
func msToSampleBy(ms int64) string {
	switch {
	case ms >= 86400000:
		days := ms / 86400000
		return fmt.Sprintf("%dd", days)
	case ms >= 3600000:
		hours := ms / 3600000
		return fmt.Sprintf("%dh", hours)
	case ms >= 60000:
		minutes := ms / 60000
		return fmt.Sprintf("%dm", minutes)
	case ms >= 1000:
		seconds := ms / 1000
		return fmt.Sprintf("%ds", seconds)
	default:
		return "1s"
	}
}

// toInt64 / toFloat64 normalise the values the QuestDB driver returns
// across its numeric column kinds. The pgx driver hands integer columns
// back as int64, but BYTE / SHORT can come through as the smaller width
// and FLOAT can come through as float32. Defensive coercion keeps the
// "zero suppression" check correct regardless of underlying width.
func toInt64(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case int:
		return int64(x), true
	}
	return 0, false
}

func toFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	}
	return 0, false
}

// getEnv mirrors the helper in package main — kept private here so
// the package is self-contained. Used by InitReader to resolve the
// QUESTDB_PG_* read-connection env contract.
func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ttlDays is the retention window applied to device_telemetry on the
// QuestDB read backend.
// Memory: never extend Flow audit/telemetry without TTL — the original
// flow_decisions table grew to 400GB with no bound. 90 days is the chosen
// default; override with DEVICE_TELEMETRY_TTL_DAYS.
func ttlDays() int {
	if v := os.Getenv("DEVICE_TELEMETRY_TTL_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 90
}

// ensureTTL applies `ALTER TABLE device_telemetry SET TTL N DAY`
// idempotently. QuestDB drops whole partitions whose end-timestamp is older
// than N days in the background; setting the same value twice is a no-op.
//
// Called once from InitReader in the live store=="questdb" branch (best
// effort — the table may not exist yet on a brand-new install).
func ensureTTL() {
	if PG == nil {
		return
	}
	days := ttlDays()
	stmt := fmt.Sprintf("ALTER TABLE device_telemetry SET TTL %d DAY", days)
	if _, err := PG.Exec(stmt); err != nil {
		// Don't shout if the table just hasn't been created yet. Other
		// errors deserve a WARN.
		if strings.Contains(err.Error(), "table does not exist") ||
			strings.Contains(err.Error(), "does not exist") {
			log.Printf("device_telemetry TTL deferred: table not yet created")
			return
		}
		log.Printf("WARN device_telemetry TTL apply: %v", err)
		return
	}
	log.Printf("device_telemetry TTL = %d DAY", days)
}
