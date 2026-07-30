package entityquery

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
	"flow-core/internal/telemetry"
	"flow-core/internal/twinstore"
)

// Find processes POST /api/entitiesQuery/find
// This is TB's universal entity query engine used by widgets for dynamic entity resolution.
func Find(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	log.Printf("DEBUG entitiesQuery/find: %s", jsonStr(body))

	entityFilter, _ := body["entityFilter"].(map[string]interface{})
	pageLink, _ := body["pageLink"].(map[string]interface{})
	entityFields, _ := body["entityFields"].([]interface{})
	latestValues, _ := body["latestValues"].([]interface{})

	if entityFilter == nil {
		httputil.WriteError(w, http.StatusBadRequest, "Missing entityFilter")
		return
	}

	filterType, _ := entityFilter["type"].(string)

	var entities []map[string]interface{}

	switch filterType {
	case "singleEntity":
		entities = handleSingleEntityFilter(tenantId, entityFilter)
	case "entityList":
		entities = handleEntityListFilter(tenantId, entityFilter)
	case "deviceType", "entityType":
		entities = handleDeviceTypeFilter(tenantId, entityFilter, pageLink)
	case "assetType":
		entities = handleAssetTypeFilter(tenantId, entityFilter, pageLink)
	case "apiUsageState":
		entities = handleApiUsageStateFilter(tenantId)
	default:
		log.Printf("WARN: Unsupported entity filter type: %s — returning empty", filterType)
		entities = []map[string]interface{}{}
	}

	// Build response in TB format
	data := []map[string]interface{}{}
	for _, entity := range entities {
		entityId := entity["id"].(string)
		entityType := entity["entityType"].(string)

		item := map[string]interface{}{
			"entityId": map[string]interface{}{
				"entityType": entityType,
				"id":         entityId,
			},
		}

		// Build entityFields (latest entity field values)
		if len(entityFields) > 0 {
			latestMap := map[string]map[string]interface{}{}
			for _, ef := range entityFields {
				efMap, _ := ef.(map[string]interface{})
				fieldType, _ := efMap["type"].(string)
				fieldKey, _ := efMap["key"].(string)

				if fieldType == "ENTITY_FIELD" {
					val := entity[fieldKey]
					ts := entity["createdTime"]
					if ts == nil {
						ts = 0
					}
					if _, ok := latestMap[fieldType]; !ok {
						latestMap[fieldType] = map[string]interface{}{}
					}
					latestMap[fieldType][fieldKey] = map[string]interface{}{
						"ts":    ts,
						"value": val,
					}
				}
			}
			item["latest"] = latestMap
		}

		// Build latestValues (ATTRIBUTE, TIME_SERIES)
		if len(latestValues) > 0 {
			latest, ok := item["latest"].(map[string]map[string]interface{})
			if !ok {
				latest = map[string]map[string]interface{}{}
			}

			for _, lv := range latestValues {
				lvMap, _ := lv.(map[string]interface{})
				lvType, _ := lvMap["type"].(string)
				lvKey, _ := lvMap["key"].(string)

				if _, ok := latest[lvType]; !ok {
					latest[lvType] = map[string]interface{}{}
				}

				if lvType == "ATTRIBUTE" || lvType == "SERVER_ATTRIBUTE" || lvType == "CLIENT_ATTRIBUTE" || lvType == "SHARED_ATTRIBUTE" {
					val := fetchLatestAttribute(entityId, lvKey, lvType)
					latest[lvType][lvKey] = val
				} else if lvType == "TIME_SERIES" {
					val := fetchLatestTimeseries(tenantId, entityType, entityId, lvKey)
					latest[lvType][lvKey] = val
				}
			}
			item["latest"] = latest
		}

		data = append(data, item)
	}

	totalElements := len(data)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    1,
		"totalElements": totalElements,
		"hasNext":       false,
	})
}

// FindKeys processes POST /api/entitiesQuery/find/keys.
// ThingsBoard calls this endpoint while configuring widgets to autocomplete
// telemetry/attribute keys for the entities matched by an entity query.
func FindKeys(w http.ResponseWriter, r *http.Request) {
	keys, ok := collectFindKeys(w, r)
	if !ok {
		return
	}

	httputil.WriteJSON(w, http.StatusOK, legacyFindKeysResponse(keys))
}

// FindKeysV2 processes POST /api/v2/entitiesQuery/find/keys.
// TB 4.x returns an AvailableEntityKeysV2 object instead of the deprecated
// flat EntityKey array. The widget editor reads the `timeseries` field.
func FindKeysV2(w http.ResponseWriter, r *http.Request) {
	keys, ok := collectFindKeys(w, r)
	if !ok {
		return
	}

	timeseries := []map[string]string{}
	for _, item := range keys {
		if strings.EqualFold(item["type"], "TIME_SERIES") {
			key := item["key"]
			timeseries = append(timeseries, map[string]string{
				"key":       key,
				"name":      key,
				"label":     key,
				"type":      "TIME_SERIES",
				"valueType": "NUMERIC",
			})
		}
	}

	response := map[string]interface{}{
		"timeseries": timeseries,
		"attributes": []map[string]string{},
	}
	httputil.WriteJSON(w, http.StatusOK, response)
}

func collectFindKeys(w http.ResponseWriter, r *http.Request) ([]map[string]string, bool) {
	if r.Method != "GET" && r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return nil, false
	}

	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return nil, false
	}
	tenantId, _ := claims["tenantId"].(string)

	body := map[string]interface{}{}
	if r.Method == "POST" && r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "Invalid body")
			return nil, false
		}
		if strings.TrimSpace(string(raw)) != "" {
			if err := json.Unmarshal(raw, &body); err != nil {
				httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
				return nil, false
			}
		}
	}

	entityFilter, _ := body["entityFilter"].(map[string]interface{})
	pageLink, _ := body["pageLink"].(map[string]interface{})
	if entityFilter == nil {
		entityFilter = entityFilterFromQuery(r)
	}

	entities := entitiesForFilter(tenantId, entityFilter, pageLink)
	keyTypes := requestedKeyTypes(body, r)
	includeTimeseries := keyTypes.includeTimeseries()

	seen := map[string]bool{}
	keys := []map[string]string{}
	if includeTimeseries {
		for _, entity := range entities {
			entityID, _ := entity["id"].(string)
			entityType, _ := entity["entityType"].(string)
			if !strings.EqualFold(entityType, "DEVICE") || entityID == "" {
				continue
			}
			for _, key := range telemetryKeysForDevice(tenantId, entityType, entityID) {
				if key == "" || seen["TIME_SERIES:"+key] {
					continue
				}
				seen["TIME_SERIES:"+key] = true
				keys = append(keys, map[string]string{
					"type": "TIME_SERIES",
					"key":  key,
				})
			}
		}
	}
	return keys, true
}

type requestedKeys struct {
	any        bool
	timeseries bool
	attribute  bool
}

func (r requestedKeys) includeTimeseries() bool {
	return !r.any || r.timeseries
}

func legacyFindKeysResponse(keys []map[string]string) map[string]interface{} {
	timeseries := []string{}
	attributes := []string{}
	seenTimeseries := map[string]bool{}
	seenAttributes := map[string]bool{}

	for _, item := range keys {
		key := item["key"]
		if key == "" {
			continue
		}
		switch strings.ToUpper(item["type"]) {
		case "TIME_SERIES", "TIMESERIES":
			if !seenTimeseries[key] {
				seenTimeseries[key] = true
				timeseries = append(timeseries, key)
			}
		case "ATTRIBUTE":
			if !seenAttributes[key] {
				seenAttributes[key] = true
				attributes = append(attributes, key)
			}
		}
	}

	return map[string]interface{}{
		"attribute":   attributes,
		"timeseries":  timeseries,
		"entityTypes": []string{},
	}
}

// ─── Filter handlers ────────────────────────────────────────────────────────

func entityFilterFromQuery(r *http.Request) map[string]interface{} {
	filterType := r.URL.Query().Get("entityFilterType")
	entityType := r.URL.Query().Get("entityType")
	entityID := r.URL.Query().Get("entityId")
	if filterType == "" {
		filterType = "entityType"
	}
	if entityType == "" {
		entityType = "DEVICE"
	}
	if strings.EqualFold(filterType, "singleEntity") && entityID != "" {
		return map[string]interface{}{
			"type": "singleEntity",
			"singleEntity": map[string]interface{}{
				"entityType": entityType,
				"id":         entityID,
			},
		}
	}
	return map[string]interface{}{
		"type":       "entityType",
		"entityType": entityType,
	}
}

func entitiesForFilter(tenantId string, entityFilter map[string]interface{}, pageLink map[string]interface{}) []map[string]interface{} {
	filterType, _ := entityFilter["type"].(string)
	switch filterType {
	case "singleEntity":
		return handleSingleEntityFilter(tenantId, entityFilter)
	case "entityList":
		return handleEntityListFilter(tenantId, entityFilter)
	case "deviceType", "entityType":
		return handleDeviceTypeFilter(tenantId, entityFilter, pageLink)
	case "assetType":
		return handleAssetTypeFilter(tenantId, entityFilter, pageLink)
	case "apiUsageState":
		return handleApiUsageStateFilter(tenantId)
	default:
		log.Printf("WARN: Unsupported entity keys filter type: %s — returning empty", filterType)
		return []map[string]interface{}{}
	}
}

func requestedKeyTypes(body map[string]interface{}, r *http.Request) requestedKeys {
	result := requestedKeys{}
	raw, ok := body["keyTypes"].([]interface{})
	if ok {
		for _, item := range raw {
			if text, ok := item.(string); ok && text != "" {
				result.any = true
				switch strings.ToUpper(text) {
				case "TIME_SERIES", "TIMESERIES":
					result.timeseries = true
				case "ATTRIBUTE":
					result.attribute = true
				}
			}
		}
	}

	query := r.URL.Query()
	if value := query.Get("timeseries"); value != "" {
		result.any = true
		result.timeseries = strings.EqualFold(value, "true")
	}
	if value := query.Get("attributes"); value != "" {
		result.any = true
		result.attribute = strings.EqualFold(value, "true")
	}
	if value := query.Get("attribute"); value != "" {
		result.any = true
		result.attribute = strings.EqualFold(value, "true")
	}

	return result
}

func telemetryKeysForDevice(tenantId, entityType, entityID string) []string {
	seen := map[string]bool{}
	keys := []string{}

	if store := twinstore.Global(); store != nil && tenantId != "" {
		if kvKeys, err := store.GetTelemetryKeys(context.Background(), tenantId, entityType, entityID); err == nil {
			for _, key := range kvKeys {
				if key != "" && !seen[key] {
					seen[key] = true
					keys = append(keys, key)
				}
			}
		}
	}

	if historyKeys, ok := telemetry.QueryQuestDBKVKeys(tenantId, entityID); ok {
		for _, key := range historyKeys {
			if key != "" && !seen[key] {
				seen[key] = true
				keys = append(keys, key)
			}
		}
	}

	return keys
}

func handleSingleEntityFilter(tenantId string, filter map[string]interface{}) []map[string]interface{} {
	singleEntity, _ := filter["singleEntity"].(map[string]interface{})
	if singleEntity == nil {
		return nil
	}

	entityType, entityId := entityRef(singleEntity, "")

	if entityType == "" || entityId == "" {
		return nil
	}

	entity := resolveEntity(tenantId, entityType, entityId)
	if entity == nil {
		return nil
	}
	return []map[string]interface{}{entity}
}

func handleEntityListFilter(tenantId string, filter map[string]interface{}) []map[string]interface{} {
	entityType, _ := filter["entityType"].(string)
	entityListRaw, _ := filter["entityList"].([]interface{})

	var results []map[string]interface{}
	for _, e := range entityListRaw {
		itemType, idStr := entityRef(e, entityType)
		if idStr == "" {
			continue
		}
		entity := resolveEntity(tenantId, itemType, idStr)
		if entity != nil {
			results = append(results, entity)
		}
	}
	return results
}

func entityRef(value interface{}, fallbackType string) (string, string) {
	switch v := value.(type) {
	case string:
		return fallbackType, v
	case map[string]interface{}:
		entityType, _ := v["entityType"].(string)
		if entityType == "" {
			entityType = fallbackType
		}
		switch id := v["id"].(type) {
		case string:
			return entityType, id
		case map[string]interface{}:
			nestedType, _ := id["entityType"].(string)
			if nestedType != "" {
				entityType = nestedType
			}
			nestedID, _ := id["id"].(string)
			return entityType, nestedID
		}
	}
	return fallbackType, ""
}

func handleDeviceTypeFilter(tenantId string, filter map[string]interface{}, pageLink map[string]interface{}) []map[string]interface{} {
	deviceTypes := deviceTypesFromFilter(filter)

	pageSize := 100
	if pageLink != nil {
		if ps, ok := pageLink["pageSize"].(float64); ok {
			pageSize = int(ps)
		}
	}

	query := "SELECT id, created_time, name, type, label FROM device WHERE tenant_id = $1"
	args := []interface{}{tenantId}
	argIdx := 2

	if len(deviceTypes) == 1 {
		query += " AND type = $" + strconv.Itoa(argIdx)
		args = append(args, deviceTypes[0])
		argIdx++
	} else if len(deviceTypes) > 1 {
		placeholders := make([]string, 0, len(deviceTypes))
		for _, deviceType := range deviceTypes {
			placeholders = append(placeholders, "$"+strconv.Itoa(argIdx))
			args = append(args, deviceType)
			argIdx++
		}
		query += " AND type IN (" + strings.Join(placeholders, ",") + ")"
	}

	// Support textSearch from pageLink
	if pageLink != nil {
		if textSearch, ok := pageLink["textSearch"].(string); ok && textSearch != "" {
			query += " AND LOWER(name) LIKE $" + strconv.Itoa(argIdx)
			args = append(args, "%"+strings.ToLower(textSearch)+"%")
			argIdx++
		}
	}

	query += " ORDER BY name LIMIT $" + strconv.Itoa(argIdx)
	args = append(args, pageSize)

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("ERROR entity query deviceType: %v", err)
		return nil
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, name, devType string
		var createdTime int64
		var label *string
		rows.Scan(&id, &createdTime, &name, &devType, &label)

		entity := map[string]interface{}{
			"id":          id,
			"entityType":  "DEVICE",
			"createdTime": createdTime,
			"name":        name,
			"type":        devType,
		}
		if label != nil {
			entity["label"] = *label
		} else {
			entity["label"] = ""
		}
		results = append(results, entity)
	}
	return results
}

func deviceTypesFromFilter(filter map[string]interface{}) []string {
	if raw, ok := filter["deviceTypes"].([]interface{}); ok {
		out := make([]string, 0, len(raw))
		for _, item := range raw {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	if s, _ := filter["deviceType"].(string); s != "" {
		return []string{s}
	}
	// For generic entityType filters.
	if s, _ := filter["entitySubtype"].(string); s != "" {
		return []string{s}
	}
	return nil
}

// handleApiUsageStateFilter resolves the tenant's api_usage_state row so that
// the "Utilisation de l'API" dashboard's REST entitiesQuery/find call returns
// the entity its widgets are bound to. Same flow as the WS apiUsageState branch.
func handleApiUsageStateFilter(tenantId string) []map[string]interface{} {
	if dbpkg.Pool == nil || tenantId == "" {
		return nil
	}
	var id string
	var createdTime int64
	if err := dbpkg.Pool.QueryRow(
		"SELECT id, created_time FROM api_usage_state WHERE tenant_id = $1 LIMIT 1",
		tenantId,
	).Scan(&id, &createdTime); err != nil {
		return nil
	}
	return []map[string]interface{}{{
		"id":          id,
		"entityType":  "API_USAGE_STATE",
		"createdTime": createdTime,
		"name":        "Tenant",
		"label":       "",
		"type":        "",
	}}
}

func handleAssetTypeFilter(tenantId string, filter map[string]interface{}, pageLink map[string]interface{}) []map[string]interface{} {
	assetType, _ := filter["assetType"].(string)

	pageSize := 100
	if pageLink != nil {
		if ps, ok := pageLink["pageSize"].(float64); ok {
			pageSize = int(ps)
		}
	}

	query := "SELECT id, created_time, name, type, label FROM asset WHERE tenant_id = $1"
	args := []interface{}{tenantId}
	argIdx := 2

	if assetType != "" {
		query += " AND type = $" + strconv.Itoa(argIdx)
		args = append(args, assetType)
		argIdx++
	}

	query += " ORDER BY name LIMIT $" + strconv.Itoa(argIdx)
	args = append(args, pageSize)

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("ERROR entity query assetType: %v", err)
		return nil
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, name, aType string
		var createdTime int64
		var label *string
		rows.Scan(&id, &createdTime, &name, &aType, &label)

		entity := map[string]interface{}{
			"id":          id,
			"entityType":  "ASSET",
			"createdTime": createdTime,
			"name":        name,
			"type":        aType,
		}
		if label != nil {
			entity["label"] = *label
		} else {
			entity["label"] = ""
		}
		results = append(results, entity)
	}
	return results
}

// ─── Entity resolver ────────────────────────────────────────────────────────

func resolveEntity(tenantId, entityType, entityId string) map[string]interface{} {
	switch entityType {
	case "DEVICE":
		var name, devType string
		var createdTime int64
		var label *string
		err := dbpkg.Pool.QueryRow("SELECT name, type, created_time, label FROM device WHERE id = $1 AND tenant_id = $2",
			entityId, tenantId).Scan(&name, &devType, &createdTime, &label)
		if err != nil {
			return nil
		}
		entity := map[string]interface{}{
			"id":          entityId,
			"entityType":  "DEVICE",
			"createdTime": createdTime,
			"name":        name,
			"type":        devType,
		}
		if label != nil {
			entity["label"] = *label
		}
		return entity

	case "ASSET":
		var name, aType string
		var createdTime int64
		var label *string
		err := dbpkg.Pool.QueryRow("SELECT name, type, created_time, label FROM asset WHERE id = $1 AND tenant_id = $2",
			entityId, tenantId).Scan(&name, &aType, &createdTime, &label)
		if err != nil {
			return nil
		}
		entity := map[string]interface{}{
			"id":          entityId,
			"entityType":  "ASSET",
			"createdTime": createdTime,
			"name":        name,
			"type":        aType,
		}
		if label != nil {
			entity["label"] = *label
		}
		return entity

	case "DASHBOARD":
		var title string
		var createdTime int64
		err := dbpkg.Pool.QueryRow("SELECT title, created_time FROM dashboard WHERE id = $1 AND tenant_id = $2",
			entityId, tenantId).Scan(&title, &createdTime)
		if err != nil {
			return nil
		}
		return map[string]interface{}{
			"id":          entityId,
			"entityType":  "DASHBOARD",
			"createdTime": createdTime,
			"name":        title,
		}

	case "TENANT":
		var title string
		var createdTime int64
		err := dbpkg.Pool.QueryRow("SELECT title, created_time FROM tenant WHERE id = $1",
			entityId).Scan(&title, &createdTime)
		if err != nil {
			return nil
		}
		return map[string]interface{}{
			"id":          entityId,
			"entityType":  "TENANT",
			"createdTime": createdTime,
			"name":        title,
		}

	default:
		log.Printf("WARN: resolveEntity unsupported entity type: %s", entityType)
		return nil
	}
}

// ─── Latest value fetchers ──────────────────────────────────────────────────

func fetchLatestAttribute(entityId, key, attrType string) map[string]interface{} {
	if dbpkg.Pool == nil {
		return map[string]interface{}{"ts": 0, "value": ""}
	}

	// Map attribute type to attribute_type int
	attrTypeInt := 0 // CLIENT_SCOPE
	switch attrType {
	case "SERVER_ATTRIBUTE":
		attrTypeInt = 2
	case "SHARED_ATTRIBUTE":
		attrTypeInt = 1
	case "CLIENT_ATTRIBUTE":
		attrTypeInt = 0
	case "ATTRIBUTE":
		// Any scope — try all
		attrTypeInt = -1
	}

	var query string
	var args []interface{}

	if attrTypeInt == -1 {
		query = `SELECT a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v, a.last_update_ts
			FROM attribute_kv a
			JOIN key_dictionary k ON a.attribute_key = k.key_id
			WHERE a.entity_id = $1 AND k.key = $2
			ORDER BY a.last_update_ts DESC LIMIT 1`
		args = []interface{}{entityId, key}
	} else {
		query = `SELECT a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v, a.last_update_ts
			FROM attribute_kv a
			JOIN key_dictionary k ON a.attribute_key = k.key_id
			WHERE a.entity_id = $1 AND k.key = $2 AND a.attribute_type = $3
			ORDER BY a.last_update_ts DESC LIMIT 1`
		args = []interface{}{entityId, key, attrTypeInt}
	}

	var boolV *bool
	var strV *string
	var longV *int64
	var dblV *float64
	var jsonV *string
	var lastTs int64

	err := dbpkg.Pool.QueryRow(query, args...).Scan(&boolV, &strV, &longV, &dblV, &jsonV, &lastTs)
	if err != nil {
		return map[string]interface{}{"ts": 0, "value": ""}
	}

	var value interface{}
	if boolV != nil {
		value = *boolV
	} else if strV != nil {
		value = *strV
	} else if longV != nil {
		value = *longV
	} else if dblV != nil {
		value = *dblV
	} else if jsonV != nil {
		value = *jsonV
	} else {
		value = ""
	}

	return map[string]interface{}{
		"ts":    lastTs,
		"value": value,
	}
}

func fetchLatestTimeseries(tenantId, entityType, entityId, key string) map[string]interface{} {
	if store := twinstore.Global(); store != nil && tenantId != "" {
		if values, err := store.GetLatestTelemetry(context.Background(), tenantId, entityType, entityId, []string{key}); err == nil {
			if value, ok := values[key]; ok {
				return map[string]interface{}{"ts": value.TS, "value": value.Value}
			}
		}
	}
	if natsTwinStateAuthoritative(entityType) {
		// Twin-state KV had no value (e.g. latest-KV pipeline not populating the
		// bucket). Fall back to the GreptimeDB history KV last-value so entity-table
		// / map / value widgets show current values instead of blank. The old QuestDB
		// wide-table query below targets device_telemetry, which does not exist on the
		// GreptimeDB default store, so it can never serve this path.
		if telemetry.PG != nil {
			if ts, value, ok := telemetry.DeviceKVLatest(tenantId, entityId, key, false); ok {
				return map[string]interface{}{"ts": ts, "value": value}
			}
		}
		return map[string]interface{}{"ts": 0, "value": ""}
	}
	// Non-device entity latest (api_usage_state, asset, …): serve the last point from
	// GreptimeDB entity_telemetry_kv when the read backend is flipped. tenantId is
	// resolved here, so it is passed through non-empty — the rendered last-point query
	// carries a MANDATORY `AND tenant_id = <session tenant>` predicate (two-layer
	// isolation). Default postgres ⇒ the unchanged Postgres path below.
	if telemetry.UsageReadBackend() == "greptime" && telemetry.PG != nil && !strings.EqualFold(entityType, "DEVICE") {
		if ts, value, ok := telemetry.EntityKVLatest(entityType, entityId, tenantId, key); ok {
			return map[string]interface{}{"ts": ts, "value": value}
		}
		return map[string]interface{}{"ts": 0, "value": ""}
	}
	// The legacy QuestDB wide-table (`device_telemetry`) latest lookup was
	// removed: no pipeline writes that per-metric-column schema, so it was dead
	// code with a raw-`%s` IDOR shape. Device latest comes from twin state / the
	// *_kv history path above; the narrow PostgreSQL compatibility table below
	// remains the last-resort fallback.
	if dbpkg.Pool != nil {
		if val := fetchLatestTimeseriesFromPostgres(entityId, key); val != nil {
			return val
		}
	}

	return map[string]interface{}{"ts": 0, "value": ""}
}

func natsTwinStateAuthoritative(entityType string) bool {
	return strings.EqualFold(entityType, "DEVICE") && strings.EqualFold(strings.TrimSpace(os.Getenv("TWIN_STATE_STORE")), "nats")
}

func fetchLatestTimeseriesFromPostgres(entityId, key string) map[string]interface{} {
	keyId := dbpkg.GetOrInsertKeyID(key)
	if keyId <= 0 {
		return nil
	}
	var boolV *bool
	var strV *string
	var longV *int64
	var dblV *float64
	var ts int64
	err := dbpkg.Pool.QueryRow(`SELECT bool_v, str_v, long_v, dbl_v, ts FROM ts_kv
		WHERE entity_id = $1 AND key = $2 ORDER BY ts DESC LIMIT 1`,
		entityId, keyId).Scan(&boolV, &strV, &longV, &dblV, &ts)
	if err != nil {
		return nil
	}
	var value interface{} = ""
	if boolV != nil {
		value = *boolV
	} else if strV != nil {
		value = *strV
	} else if longV != nil {
		value = *longV
	} else if dblV != nil {
		value = *dblV
	}
	return map[string]interface{}{
		"ts":    ts,
		"value": value,
	}
}

func jsonStr(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
