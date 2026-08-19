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
	"flow-core/internal/topology"
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
	relationsQueryItems := false

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
	case "relationsQuery":
		entities = handleRelationsQueryFilter(w, r, tenantId, claims, entityFilter, entityFields, latestValues)
		relationsQueryItems = true
	// The filter names below are TB's older vocabulary for the same
	// screens. They used to fall through to the default case and answer an
	// empty 200 with only a server-side WARN, even though the tables and
	// the traversal primitive they need are all real and used elsewhere
	// (docs/UI_CONTRACT_DATA_FIDELITY.md P2).
	case "entityViewType":
		entities = handleEntityViewTypeFilter(tenantId, entityFilter, pageLink)
	case "entityName":
		entities = handleEntityNameFilter(tenantId, entityFilter, pageLink)
	case "deviceSearchQuery", "assetSearchQuery", "entityViewSearchQuery":
		entities = handleEntitySearchQueryFilter(tenantId, filterType, entityFilter)
	case "stateEntityOwner":
		entities = handleStateEntityOwnerFilter(tenantId, entityFilter)
	default:
		log.Printf("WARN: Unsupported entity filter type: %s — returning empty", filterType)
		entities = []map[string]interface{}{}
	}

	// relationsQuery returns pre-shaped WS-proven items
	// ({entityId, level, latest, timeseries, aggLatest}); wrap them in the
	// standard envelope without re-running the generic builder.
	if relationsQueryItems {
		httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
			"data":          entities,
			"totalPages":    1,
			"totalElements": len(entities),
			"hasNext":       false,
		})
		return
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

	// Every element costs one resolveEntity round-trip to Postgres, so an
	// unbounded body-supplied list is a request amplifier: 100k ids = 100k
	// queries from a single POST. Bound it at the page-size ceiling — a real
	// dashboard's entityList filter holds a handful of pinned entities, never
	// more than a page's worth.
	if len(entityListRaw) > httputil.MaxPageSize {
		log.Printf("WARN entityList filter truncated: %d ids > max %d", len(entityListRaw), httputil.MaxPageSize)
		entityListRaw = entityListRaw[:httputil.MaxPageSize]
	}

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

	// pageSize comes from the request BODY, so it bypasses the query-string
	// clamp entirely: an unbounded LIMIT here streams the tenant's whole
	// device/asset table into Go maps. Same bound as the REST plane.
	pageSize := 100
	if pageLink != nil {
		if ps, ok := pageLink["pageSize"].(float64); ok {
			pageSize = int(ps)
		}
	}
	pageSize = httputil.ClampPageSize(pageSize, 100)

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

// handleEntityViewTypeFilter serves the `entityViewType` filter — the
// entity_view equivalent of deviceType/assetType. Same pageSize clamp
// rationale as those: pageSize arrives in the request BODY and so bypasses
// the query-string clamp entirely.
func handleEntityViewTypeFilter(tenantId string, filter map[string]interface{}, pageLink map[string]interface{}) []map[string]interface{} {
	viewType, _ := filter["entityViewType"].(string)
	if viewType == "" {
		viewType, _ = filter["entityViewTypes"].(string)
	}

	pageSize := 100
	if pageLink != nil {
		if ps, ok := pageLink["pageSize"].(float64); ok {
			pageSize = int(ps)
		}
	}
	pageSize = httputil.ClampPageSize(pageSize, 100)

	query := "SELECT id, created_time, name, type FROM entity_view WHERE tenant_id = $1"
	args := []interface{}{tenantId}
	argIdx := 2
	if viewType != "" {
		query += " AND type = $" + strconv.Itoa(argIdx)
		args = append(args, viewType)
		argIdx++
	}
	if nameFilter, _ := filter["entityViewNameFilter"].(string); nameFilter != "" {
		query += " AND LOWER(name) LIKE $" + strconv.Itoa(argIdx)
		args = append(args, strings.ToLower(nameFilter)+"%")
		argIdx++
	}
	query += " ORDER BY name LIMIT $" + strconv.Itoa(argIdx)
	args = append(args, pageSize)

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("ERROR entity query entityViewType: %v", err)
		return nil
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, name, vType string
		var createdTime int64
		if rows.Scan(&id, &createdTime, &name, &vType) != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"id": id, "entityType": "ENTITY_VIEW", "createdTime": createdTime,
			"name": name, "type": vType,
		})
	}
	return results
}

// entityNameTable maps an entityType to the table and name column the
// entityName / stateEntityOwner filters need. Only types with a real backing
// table are listed; anything else is refused rather than guessed at.
func entityNameTable(entityType string) (table, nameCol string, ok bool) {
	switch strings.ToUpper(strings.TrimSpace(entityType)) {
	case "DEVICE":
		return "device", "name", true
	case "ASSET":
		return "asset", "name", true
	case "ENTITY_VIEW":
		return "entity_view", "name", true
	case "DASHBOARD":
		return "dashboard", "title", true
	case "CUSTOMER":
		return "customer", "title", true
	case "USER":
		return "tb_user", "email", true
	}
	return "", "", false
}

// handleEntityNameFilter serves the `entityName` filter: a name-prefix search
// scoped to one entity type. TB matches by prefix, not substring.
func handleEntityNameFilter(tenantId string, filter map[string]interface{}, pageLink map[string]interface{}) []map[string]interface{} {
	entityType, _ := filter["entityType"].(string)
	table, nameCol, ok := entityNameTable(entityType)
	if !ok {
		log.Printf("WARN: entityName filter on unsupported entity type: %s", entityType)
		return nil
	}
	entityType = strings.ToUpper(strings.TrimSpace(entityType))
	nameFilter, _ := filter["entityNameFilter"].(string)

	pageSize := 100
	if pageLink != nil {
		if ps, ok := pageLink["pageSize"].(float64); ok {
			pageSize = int(ps)
		}
	}
	pageSize = httputil.ClampPageSize(pageSize, 100)

	// table/nameCol come from the closed allow-list above, never from the
	// request, so they are safe to interpolate; every value stays bound.
	query := "SELECT id, created_time, " + nameCol + " FROM " + table + " WHERE tenant_id = $1"
	args := []interface{}{tenantId}
	argIdx := 2
	if nameFilter != "" {
		query += " AND LOWER(" + nameCol + ") LIKE $" + strconv.Itoa(argIdx)
		args = append(args, strings.ToLower(nameFilter)+"%")
		argIdx++
	}
	query += " ORDER BY " + nameCol + " LIMIT $" + strconv.Itoa(argIdx)
	args = append(args, pageSize)

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("ERROR entity query entityName: %v", err)
		return nil
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, name string
		var createdTime int64
		if rows.Scan(&id, &createdTime, &name) != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"id": id, "entityType": entityType, "createdTime": createdTime, "name": name,
		})
	}
	return results
}

// handleEntitySearchQueryFilter serves the `deviceSearchQuery`,
// `assetSearchQuery` and `entityViewSearchQuery` filters: walk the relation
// graph out from a root entity, then keep only neighbours of the requested
// entity type (optionally narrowed to specific subtypes).
//
// It reuses topology.NeighborsTenant — the same tenant-scoped traversal
// primitive relationsQuery uses, with the tenant predicate carried inside the
// query rather than applied afterwards, so a foreign root yields nothing
// rather than leaking. Unlike relationsQuery this returns the generic entity
// shape, because these filters feed ordinary entity tables in the UI.
func handleEntitySearchQueryFilter(tenantId, filterType string, filter map[string]interface{}) []map[string]interface{} {
	if dbpkg.Pool == nil {
		return nil
	}

	var wantType string
	var subtypeKeys []string
	switch filterType {
	case "deviceSearchQuery":
		wantType, subtypeKeys = "DEVICE", []string{"deviceTypes"}
	case "assetSearchQuery":
		wantType, subtypeKeys = "ASSET", []string{"assetTypes"}
	case "entityViewSearchQuery":
		wantType, subtypeKeys = "ENTITY_VIEW", []string{"entityViewTypes"}
	default:
		return nil
	}

	// TB nests the traversal parameters under "rootEntity"/"relationType"
	// directly on the filter, older payloads under a "rootEntity" sibling.
	rootType, rootID := entityRef(filter["rootEntity"], "")
	if rootType == "" || rootID == "" {
		log.Printf("WARN: %s filter without a resolvable rootEntity", filterType)
		return nil
	}

	direction, _ := filter["direction"].(string)
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction != "TO" {
		direction = "FROM"
	}

	var relationTypes []string
	if raw, ok := filter["relationType"].(string); ok && raw != "" {
		relationTypes = []string{raw}
	}
	if raw, ok := filter["relationTypes"].([]interface{}); ok {
		for _, item := range raw {
			if s, ok := item.(string); ok && s != "" {
				relationTypes = append(relationTypes, s)
			}
		}
	}

	neighbours, err := topology.NeighborsTenant(dbpkg.Pool,
		tenantId,
		topology.EntityRef{Type: strings.ToUpper(strings.TrimSpace(rootType)), ID: rootID},
		direction, relationTypes)
	if err != nil {
		log.Printf("ERROR entity query %s: %v", filterType, err)
		return nil
	}

	// Optional subtype narrowing (deviceTypes / assetTypes / entityViewTypes).
	wantSubtypes := map[string]bool{}
	for _, key := range subtypeKeys {
		if raw, ok := filter[key].([]interface{}); ok {
			for _, item := range raw {
				if s, ok := item.(string); ok && s != "" {
					wantSubtypes[s] = true
				}
			}
		}
	}

	var results []map[string]interface{}
	for _, nb := range neighbours {
		if !strings.EqualFold(nb.Type, wantType) {
			continue
		}
		entity := resolveEntity(tenantId, wantType, nb.ID)
		if entity == nil {
			continue
		}
		if len(wantSubtypes) > 0 {
			subtype, _ := entity["type"].(string)
			if !wantSubtypes[subtype] {
				continue
			}
		}
		results = append(results, entity)
	}
	return results
}

// handleStateEntityOwnerFilter serves the `stateEntityOwner` filter: resolve
// the owner of the dashboard-state entity — its customer when it has one,
// otherwise the tenant. TB uses this to bind "owner" widgets.
func handleStateEntityOwnerFilter(tenantId string, filter map[string]interface{}) []map[string]interface{} {
	if dbpkg.Pool == nil {
		return nil
	}
	entityType, entityID := entityRef(filter["singleEntity"], "")
	if entityType == "" || entityID == "" {
		// TB also accepts the entity inline rather than under singleEntity.
		entityType, entityID = entityRef(filter, "")
	}
	table, _, ok := entityNameTable(entityType)
	if !ok || entityID == "" {
		log.Printf("WARN: stateEntityOwner filter without a resolvable entity (%s)", entityType)
		return nil
	}

	// table comes from the closed allow-list; the values stay bound.
	var customerID *string
	switch table {
	case "customer":
		// A customer owns itself.
		if entity := resolveEntity(tenantId, "CUSTOMER", entityID); entity != nil {
			return []map[string]interface{}{entity}
		}
		return nil
	case "dashboard":
		// Dashboards carry assigned_customers, not a single customer_id —
		// ownership there is the tenant.
	default:
		_ = dbpkg.Pool.QueryRow(
			"SELECT customer_id::text FROM "+table+" WHERE id = $1 AND tenant_id = $2",
			entityID, tenantId).Scan(&customerID)
	}

	// TB classic stores its "no owner" sentinel rather than NULL in
	// customer_id, so an unassigned entity must fall through to the tenant.
	if customerID != nil && *customerID != "" && !strings.HasPrefix(*customerID, "13814000-1dd2-11b2") {
		if entity := resolveEntity(tenantId, "CUSTOMER", *customerID); entity != nil {
			return []map[string]interface{}{entity}
		}
	}
	if entity := resolveEntity(tenantId, "TENANT", tenantId); entity != nil {
		return []map[string]interface{}{entity}
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

// handleRelationsQueryFilter serves the REST `relationsQuery` entity filter —
// the same tenant-scoped graph walk the WS plane exposes, reusing the proven
// WS BFS shape but with the tenant predicate carried inside the traversal
// primitive (topology.NeighborsTenant, Wave 1) instead of post-hoc scoping.
// It yields the same item shape as the proven WS branch:
//
//	{"entityId": {"entityType", "id"}, "level": <int>,
//	 "latest": {"ENTITY_FIELD": {...}, "ATTRIBUTE": {...}},
//	 "timeseries": {}, "aggLatest": {}}
//
// SECURITY: the root is attacker-supplied. It is therefore gated the same way
// the WS walk is: the root must belong to the caller's tenant (SYS_ADMIN may
// cross), otherwise 403 — never traverse a foreign root. Depth is clamped to
// DefaultExpandMaxDepth (400 on exceed); the visited-node budget returns 422.
func handleRelationsQueryFilter(w http.ResponseWriter, r *http.Request, tenantId string, claims map[string]interface{},
	entityFilter map[string]interface{}, entityFields []interface{}, latestValues []interface{}) []map[string]interface{} {

	if dbpkg.Pool == nil {
		return nil
	}

	rootType, rootID := entityRef(entityFilter["rootEntity"], "")
	if rootType == "" || rootID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing rootEntity")
		return nil
	}
	rootType = strings.ToUpper(strings.TrimSpace(rootType))

	// Direction defaults to FROM (outgoing) — same as the WS walk.
	direction, _ := entityFilter["direction"].(string)
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction == "" {
		direction = "FROM"
	}
	if direction != "FROM" && direction != "TO" {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid direction")
		return nil
	}

	maxLevel := 1
	if raw, ok := entityFilter["maxLevel"].(float64); ok {
		maxLevel = int(raw)
	}
	if maxLevel < 1 {
		maxLevel = 1
	}
	if maxLevel > topology.DefaultExpandMaxDepth {
		httputil.WriteError(w, http.StatusBadRequest, "maxLevel exceeds the expand depth limit")
		return nil
	}

	relationTypes := []string{"Contains"}
	if raw, ok := entityFilter["relationTypes"].([]interface{}); ok && len(raw) > 0 {
		relationTypes = relationTypes[:0]
		for _, item := range raw {
			if s, ok := item.(string); ok && s != "" {
				relationTypes = append(relationTypes, s)
			}
		}
	}

	allowedTypes := map[string]bool{}
	if raw, ok := entityFilter["entityTypes"].([]interface{}); ok {
		for _, item := range raw {
			if s, ok := item.(string); ok && s != "" {
				allowedTypes[strings.ToUpper(strings.TrimSpace(s))] = true
			}
		}
	}

	// Root ownership gate — SYS_ADMIN may cross tenants; everyone else is
	// confined to their own tenant. Fail closed on empty tenant. SYS_ADMIN
	// resolves the root's ACTUAL tenant so the traversal predicate is real,
	// never the empty/system JWT tenant.
	if callerIsSysAdmin(claims) {
		actual, err := topology.ResolveEntityTenant(dbpkg.Pool, topology.EntityRef{Type: rootType, ID: rootID})
		if err != nil {
			log.Printf("ERROR relationsQuery root resolution root_type=%s root_id=%s: %v", rootType, rootID, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Root entity resolution failed")
			return nil
		}
		tenantId = actual
	} else {
		if tenantId == "" {
			httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
			return nil
		}
		owned, err := topology.ResolveEntityTenant(dbpkg.Pool, topology.EntityRef{Type: rootType, ID: rootID})
		if err != nil {
			log.Printf("ERROR relationsQuery root resolution tenant_id=%s root_type=%s root_id=%s: %v", tenantId, rootType, rootID, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Root entity resolution failed")
			return nil
		}
		if owned != tenantId {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant relation query denied")
			return nil
		}
	}

	// Level-tracking BFS over the tenant-scoped one-hop primitive — the same
	// shape as the proven WS walk, but every hop carries the tenant predicate
	// at SQL level (NeighborsTenant), so a cross-tenant edge can never leak.
	type node struct {
		ID    string
		Type  string
		Level int
	}
	visited := map[string]bool{rootType + ":" + rootID: true}
	queue := []node{{ID: rootID, Type: rootType, Level: 0}}
	hits := []node{}
	budgetHit := false
	for len(queue) > 0 && !budgetHit {
		cur := queue[0]
		queue = queue[1:]
		if cur.Level >= maxLevel {
			continue
		}
		neighbors, err := topology.NeighborsTenant(dbpkg.Pool, tenantId,
			topology.EntityRef{Type: cur.Type, ID: cur.ID}, direction, relationTypes)
		if err != nil {
			log.Printf("ERROR relationsQuery neighbors tenant_id=%s node_type=%s node_id=%s: %v", tenantId, cur.Type, cur.ID, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Relation query failed")
			return nil
		}
		for _, neighbor := range neighbors {
			if len(visited) >= topology.DefaultExpandMaxNodes {
				log.Printf("WARN relationsQuery budget exhausted: root=%s visited=%d (cap %d)",
					rootID, len(visited), topology.DefaultExpandMaxNodes)
				budgetHit = true
				break
			}
			key := neighbor.Type + ":" + neighbor.ID
			if visited[key] {
				continue
			}
			visited[key] = true
			child := node{ID: neighbor.ID, Type: neighbor.Type, Level: cur.Level + 1}
			queue = append(queue, child)
			if len(allowedTypes) == 0 || allowedTypes[neighbor.Type] {
				hits = append(hits, child)
			}
		}
	}
	if budgetHit {
		httputil.WriteError(w, http.StatusUnprocessableEntity, "Relation query budget exceeded")
		return nil
	}

	fieldSpecs := parseEntityFieldSpecs(entityFields)
	attrKeys := parseAttrKeys(latestValues)

	out := make([]map[string]interface{}, 0, len(hits))
	for _, h := range hits {
		// Hydration is tenant scoped: a node reached through a cross-tenant
		// edge resolves to nil for this tenant and is silently dropped instead
		// of leaking (same defense-in-depth as the WS walk).
		entity := resolveEntity(tenantId, h.Type, h.ID)
		if entity == nil {
			continue
		}
		item := map[string]interface{}{
			"entityId":   map[string]interface{}{"entityType": h.Type, "id": h.ID},
			"level":      h.Level,
			"timeseries": map[string]interface{}{},
			"aggLatest":  map[string]interface{}{},
		}
		latest := map[string]interface{}{}
		if len(fieldSpecs) > 0 {
			latest["ENTITY_FIELD"] = buildEntityFieldLatest(entity, fieldSpecs)
		}
		if len(attrKeys) > 0 {
			attrs := map[string]interface{}{}
			for _, key := range attrKeys {
				if val := fetchLatestAttribute(h.ID, key, "SERVER_SCOPE"); val != nil {
					attrs[key] = val
				}
			}
			if len(attrs) > 0 {
				latest["ATTRIBUTE"] = attrs
			}
		}
		item["latest"] = latest
		out = append(out, item)
	}
	return out
}

// callerIsSysAdmin reports whether the verified JWT carries the SYS_ADMIN
// scope. Mirrors the identical check in internal/twin and internal/tenant —
// kept local to avoid a cross-package import.
func callerIsSysAdmin(claims map[string]interface{}) bool {
	scopes, _ := claims["scopes"].([]interface{})
	for _, s := range scopes {
		if str, ok := s.(string); ok && str == "SYS_ADMIN" {
			return true
		}
	}
	return false
}

// parseEntityFieldSpecs normalizes the entityFields array into the shape the
// WS buildEntityFieldLatest consumes (only ENTITY_FIELD entries matter here).
func parseEntityFieldSpecs(entityFields []interface{}) []struct {
	Type string `json:"type"`
	Key  string `json:"key"`
} {
	out := []struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	}{}
	for _, ef := range entityFields {
		efMap, _ := ef.(map[string]interface{})
		ftype, _ := efMap["type"].(string)
		fkey, _ := efMap["key"].(string)
		if ftype == "ENTITY_FIELD" && fkey != "" {
			out = append(out, struct {
				Type string `json:"type"`
				Key  string `json:"key"`
			}{Type: ftype, Key: fkey})
		}
	}
	return out
}

// parseAttrKeys extracts requested attribute keys from the latestValues array
// (ATTRIBUTE-typed entries only).
func parseAttrKeys(latestValues []interface{}) []string {
	keys := []string{}
	for _, lv := range latestValues {
		lvMap, _ := lv.(map[string]interface{})
		ftype, _ := lvMap["type"].(string)
		fkey, _ := lvMap["key"].(string)
		if strings.EqualFold(ftype, "ATTRIBUTE") && fkey != "" {
			keys = append(keys, fkey)
		}
	}
	return keys
}

// buildEntityFieldLatest maps requested ENTITY_FIELD keys to {ts, value} from
// an entity row (same shape as the WS builder).
func buildEntityFieldLatest(entity map[string]interface{}, fields []struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}) map[string]interface{} {
	out := map[string]interface{}{}
	ts, _ := entity["createdTime"].(int64)
	for _, f := range fields {
		if f.Type != "ENTITY_FIELD" {
			continue
		}
		out[f.Key] = map[string]interface{}{
			"ts":    ts,
			"value": entity[f.Key],
		}
	}
	return out
}

func handleAssetTypeFilter(tenantId string, filter map[string]interface{}, pageLink map[string]interface{}) []map[string]interface{} {
	assetType, _ := filter["assetType"].(string)

	// pageSize comes from the request BODY, so it bypasses the query-string
	// clamp entirely: an unbounded LIMIT here streams the tenant's whole
	// device/asset table into Go maps. Same bound as the REST plane.
	pageSize := 100
	if pageLink != nil {
		if ps, ok := pageLink["pageSize"].(float64); ok {
			pageSize = int(ps)
		}
	}
	pageSize = httputil.ClampPageSize(pageSize, 100)

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

	// CUSTOMER / USER / ENTITY_VIEW used to fall through to the default
	// case below — a singleEntity or entityList filter naming one of them
	// resolved to nil and was silently dropped from the result set, even
	// though all three tables are real and read elsewhere in this codebase
	// (docs/UI_CONTRACT_DATA_FIDELITY.md P2).
	case "CUSTOMER":
		var title string
		var createdTime int64
		err := dbpkg.Pool.QueryRow("SELECT title, created_time FROM customer WHERE id = $1 AND tenant_id = $2",
			entityId, tenantId).Scan(&title, &createdTime)
		if err != nil {
			return nil
		}
		return map[string]interface{}{
			"id":          entityId,
			"entityType":  "CUSTOMER",
			"createdTime": createdTime,
			"name":        title,
		}

	case "USER":
		var email string
		var createdTime int64
		var firstName, lastName *string
		err := dbpkg.Pool.QueryRow(
			"SELECT email, created_time, first_name, last_name FROM tb_user WHERE id = $1 AND tenant_id = $2",
			entityId, tenantId).Scan(&email, &createdTime, &firstName, &lastName)
		if err != nil {
			return nil
		}
		// TB names a user by their full name when set, falling back to the
		// email — the same precedence the user list surfaces.
		name := strings.TrimSpace(strings.TrimSpace(derefStr(firstName)) + " " + strings.TrimSpace(derefStr(lastName)))
		if name == "" {
			name = email
		}
		return map[string]interface{}{
			"id":          entityId,
			"entityType":  "USER",
			"createdTime": createdTime,
			"name":        name,
			"email":       email,
		}

	case "ENTITY_VIEW":
		var name, viewType string
		var createdTime int64
		err := dbpkg.Pool.QueryRow("SELECT name, type, created_time FROM entity_view WHERE id = $1 AND tenant_id = $2",
			entityId, tenantId).Scan(&name, &viewType, &createdTime)
		if err != nil {
			return nil
		}
		return map[string]interface{}{
			"id":          entityId,
			"entityType":  "ENTITY_VIEW",
			"createdTime": createdTime,
			"name":        name,
			"type":        viewType,
		}

	default:
		log.Printf("WARN: resolveEntity unsupported entity type: %s", entityType)
		return nil
	}
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
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
