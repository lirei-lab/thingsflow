package tenant

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/twinstore"
	"flow-core/internal/user"
)

// HandleTenantById processes GET /api/tenant/{id}
func HandleTenantById(w http.ResponseWriter, r *http.Request, tenantId string) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	var id, title string
	var createdTime int64
	var additionalInfo, country, state, city, address, address2, zip, phone, email *string
	var version *int64

	err = dbpkg.Pool.QueryRow(`
		SELECT id, created_time, title, additional_info, country, state, city, 
		       address, address2, zip, phone, email, version
		FROM tenant WHERE id = $1`, tenantId).Scan(
		&id, &createdTime, &title, &additionalInfo, &country, &state, &city,
		&address, &address2, &zip, &phone, &email, &version)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Tenant not found")
		return
	}

	result := map[string]interface{}{
		"id": map[string]interface{}{
			"entityType": "TENANT",
			"id":         id,
		},
		"createdTime": createdTime,
		"title":       title,
		"name":        title,
	}

	httputil.SetOptional(result, "country", country)
	httputil.SetOptional(result, "state", state)
	httputil.SetOptional(result, "city", city)
	httputil.SetOptional(result, "address", address)
	httputil.SetOptional(result, "address2", address2)
	httputil.SetOptional(result, "zip", zip)
	httputil.SetOptional(result, "phone", phone)
	httputil.SetOptional(result, "email", email)

	if version != nil {
		result["version"] = *version
	} else {
		result["version"] = 1
	}
	if additionalInfo != nil && *additionalInfo != "" {
		var info interface{}
		json.Unmarshal([]byte(*additionalInfo), &info)
		result["additionalInfo"] = info
	} else {
		result["additionalInfo"] = nil
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// callerIsSysAdmin reports whether the JWT carries the SYS_ADMIN scope. A
// SYS_ADMIN legitimately reads across tenants (the platform admin console);
// every other authority is confined to its own tenant. Mirrors the scope
// check in httputil.RequireSysAdmin.
func callerIsSysAdmin(claims map[string]interface{}) bool {
	scopes, _ := claims["scopes"].([]interface{})
	for _, s := range scopes {
		if str, ok := s.(string); ok && str == "SYS_ADMIN" {
			return true
		}
	}
	return false
}

// HandleUserById processes GET /api/user/{id}
func HandleUserById(w http.ResponseWriter, r *http.Request, userId string) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	u, err := user.FindByID(userId)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "User not found")
		return
	}

	// Tenant ownership: a SYS_ADMIN may read any user; everyone else is confined
	// to their own tenant. Without this, tenant A could read tenant B's users
	// (email/authority/PII) by UUID.
	if !callerIsSysAdmin(claims) {
		callerTenant, _ := claims["tenantId"].(string)
		if callerTenant == "" || u.TenantID != callerTenant {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user.BuildResponse(u))
}

// HandleCustomerById processes GET /api/customer/{id}
func HandleCustomerById(w http.ResponseWriter, r *http.Request, customerId string) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	var id, title string
	var createdTime int64
	var tenantIdStr, additionalInfo, country, state, city, address, address2, zip, phone, email *string
	var version *int64

	err = dbpkg.Pool.QueryRow(`
		SELECT id, created_time, title, tenant_id, additional_info, country, state, city,
		       address, address2, zip, phone, email, version
		FROM customer WHERE id = $1`, customerId).Scan(
		&id, &createdTime, &title, &tenantIdStr, &additionalInfo, &country, &state, &city,
		&address, &address2, &zip, &phone, &email, &version)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Customer not found")
		return
	}

	tid := "13814000-1dd2-11b2-8080-808080808080"
	if tenantIdStr != nil {
		tid = *tenantIdStr
	}

	// Tenant ownership: SYS_ADMIN reads any customer; otherwise the customer
	// must belong to the caller's tenant. Closes the cross-tenant IDOR.
	if !callerIsSysAdmin(claims) {
		callerTenant, _ := claims["tenantId"].(string)
		if callerTenant == "" || tid != callerTenant {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
			return
		}
	}

	result := map[string]interface{}{
		"id": map[string]interface{}{
			"entityType": "CUSTOMER",
			"id":         id,
		},
		"createdTime": createdTime,
		"tenantId": map[string]interface{}{
			"entityType": "TENANT",
			"id":         tid,
		},
		"title": title,
		"name":  title,
	}

	httputil.SetOptional(result, "country", country)
	httputil.SetOptional(result, "state", state)
	httputil.SetOptional(result, "city", city)
	httputil.SetOptional(result, "address", address)
	httputil.SetOptional(result, "address2", address2)
	httputil.SetOptional(result, "zip", zip)
	httputil.SetOptional(result, "phone", phone)
	httputil.SetOptional(result, "email", email)

	if version != nil {
		result["version"] = *version
	} else {
		result["version"] = 1
	}
	if additionalInfo != nil && *additionalInfo != "" {
		var info interface{}
		json.Unmarshal([]byte(*additionalInfo), &info)
		result["additionalInfo"] = info
	} else {
		result["additionalInfo"] = nil
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// HandleAlarmRest processes alarm REST API endpoints
func HandleAlarmRest(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	path := r.URL.Path

	// GET /api/alarms (or /api/v2/alarms) — list all tenant alarms (paginated)
	if (path == "/api/alarms" || path == "/api/v2/alarms") && r.Method == "GET" {
		HandleAlarmsQueryFind(w, r)
		return
	}

	// GET /api/alarm/DEVICE/{deviceId} — list alarms for a device.
	// Also matches /api/v2/alarm/DEVICE/{deviceId} (modern UI variant).
	if strings.Contains(path, "/alarm/DEVICE/") || strings.Contains(path, "/alarm/device/") {
		parts := strings.Split(path, "/")
		if len(parts) >= 5 {
			deviceId := parts[len(parts)-1]
			handleAlarmsByDevice(w, r, tenantId, deviceId)
			return
		}
	}

	// POST /api/alarm/{alarmId}/ack
	if strings.HasSuffix(path, "/ack") && r.Method == "POST" {
		parts := strings.Split(strings.TrimSuffix(path, "/ack"), "/")
		alarmId := parts[len(parts)-1]
		handleAlarmAck(w, alarmId)
		return
	}

	// POST /api/alarm/{alarmId}/clear
	if strings.HasSuffix(path, "/clear") && r.Method == "POST" {
		parts := strings.Split(strings.TrimSuffix(path, "/clear"), "/")
		alarmId := parts[len(parts)-1]
		handleAlarmClear(w, alarmId)
		return
	}

	// GET /api/alarm/{alarmId} — single alarm
	// Reject non-UUID segments up front so we don't hit postgres with
	// "invalid input syntax for type uuid" for paths like /api/alarm/rules
	// that the UI sometimes probes.
	if r.Method == "GET" {
		parts := strings.Split(path, "/")
		alarmId := parts[len(parts)-1]
		if !httputil.LooksLikeUUID(alarmId) {
			httputil.WriteError(w, http.StatusNotFound, "Alarm endpoint not found")
			return
		}
		handleAlarmById(w, tenantId, alarmId)
		return
	}

	httputil.WriteError(w, http.StatusNotFound, "Alarm endpoint not found")
}

// HandleAlarmsQueryFind processes POST /api/alarmsQuery/find
func HandleAlarmsQueryFind(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	pageSize := httputil.PageSize(r, 10)
	page := httputil.IntParam(r, "page", 0)

	var totalElements int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM alarm WHERE tenant_id = $1", tenantId).Scan(&totalElements)

	offset := page * pageSize
	rows, err := dbpkg.Pool.Query(`
		SELECT a.id, a.created_time, a.type, a.severity, a.originator_id, a.originator_type,
		       a.acknowledged, a.cleared, a.start_ts, a.end_ts, a.ack_ts, a.clear_ts, a.assign_ts,
		       a.customer_id, a.assignee_id, a.additional_info,
		       COALESCE(a.propagate, false), COALESCE(a.propagate_to_owner, false),
		       COALESCE(a.propagate_to_tenant, false), a.propagate_relation_types,
		       COALESCE(d.name, a_e.name, c.title, '') AS originator_name,
		       COALESCE(d.label, a_e.label, '') AS originator_label
		FROM alarm a
		LEFT JOIN device d ON a.originator_id = d.id AND a.originator_type = 5
		LEFT JOIN asset a_e ON a.originator_id = a_e.id AND a.originator_type = 4
		LEFT JOIN customer c ON a.originator_id = c.id AND a.originator_type = 1
		WHERE a.tenant_id = $1
		ORDER BY a.created_time DESC
		LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, alarmType, severity, originatorId string
		var createdTime, startTs, endTs, ackTs, clearTs, assignTs int64
		var originatorTypeOrd int
		var acknowledged, cleared, propagate, propagateToOwner, propagateToTenant bool
		var customerIdStr, assigneeIdStr, additionalInfo, propagateRelTypes *string
		var originatorName, originatorLabel string

		rows.Scan(&id, &createdTime, &alarmType, &severity, &originatorId, &originatorTypeOrd,
			&acknowledged, &cleared, &startTs, &endTs, &ackTs, &clearTs, &assignTs,
			&customerIdStr, &assigneeIdStr, &additionalInfo,
			&propagate, &propagateToOwner, &propagateToTenant, &propagateRelTypes,
			&originatorName, &originatorLabel)

		status := "ACTIVE_UNACK"
		if cleared && acknowledged {
			status = "CLEARED_ACK"
		} else if cleared {
			status = "CLEARED_UNACK"
		} else if acknowledged {
			status = "ACTIVE_ACK"
		}
		originatorType := originatorTypeOrdinalToString(originatorTypeOrd)

		item := map[string]interface{}{
			"id":                    map[string]interface{}{"entityType": "ALARM", "id": id},
			"createdTime":           createdTime,
			"tenantId":              map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"originator":            map[string]interface{}{"entityType": originatorType, "id": originatorId},
			"name":                  alarmType,
			"type":                  alarmType,
			"severity":              severity,
			"status":                status,
			"acknowledged":          acknowledged,
			"cleared":               cleared,
			"startTs":               startTs,
			"endTs":                 endTs,
			"ackTs":                 ackTs,
			"clearTs":               clearTs,
			"assignTs":              assignTs,
			"originatorDisplayName": originatorName,
			"originatorName":        originatorName,
			"originatorLabel":       originatorLabel,
			"propagate":             propagate,
			"propagateToOwner":      propagateToOwner,
			"propagateToTenant":     propagateToTenant,
		}
		// propagateRelationTypes is text[]; expose as []string
		if propagateRelTypes != nil && *propagateRelTypes != "" && *propagateRelTypes != "{}" {
			item["propagateRelationTypes"] = dbutil.ParsePgTextArray([]byte(*propagateRelTypes))
		} else {
			item["propagateRelationTypes"] = []string{}
		}
		if customerIdStr != nil && *customerIdStr != "" {
			item["customerId"] = map[string]interface{}{"entityType": "CUSTOMER", "id": *customerIdStr}
		} else {
			item["customerId"] = nil
		}
		if assigneeIdStr != nil && *assigneeIdStr != "" {
			item["assigneeId"] = map[string]interface{}{"entityType": "USER", "id": *assigneeIdStr}
		} else {
			item["assigneeId"] = nil
		}
		item["assignee"] = nil

		if additionalInfo != nil && *additionalInfo != "" {
			var info interface{}
			json.Unmarshal([]byte(*additionalInfo), &info)
			item["details"] = info
		} else {
			item["details"] = nil
		}

		data = append(data, item)
	}

	totalPages := int(math.Ceil(float64(totalElements) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": totalElements,
		"hasNext":       (page + 1) < totalPages,
	})
}

// ─── Internal alarm handlers ────────────────────────────────────────────────

func handleAlarmsByDevice(w http.ResponseWriter, r *http.Request, tenantId, deviceId string) {
	pageSize := httputil.PageSize(r, 10)
	page := httputil.IntParam(r, "page", 0)

	var totalElements int
	dbpkg.Pool.QueryRow(`SELECT count(*) FROM alarm a JOIN entity_alarm ea ON ea.alarm_id = a.id 
		WHERE ea.entity_id = $1 AND a.tenant_id = $2`, deviceId, tenantId).Scan(&totalElements)

	offset := page * pageSize
	rows, err := dbpkg.Pool.Query(`
		SELECT a.id, a.created_time, a.type, a.severity, a.originator_id,
		       a.acknowledged, a.cleared, a.start_ts, a.end_ts, a.additional_info
		FROM alarm a
		JOIN entity_alarm ea ON ea.alarm_id = a.id
		WHERE ea.entity_id = $1 AND a.tenant_id = $2
		ORDER BY a.created_time DESC 
		LIMIT $3 OFFSET $4`, deviceId, tenantId, pageSize, offset)
	if err != nil {
		log.Printf("ERROR querying alarms by device: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, alarmType, severity, originatorId string
		var createdTime, startTs, endTs int64
		var acknowledged, cleared bool
		var additionalInfo *string

		rows.Scan(&id, &createdTime, &alarmType, &severity, &originatorId,
			&acknowledged, &cleared, &startTs, &endTs, &additionalInfo)

		status := "ACTIVE_UNACK"
		if cleared {
			status = "CLEARED_ACK"
		} else if acknowledged {
			status = "ACTIVE_ACK"
		}

		item := map[string]interface{}{
			"id": map[string]interface{}{
				"entityType": "ALARM",
				"id":         id,
			},
			"createdTime": createdTime,
			"originator": map[string]interface{}{
				"entityType": "DEVICE",
				"id":         originatorId,
			},
			"type":         alarmType,
			"severity":     severity,
			"status":       status,
			"acknowledged": acknowledged,
			"cleared":      cleared,
			"startTs":      startTs,
			"endTs":        endTs,
		}

		if additionalInfo != nil && *additionalInfo != "" {
			var info interface{}
			json.Unmarshal([]byte(*additionalInfo), &info)
			item["details"] = info
		}

		data = append(data, item)
	}

	totalPages := int(math.Ceil(float64(totalElements) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": totalElements,
		"hasNext":       (page + 1) < totalPages,
	})
}

func handleAlarmById(w http.ResponseWriter, tenantId, alarmId string) {
	var id, alarmType, severity, originatorId string
	var createdTime, startTs, endTs int64
	var acknowledged, cleared bool
	var additionalInfo *string

	err := dbpkg.Pool.QueryRow(`
		SELECT id, created_time, type, severity, originator_id,
		       acknowledged, cleared, start_ts, end_ts, additional_info
		FROM alarm WHERE id = $1 AND tenant_id = $2`, alarmId, tenantId).Scan(
		&id, &createdTime, &alarmType, &severity, &originatorId,
		&acknowledged, &cleared, &startTs, &endTs, &additionalInfo)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Alarm not found")
		return
	}

	status := "ACTIVE_UNACK"
	if cleared {
		status = "CLEARED_ACK"
	} else if acknowledged {
		status = "ACTIVE_ACK"
	}

	result := map[string]interface{}{
		"id": map[string]interface{}{
			"entityType": "ALARM",
			"id":         id,
		},
		"createdTime": createdTime,
		"tenantId": map[string]interface{}{
			"entityType": "TENANT",
			"id":         tenantId,
		},
		"originator": map[string]interface{}{
			"entityType": "DEVICE",
			"id":         originatorId,
		},
		"type":         alarmType,
		"severity":     severity,
		"status":       status,
		"acknowledged": acknowledged,
		"cleared":      cleared,
		"startTs":      startTs,
		"endTs":        endTs,
	}

	if additionalInfo != nil && *additionalInfo != "" {
		var info interface{}
		json.Unmarshal([]byte(*additionalInfo), &info)
		result["details"] = info
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleAlarmAck(w http.ResponseWriter, alarmId string) {
	_, err := dbpkg.Pool.Exec("UPDATE alarm SET acknowledged = true, ack_ts = $1 WHERE id = $2",
		currentTimeMillis(), alarmId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to acknowledge alarm")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func handleAlarmClear(w http.ResponseWriter, alarmId string) {
	_, err := dbpkg.Pool.Exec("UPDATE alarm SET cleared = true, clear_ts = $1 WHERE id = $2",
		currentTimeMillis(), alarmId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to clear alarm")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ─── Attribute REST handler ─────────────────────────────────────────────────

// attributeEntityTable maps the {entityType} path segment to the Postgres table
// that carries its tenant_id — the ownership oracle for the attribute surface.
// Mirrors the tenantOwnedTable set in internal/ws (same threat: attribute_kv has
// NO tenant column, so ownership must be proven against the entity's own table).
// Unknown types fail closed.
func attributeEntityTable(entityType string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(entityType)) {
	case "DEVICE":
		return "device", true
	case "ASSET":
		return "asset", true
	case "ENTITY_VIEW":
		return "entity_view", true
	case "CUSTOMER":
		return "customer", true
	case "DASHBOARD":
		return "dashboard", true
	case "USER":
		return "tb_user", true
	case "EDGE":
		return "edge", true
	default:
		return "", false
	}
}

// attrCallerScope is the verified caller identity the attribute handlers thread
// through: tenant from the JWT (never from the request path/body) plus the
// SYS_ADMIN escape hatch. ownershipPredicate() lets the read queries re-assert
// ownership inside SQL as defense in depth (same shape as internal/ws).
type attrCallerScope struct {
	TenantID string
	SysAdmin bool
	Table    string // entity table resolved from the path's {entityType}
}

// ownershipPredicate returns an EXISTS clause binding attribute rows to the
// caller's tenant, or "" for SYS_ADMIN (may read across tenants). The table
// name comes from the attributeEntityTable constant set, never from user input;
// the tenant id stays a bound parameter.
func (s attrCallerScope) ownershipPredicate(argIdx int) (string, bool) {
	if s.SysAdmin {
		return "", false
	}
	return " AND EXISTS (SELECT 1 FROM " + s.Table + " o WHERE o.id = a.entity_id AND o.tenant_id = $" + strconv.Itoa(argIdx) + ")", true
}

// entityOwnedByTenant proves the entity behind the raw path UUID belongs to the
// caller's tenant. Fail closed on any error (missing entity, bad UUID, db down).
func entityOwnedByTenant(table, entityId, tenantID string) bool {
	if dbpkg.Pool == nil || tenantID == "" {
		return false
	}
	var owned bool
	if err := dbpkg.Pool.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM "+table+" WHERE id = $1 AND tenant_id = $2)",
		entityId, tenantID).Scan(&owned); err != nil {
		return false
	}
	return owned
}

// normalizeAttributeScope validates a scope path segment against the canonical
// TB set and returns (normalized name, attribute_type ordinal, ok). The ordinal
// mapping CLIENT=0/SHARED=1/SERVER=2 is the single source of truth for both the
// attribute_kv column and the twin-KV merge — one normalization, used for both,
// so the two stores can never disagree about which scope a write landed in.
func normalizeAttributeScope(scope string) (string, int, bool) {
	switch strings.ToUpper(strings.TrimSpace(scope)) {
	case "CLIENT_SCOPE":
		return "CLIENT_SCOPE", 0, true
	case "SHARED_SCOPE":
		return "SHARED_SCOPE", 1, true
	case "SERVER_SCOPE":
		return "SERVER_SCOPE", 2, true
	default:
		return "", -1, false
	}
}

// HandleAttributeRest processes /api/plugins/telemetry/{entityType}/{entityId}/attributes/*
func HandleAttributeRest(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	path := r.URL.Path
	parts := strings.Split(path, "/")
	// /api/plugins/telemetry/DEVICE/{id}/keys/attributes → parts = [, api, plugins, telemetry, DEVICE, {id}, keys, attributes]
	// /api/plugins/telemetry/DEVICE/{id}/values/attributes/{scope}

	if len(parts) < 7 {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid path")
		return
	}

	entityType := parts[4]
	entityId := parts[5]

	// Tenant gate for the WHOLE attribute surface. attribute_kv has no
	// tenant_id column and both reads and writes used to go by the raw path
	// UUID, so any authenticated user could read or overwrite any tenant's
	// attributes (IDOR). Ownership is proven against the entity's own table,
	// with the tenant taken from the VERIFIED JWT — never from the request.
	// SYS_ADMIN legitimately crosses tenants (platform admin console).
	scope := attrCallerScope{SysAdmin: callerIsSysAdmin(claims)}
	scope.TenantID, _ = claims["tenantId"].(string)
	if !scope.SysAdmin {
		table, known := attributeEntityTable(entityType)
		if !known || !entityOwnedByTenant(table, entityId, scope.TenantID) {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
			return
		}
		scope.Table = table
	}

	// POST /api/plugins/telemetry/{entityType}/{entityId}/{scope} — save attributes
	if r.Method == "POST" && len(parts) >= 7 {
		handleSaveAttributeRest(w, r, scope, entityId, parts[6])
		return
	}

	// GET .../keys/attributes
	if strings.Contains(path, "/keys/attributes") {
		handleAttributeKeys(w, scope, entityId)
		return
	}

	// GET .../values/attributes/{scope}
	if strings.Contains(path, "/values/attributes") {
		attrScope := ""
		for i, p := range parts {
			if p == "attributes" && i+1 < len(parts) {
				attrScope = parts[i+1]
				break
			}
		}
		keys := r.URL.Query().Get("keys")
		handleAttributeValues(w, scope, entityId, attrScope, keys)
		return
	}

	httputil.WriteError(w, http.StatusNotFound, "Attribute endpoint not found")
}

func handleAttributeKeys(w http.ResponseWriter, caller attrCallerScope, entityId string) {
	query := `
		SELECT DISTINCT k.key FROM attribute_kv a
		JOIN key_dictionary k ON a.attribute_key = k.key_id
		WHERE a.entity_id = $1`
	args := []interface{}{entityId}
	// Defense in depth: the handler already 403'd unowned entities, but the
	// read itself re-asserts ownership so a future routing mistake cannot
	// turn into a cross-tenant read (same pattern as ws.sendInitialAttributes).
	if pred, ok := caller.ownershipPredicate(2); ok {
		query += pred
		args = append(args, caller.TenantID)
	}
	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	keys := []string{}
	for rows.Next() {
		var key string
		rows.Scan(&key)
		keys = append(keys, key)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(keys)
}

func handleAttributeValues(w http.ResponseWriter, caller attrCallerScope, entityId, scope, keysParam string) {
	// An absent scope segment means "all scopes" (TB classic behaviour); a
	// PRESENT but unknown one is a client bug — reject instead of silently
	// widening the read to every scope.
	attrType := -1
	if strings.TrimSpace(scope) != "" {
		var ok bool
		_, attrType, ok = normalizeAttributeScope(scope)
		if !ok {
			httputil.WriteError(w, http.StatusBadRequest, "Unknown attribute scope")
			return
		}
	}

	query := `SELECT k.key, a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v, a.last_update_ts
		FROM attribute_kv a
		JOIN key_dictionary k ON a.attribute_key = k.key_id
		WHERE a.entity_id = $1`
	args := []interface{}{entityId}
	argIdx := 2

	// Defense in depth — see handleAttributeKeys.
	if pred, ok := caller.ownershipPredicate(argIdx); ok {
		query += pred
		args = append(args, caller.TenantID)
		argIdx++
	}

	if attrType >= 0 {
		query += " AND a.attribute_type = $" + strconv.Itoa(argIdx)
		args = append(args, attrType)
		argIdx++
	}

	if keysParam != "" {
		keys := strings.Split(keysParam, ",")
		placeholders := make([]string, len(keys))
		for i, k := range keys {
			placeholders[i] = "$" + strconv.Itoa(argIdx+i)
			args = append(args, strings.TrimSpace(k))
		}
		query += " AND k.key IN (" + strings.Join(placeholders, ",") + ")"
	}

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	result := []map[string]interface{}{}
	for rows.Next() {
		var key string
		var boolV *bool
		var strV *string
		var longV *int64
		var dblV *float64
		var jsonV *string
		var lastTs int64

		rows.Scan(&key, &boolV, &strV, &longV, &dblV, &jsonV, &lastTs)

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
		}

		result = append(result, map[string]interface{}{
			"key":          key,
			"value":        value,
			"lastUpdateTs": lastTs,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleSaveAttributeRest(w http.ResponseWriter, r *http.Request, caller attrCallerScope, entityId, scope string) {
	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	// A write MUST name a valid scope: the old fallback silently rewrote
	// typos (e.g. "SHAERD_SCOPE") into SERVER_SCOPE, persisting the data in
	// a scope the caller never asked for. The normalized value feeds BOTH
	// attribute_kv and the KV merge so the two stores always agree.
	normalizedScope, attrType, ok := normalizeAttributeScope(scope)
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, "Unknown attribute scope")
		return
	}

	for key, value := range data {
		saveAttributeKV(entityId, attrType, key, value)
	}
	// The merge is keyed by the JWT tenant (attrCallerScope.TenantID), never
	// anything from the request path/body — an attacker-controlled tenant here
	// would let cross-tenant KV documents be forged even with the ownership
	// gate above (SYS_ADMIN writes land under the sysadmin's own tenant key).
	if store := twinstore.Global(); store != nil && caller.TenantID != "" {
		if err := store.MergeAttributes(context.Background(), caller.TenantID, "DEVICE", entityId, normalizedScope, currentTimeMillis(), data); err != nil {
			log.Printf("WARN: Failed to save attributes to twin state for entity %s: %v", entityId, err)
		}
	}

	// No direct WS broadcast here: the MergeAttributes above makes the twin
	// KV watch (twin_state.go) observe this write, and the watch is the
	// SINGLE publisher of attribute pushes. Broadcasting from the write path
	// too made every REST attribute save two frames per subscriber
	// (milestone 3 phase 1 fix — the watch also diffs, so it only pushes
	// what actually changed).
	w.WriteHeader(http.StatusOK)
}

func saveAttributeKV(entityId string, attrType int, key string, value interface{}) {
	keyId := dbpkg.GetOrInsertKeyID(key)
	if keyId == -1 {
		return
	}

	ts := currentTimeMillis()
	var boolV *bool
	var strV *string
	var longV *int64
	var dblV *float64
	var jsonV *string

	switch v := value.(type) {
	case bool:
		boolV = &v
	case float64:
		dblV = &v
	case string:
		strV = &v
	default:
		s, _ := json.Marshal(v)
		str := string(s)
		jsonV = &str
	}

	_, err := dbpkg.Pool.Exec(`INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, bool_v, str_v, long_v, dbl_v, json_v, last_update_ts) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
		ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET 
		bool_v = EXCLUDED.bool_v, str_v = EXCLUDED.str_v, long_v = EXCLUDED.long_v, 
		dbl_v = EXCLUDED.dbl_v, json_v = EXCLUDED.json_v, last_update_ts = EXCLUDED.last_update_ts`,
		entityId, attrType, keyId, boolV, strV, longV, dblV, jsonV, ts)
	if err != nil {
		log.Printf("WARN: Failed to save attribute %s: %v", key, err)
	}
}

func currentTimeMillis() int64 {
	return time.Now().UnixMilli()
}

// originatorTypeOrdinalToString maps the ThingsBoard Java EntityType enum
// ordinal stored in alarm.originator_type to its symbolic name. Mirrors the
// constants used elsewhere in the bridge (see alarms.go originatorTypeDevice).
func originatorTypeOrdinalToString(ord int) string {
	switch ord {
	case 0:
		return "TENANT"
	case 1:
		return "CUSTOMER"
	case 2:
		return "USER"
	case 3:
		return "DASHBOARD"
	case 4:
		return "ASSET"
	case 5:
		return "DEVICE"
	case 9:
		return "ENTITY_VIEW"
	default:
		return "DEVICE"
	}
}
