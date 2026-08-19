package tenant

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/twinmodel"
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

	// POST /api/alarmsQuery/find carries its scope in the BODY: the entity
	// whose alarms are wanted, plus a pageLink. This used to be ignored
	// entirely — the handler answered the full, unfiltered tenant alarm
	// page, so a widget asking for one device's alarms silently received
	// every other device's too (docs/UI_CONTRACT_DATA_FIDELITY.md P3).
	// GET /api/alarms shares this handler and sends no body, so parsing is
	// POST-only and every failure falls back to the previous behaviour.
	var scopeEntityId string
	if r.Method == http.MethodPost && r.Body != nil {
		var body map[string]interface{}
		if json.NewDecoder(r.Body).Decode(&body) == nil {
			scopeEntityId = alarmQueryEntityId(body)
			if pl, ok := body["pageLink"].(map[string]interface{}); ok {
				if ps, ok := pl["pageSize"].(float64); ok && int(ps) > 0 {
					pageSize = httputil.ClampPageSize(int(ps), 10)
				}
				if pg, ok := pl["page"].(float64); ok && int(pg) >= 0 {
					page = int(pg)
				}
			}
		}
	}

	// Scoping reuses the entity_alarm join handleAlarmsByDevice already
	// relies on: it is the table that records which entities an alarm was
	// propagated to, so it answers "this entity's alarms" for originators
	// and propagation targets alike.
	countQuery := "SELECT count(*) FROM alarm a WHERE a.tenant_id = $1"
	scopeJoin := ""
	scopeWhere := ""
	args := []interface{}{tenantId}
	if scopeEntityId != "" {
		countQuery = `SELECT count(*) FROM alarm a
			JOIN entity_alarm ea ON ea.alarm_id = a.id
			WHERE a.tenant_id = $1 AND ea.entity_id = $2`
		scopeJoin = " JOIN entity_alarm ea ON ea.alarm_id = a.id"
		scopeWhere = " AND ea.entity_id = $2"
		args = append(args, scopeEntityId)
	}

	var totalElements int
	dbpkg.Pool.QueryRow(countQuery, args...).Scan(&totalElements)

	offset := page * pageSize
	listArgs := append(append([]interface{}{}, args...), pageSize, offset)
	limitIdx := strconv.Itoa(len(args) + 1)
	offsetIdx := strconv.Itoa(len(args) + 2)
	rows, err := dbpkg.Pool.Query(`
		SELECT a.id, a.created_time, a.type, a.severity, a.originator_id, a.originator_type,
		       a.acknowledged, a.cleared, a.start_ts, a.end_ts, a.ack_ts, a.clear_ts, a.assign_ts,
		       a.customer_id, a.assignee_id, a.additional_info,
		       COALESCE(a.propagate, false), COALESCE(a.propagate_to_owner, false),
		       COALESCE(a.propagate_to_tenant, false), a.propagate_relation_types,
		       COALESCE(d.name, a_e.name, c.title, '') AS originator_name,
		       COALESCE(d.label, a_e.label, '') AS originator_label
		FROM alarm a`+scopeJoin+`
		LEFT JOIN device d ON a.originator_id = d.id AND a.originator_type = 5
		LEFT JOIN asset a_e ON a.originator_id = a_e.id AND a.originator_type = 4
		LEFT JOIN customer c ON a.originator_id = c.id AND a.originator_type = 1
		WHERE a.tenant_id = $1`+scopeWhere+`
		ORDER BY a.created_time DESC
		LIMIT $`+limitIdx+` OFFSET $`+offsetIdx, listArgs...)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	// Assignee ids seen while scanning; resolved in one pass once `rows` is
	// closed (see the note at the assignee assignment below).
	assigneeIds := map[string]map[string]interface{}{}
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
		// `assignee` used to be an unconditional nil even when assignee_id
		// was set — api.go documents it as v2's extra field over v1, so it
		// went unmet exactly when it mattered. It is filled in AFTER this
		// loop, not here: querying per row while `rows` is still open holds
		// a pooled connection and waits for another, which deadlocks
		// outright on a single-connection pool and is an N+1 on any pool.
		if assigneeIdStr != nil && *assigneeIdStr != "" {
			item["assigneeId"] = map[string]interface{}{"entityType": "USER", "id": *assigneeIdStr}
			assigneeIds[*assigneeIdStr] = nil
		} else {
			item["assigneeId"] = nil
			item["assignee"] = nil
		}

		if additionalInfo != nil && *additionalInfo != "" {
			var info interface{}
			json.Unmarshal([]byte(*additionalInfo), &info)
			item["details"] = info
		} else {
			item["details"] = nil
		}

		data = append(data, item)
	}
	rows.Close()

	// Resolve each distinct assignee once, with the result rows closed so
	// no pooled connection is held while we query. Page size bounds this to
	// at most pageSize lookups, and repeats collapse to one.
	for userId := range assigneeIds {
		assigneeIds[userId] = resolveAssignee(tenantId, userId)
	}
	for _, item := range data {
		if ref, ok := item["assigneeId"].(map[string]interface{}); ok {
			if userId, ok := ref["id"].(string); ok {
				item["assignee"] = assigneeIds[userId]
			}
		}
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

// alarmQueryEntityId digs the target entity id out of an alarmsQuery body.
// TB has shipped several shapes for it over the years and the UI still sends
// a mix, so all of them are accepted and anything unrecognised yields "" —
// which the caller treats as "no scope", i.e. the previous whole-tenant
// behaviour, rather than an error.
func alarmQueryEntityId(body map[string]interface{}) string {
	// {"entityFilter": {"singleEntity": {"id": "...", "entityType": "..."}}}
	if ef, ok := body["entityFilter"].(map[string]interface{}); ok {
		if id := entityIdFromRef(ef["singleEntity"]); id != "" {
			return id
		}
		// {"entityFilter": {"entityList": ["id", ...]}} — scope to the first;
		// the join below takes a single entity.
		if list, ok := ef["entityList"].([]interface{}); ok && len(list) > 0 {
			if id := entityIdFromRef(list[0]); id != "" {
				return id
			}
		}
	}
	// Legacy flat shapes: {"entityId": {...}} or {"entityId": "..."}.
	return entityIdFromRef(body["entityId"])
}

// entityIdFromRef accepts a bare id string, {"id": "..."} or a nested
// {"id": {"id": "..."}} and returns the id only when it is a real UUID —
// so a malformed reference degrades to "no scope" instead of reaching
// postgres and erroring on an invalid uuid cast.
func entityIdFromRef(value interface{}) string {
	switch v := value.(type) {
	case string:
		if httputil.LooksLikeUUID(v) {
			return v
		}
	case map[string]interface{}:
		switch id := v["id"].(type) {
		case string:
			if httputil.LooksLikeUUID(id) {
				return id
			}
		case map[string]interface{}:
			if nested, ok := id["id"].(string); ok && httputil.LooksLikeUUID(nested) {
				return nested
			}
		}
	}
	return ""
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

// handleAlarmById serves GET /api/alarm/{id}.
//
// This used to select a much narrower column set than the sibling list query
// (HandleAlarmsQueryFind) and then fill the difference with constants: the
// originator's entityType was hardcoded "DEVICE" whatever the real
// originator_type ordinal said, and a cleared-but-unacknowledged alarm was
// mislabelled CLEARED_ACK because the status derivation collapsed two of the
// four states. customerId/assigneeId/propagate*/ackTs/clearTs/assignTs and
// the originator's name were simply absent. It now reads the same columns
// and derives the same fields as the list, so opening one alarm and seeing
// it in a list cannot disagree (docs/UI_CONTRACT_DATA_FIDELITY.md P3).
func handleAlarmById(w http.ResponseWriter, tenantId, alarmId string) {
	var id, alarmType, severity, originatorId string
	var createdTime, startTs, endTs, ackTs, clearTs, assignTs int64
	var originatorTypeOrd int
	var acknowledged, cleared, propagate, propagateToOwner, propagateToTenant bool
	var customerIdStr, assigneeIdStr, additionalInfo, propagateRelTypes *string
	var originatorName, originatorLabel string

	err := dbpkg.Pool.QueryRow(`
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
		WHERE a.id = $1 AND a.tenant_id = $2`, alarmId, tenantId).Scan(
		&id, &createdTime, &alarmType, &severity, &originatorId, &originatorTypeOrd,
		&acknowledged, &cleared, &startTs, &endTs, &ackTs, &clearTs, &assignTs,
		&customerIdStr, &assigneeIdStr, &additionalInfo,
		&propagate, &propagateToOwner, &propagateToTenant, &propagateRelTypes,
		&originatorName, &originatorLabel)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Alarm not found")
		return
	}

	result := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "ALARM", "id": id},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"originator": map[string]interface{}{
			"entityType": originatorTypeOrdinalToString(originatorTypeOrd),
			"id":         originatorId,
		},
		"name":                  alarmType,
		"type":                  alarmType,
		"severity":              severity,
		"status":                alarmStatus(cleared, acknowledged),
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
	if propagateRelTypes != nil && *propagateRelTypes != "" && *propagateRelTypes != "{}" {
		result["propagateRelationTypes"] = dbutil.ParsePgTextArray([]byte(*propagateRelTypes))
	} else {
		result["propagateRelationTypes"] = []string{}
	}
	if customerIdStr != nil && *customerIdStr != "" {
		result["customerId"] = map[string]interface{}{"entityType": "CUSTOMER", "id": *customerIdStr}
	} else {
		result["customerId"] = nil
	}
	if assigneeIdStr != nil && *assigneeIdStr != "" {
		result["assigneeId"] = map[string]interface{}{"entityType": "USER", "id": *assigneeIdStr}
		result["assignee"] = resolveAssignee(tenantId, *assigneeIdStr)
	} else {
		result["assigneeId"] = nil
		result["assignee"] = nil
	}

	if additionalInfo != nil && *additionalInfo != "" {
		var info interface{}
		json.Unmarshal([]byte(*additionalInfo), &info)
		result["details"] = info
	} else {
		result["details"] = nil
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// alarmStatus derives TB's four-state alarm status. Kept in one place
// because a by-id read and a list read disagreeing on it is exactly the
// defect this replaced.
func alarmStatus(cleared, acknowledged bool) string {
	switch {
	case cleared && acknowledged:
		return "CLEARED_ACK"
	case cleared:
		return "CLEARED_UNACK"
	case acknowledged:
		return "ACTIVE_ACK"
	default:
		return "ACTIVE_UNACK"
	}
}

// resolveAssignee returns TB's inline assignee shape for an alarm's
// assignee_id. Deliberately a narrow tenant-scoped query rather than
// user.FindByID, which joins user_credentials — an assignee without a
// credentials row would resolve to nothing there, reintroducing the very
// always-nil assignee this fixes. Returns nil when the id doesn't resolve
// inside the caller's tenant.
func resolveAssignee(tenantId, userId string) map[string]interface{} {
	var email string
	var firstName, lastName *string
	if dbpkg.Pool.QueryRow(
		"SELECT email, first_name, last_name FROM tb_user WHERE id = $1 AND tenant_id = $2",
		userId, tenantId).Scan(&email, &firstName, &lastName) != nil {
		return nil
	}
	out := map[string]interface{}{
		"id":    map[string]interface{}{"entityType": "USER", "id": userId},
		"email": email,
	}
	if firstName != nil {
		out["firstName"] = *firstName
	}
	if lastName != nil {
		out["lastName"] = *lastName
	}
	return out
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
// caller's tenant. Missing entities return false; database failures stay
// distinguishable so write handlers can return 500 instead of misreporting an
// infrastructure outage as an authorization decision.
func entityOwnedByTenant(table, entityId, tenantID string) (bool, error) {
	if dbpkg.Pool == nil || tenantID == "" {
		return false, errors.New("attribute database unavailable")
	}
	var owned bool
	if err := dbpkg.Pool.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM "+table+" WHERE id = $1 AND tenant_id = $2)",
		entityId, tenantID).Scan(&owned); err != nil {
		return false, err
	}
	return owned, nil
}

// attributeEntityTenant resolves the persistence tenant from the entity row.
// The table is selected only through attributeEntityTable, so the interpolated
// identifier is never request-controlled. SYS_ADMIN writes use this instead of
// their empty/system JWT tenant before model lookup and KV persistence.
func attributeEntityTenant(table, entityID string) (string, error) {
	if dbpkg.Pool == nil {
		return "", errors.New("attribute database unavailable")
	}
	var tenantID string
	if err := dbpkg.Pool.QueryRow(
		"SELECT tenant_id::text FROM "+table+" WHERE id=$1", entityID,
	).Scan(&tenantID); err != nil {
		return "", err
	}
	return tenantID, nil
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

	entityType := strings.ToUpper(strings.TrimSpace(parts[4]))
	entityId := parts[5]
	isAttributeWrite := r.Method == "POST" && len(parts) >= 7
	if isAttributeWrite && entityType != "DEVICE" && entityType != "ASSET" {
		httputil.WriteError(w, http.StatusBadRequest, "Unknown entity category")
		return
	}

	// Tenant gate for the WHOLE attribute surface. attribute_kv has no
	// tenant_id column and both reads and writes used to go by the raw path
	// UUID, so any authenticated user could read or overwrite any tenant's
	// attributes (IDOR). Ownership is proven against the entity's own table,
	// with the tenant taken from the VERIFIED JWT — never from the request.
	// SYS_ADMIN legitimately crosses tenants (platform admin console).
	scope := attrCallerScope{SysAdmin: callerIsSysAdmin(claims)}
	scope.TenantID, _ = claims["tenantId"].(string)
	table, knownEntityType := attributeEntityTable(entityType)
	if !scope.SysAdmin {
		if !knownEntityType {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
			return
		}
		owned, err := entityOwnedByTenant(table, entityId, scope.TenantID)
		if err != nil {
			log.Printf("ERROR: Failed to verify attribute entity tenant entity_type=%s entity_id=%s: %v", entityType, entityId, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Attribute entity tenant verification failed")
			return
		}
		if !owned {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
			return
		}
		scope.Table = table
	} else if knownEntityType {
		scope.Table = table
	}
	if isAttributeWrite && scope.SysAdmin {
		actualTenant, err := attributeEntityTenant(table, entityId)
		if err != nil {
			log.Printf("ERROR: Failed to resolve attribute entity tenant entity_type=%s entity_id=%s: %v", entityType, entityId, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Attribute entity tenant resolution failed")
			return
		}
		scope.TenantID = actualTenant
	}

	// POST /api/plugins/telemetry/{entityType}/{entityId}/{scope} — save attributes
	if isAttributeWrite {
		handleSaveAttributeRest(w, r, scope, entityType, entityId, parts[6])
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

func handleSaveAttributeRest(w http.ResponseWriter, r *http.Request, caller attrCallerScope, entityType, entityId, scope string) {
	var data map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&data); err != nil || data == nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	// A write MUST name a valid scope: the old fallback silently rewrote
	// typos (e.g. "SHAERD_SCOPE") into SERVER_SCOPE, persisting the data in
	// a scope the caller never asked for. The normalized value feeds BOTH
	// attribute_kv and the KV merge so the two stores always agree.
	normalizedScope, _, ok := normalizeAttributeScope(scope)
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, "Unknown attribute scope")
		return
	}
	if err := twinmodel.ValidateAttributes(r.Context(), dbpkg.Pool, caller.TenantID, entityType, entityId, data); err != nil {
		if errors.Is(err, twinmodel.ErrAttributesRejected) {
			httputil.WriteError(w, http.StatusBadRequest, "Attributes violate pinned twin model")
			return
		}
		log.Printf("ERROR: Twin model attribute enforcement failed tenant_id=%s entity_type=%s entity_id=%s: %v",
			caller.TenantID, entityType, entityId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model attribute enforcement failed")
		return
	}

	if err := SaveAttributesKV(r.Context(), caller.TenantID, entityType, entityId, normalizedScope, data); err != nil {
		log.Printf("ERROR: Failed to save attributes for entity %s: %v", entityId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save attributes")
		return
	}

	// No direct WS broadcast here: the MergeAttributes above (inside
	// SaveAttributesKV) makes the twin KV watch (twin_state.go) observe this
	// write, and the watch is the SINGLE publisher of attribute pushes.
	// Broadcasting from the write path too made every REST attribute save two
	// frames per subscriber (milestone 3 phase 1 fix — the watch also diffs,
	// so it only pushes what actually changed).
	w.WriteHeader(http.StatusOK)
}

func saveAttributesKV(entityId string, attrType int, values map[string]interface{}) error {
	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for key, value := range values {
		keyId, err := getOrInsertKeyID(tx, key)
		if err != nil {
			return err
		}
		var boolV *bool
		var strV *string
		var longV *int64
		var dblV *float64
		var jsonV *string
		switch value := value.(type) {
		case bool:
			boolV = &value
		case float64:
			dblV = &value
		case string:
			strV = &value
		default:
			encoded, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("marshal attribute %q: %w", key, err)
			}
			encodedString := string(encoded)
			jsonV = &encodedString
		}
		if _, err := tx.Exec(`INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, bool_v, str_v, long_v, dbl_v, json_v, last_update_ts)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET
			bool_v = EXCLUDED.bool_v, str_v = EXCLUDED.str_v, long_v = EXCLUDED.long_v,
			dbl_v = EXCLUDED.dbl_v, json_v = EXCLUDED.json_v, last_update_ts = EXCLUDED.last_update_ts`,
			entityId, attrType, keyId, boolV, strV, longV, dblV, jsonV, currentTimeMillis()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func getOrInsertKeyID(tx *sql.Tx, key string) (int, error) {
	var keyID int
	err := tx.QueryRow(`INSERT INTO key_dictionary (key) VALUES ($1)
		ON CONFLICT (key) DO NOTHING RETURNING key_id`, key).Scan(&keyID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRow(`SELECT key_id FROM key_dictionary WHERE key=$1`, key).Scan(&keyID)
	}
	if err != nil {
		return 0, fmt.Errorf("resolve key dictionary ID for %q: %w", key, err)
	}
	return keyID, nil
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
