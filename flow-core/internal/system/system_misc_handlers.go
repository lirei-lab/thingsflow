package system

import (
	"encoding/json"
	"log"
	"math"
	"net/http"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// HandleAuditLogs serves GET /api/audit/logs?... (also /audit/logs/customer, /user, /entity).
// Returns a TB-style PageData of audit_log rows for the current tenant.
func HandleAuditLogs(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM audit_log WHERE tenant_id = $1", tenantId).Scan(&total)

	offset := page * pageSize
	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, tenant_id, customer_id, entity_id, entity_type, entity_name,
		       user_id, user_name, action_type, action_data, action_status, action_failure_details
		FROM audit_log
		WHERE tenant_id = $1
		ORDER BY created_time DESC
		LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		// audit_log can fail for missing column on some schemas — return empty list
		log.Printf("WARN audit_log query: %v", err)
		httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
			"data": []interface{}{}, "totalPages": 0, "totalElements": 0, "hasNext": false,
		})
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, tid string
		var createdTime int64
		var customerId, entityId, entityType, entityName *string
		var userId, userName, actionType, actionData, actionStatus, actionFailureDetails *string
		if err := rows.Scan(&id, &createdTime, &tid, &customerId, &entityId, &entityType, &entityName,
			&userId, &userName, &actionType, &actionData, &actionStatus, &actionFailureDetails); err != nil {
			continue
		}
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "AUDIT_LOG", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
		}
		httputil.SetOptional(item, "userName", userName)
		httputil.SetOptional(item, "entityName", entityName)
		httputil.SetOptional(item, "actionType", actionType)
		httputil.SetOptional(item, "actionStatus", actionStatus)
		httputil.SetOptional(item, "actionFailureDetails", actionFailureDetails)
		if userId != nil && *userId != "" {
			item["userId"] = map[string]interface{}{"entityType": "USER", "id": *userId}
		}
		if customerId != nil && *customerId != "" {
			item["customerId"] = map[string]interface{}{"entityType": "CUSTOMER", "id": *customerId}
		}
		if entityId != nil && entityType != nil && *entityId != "" {
			item["entityId"] = map[string]interface{}{"entityType": *entityType, "id": *entityId}
		}
		if actionData != nil && *actionData != "" {
			var ad interface{}
			if err := json.Unmarshal([]byte(*actionData), &ad); err == nil {
				item["actionData"] = ad
			}
		}
		data = append(data, item)
	}

	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	if totalPages == 0 && total > 0 {
		totalPages = 1
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

// HandleOAuth2ClientInfos GET /api/oauth2/client/infos — list of available OAuth2 providers.
// We don't run OAuth in this PoC so return empty.
func HandleOAuth2ClientInfos(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, []interface{}{})
}

// HandleOAuth2ConfigTemplate GET /api/oauth2/config/template — provider templates (Google, GitHub...).
func HandleOAuth2ConfigTemplate(w http.ResponseWriter, r *http.Request) {
	// No token check: TB-Java exposes this without auth so the login screen can show provider buttons.
	httputil.WriteJSON(w, http.StatusOK, []interface{}{})
}

// HandleTenantDashboardHomeInfo GET /api/tenant/dashboard/home/info — minimal info for
// the tenant's configured home dashboard. TB shape: {dashboardId, hideDashboardToolbar}.
func HandleTenantDashboardHomeInfo(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"dashboardId":          nil,
		"hideDashboardToolbar": true,
	})
}

// HandleMobileBundleInfos GET /api/mobile/bundle/infos — used by the Mobile Center.
func HandleMobileBundleInfos(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pageSize := httputil.IntParam(r, "pageSize", 10)
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          []interface{}{},
		"totalPages":    0,
		"totalElements": 0,
		"hasNext":       false,
		"pageSize":      pageSize,
	})
}

// HandleNotificationsCenter GET /api/notifications — notification inbox listing.
func HandleNotificationsCenter(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          []interface{}{},
		"totalPages":    0,
		"totalElements": 0,
		"hasNext":       false,
	})
}
