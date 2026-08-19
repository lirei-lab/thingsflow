package system

import (
	"database/sql"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strings"

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
	pageSize := httputil.PageSize(r, 10)
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
func HandleOAuth2ClientInfos(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	// Real oauth2_client rows exist and are fully CRUD-able via
	// GET/POST /api/oauth2/client (listOAuth2Clients, above) — this "infos"
	// variant was returning [] unconditionally instead of the same query.
	out := []map[string]interface{}{}
	rows, err := dbpkg.Pool.Query(
		`SELECT id, created_time, title, login_button_label FROM oauth2_client ORDER BY title`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			var createdTime int64
			var title, label *string
			if rows.Scan(&id, &createdTime, &title, &label) == nil {
				out = append(out, map[string]interface{}{
					"id":               map[string]interface{}{"entityType": "OAUTH2_CLIENT", "id": id},
					"createdTime":      createdTime,
					"title":            valOr(title, ""),
					"loginButtonLabel": valOr(label, ""),
				})
			}
		}
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

// HandleOAuth2ConfigTemplate GET /api/oauth2/config/template — provider templates (Google, GitHub...).
func HandleOAuth2ConfigTemplate(w http.ResponseWriter, r *http.Request) {
	// No token check: TB-Java exposes this without auth so the login screen can show provider buttons.
	// oauth2_client_registration_template is real, seeded at every boot
	// (internal/bootstrap.loadOAuth2Templates) — this handler was returning
	// [] unconditionally instead of reading it.
	out := []map[string]interface{}{}
	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, provider_id, authorization_uri, token_uri, scope,
		       user_info_uri, user_name_attribute_name, jwk_set_uri,
		       client_authentication_method, type,
		       basic_email_attribute_key, basic_first_name_attribute_key,
		       basic_last_name_attribute_key, basic_tenant_name_strategy,
		       comment, login_button_icon, login_button_label, help_link
		  FROM oauth2_client_registration_template ORDER BY provider_id`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id, providerID string
			var createdTime int64
			var authURI, tokenURI, scope, userInfoURI, userNameAttr, jwkSetURI, clientAuthMethod, typ *string
			var emailKey, firstNameKey, lastNameKey, tenantStrategy, comment, icon, label, helpLink *string
			if rows.Scan(&id, &createdTime, &providerID, &authURI, &tokenURI, &scope,
				&userInfoURI, &userNameAttr, &jwkSetURI, &clientAuthMethod, &typ,
				&emailKey, &firstNameKey, &lastNameKey, &tenantStrategy,
				&comment, &icon, &label, &helpLink) == nil {
				scopeList := []string{}
				if s := valOr(scope, ""); s != "" {
					scopeList = strings.Split(s, ",")
				}
				out = append(out, map[string]interface{}{
					"id":                         map[string]interface{}{"entityType": "OAUTH2_CLIENT_REGISTRATION_TEMPLATE", "id": id},
					"createdTime":                createdTime,
					"providerId":                 providerID,
					"authorizationUri":           valOr(authURI, ""),
					"accessTokenUri":             valOr(tokenURI, ""),
					"scope":                      scopeList,
					"userInfoUri":                valOr(userInfoURI, ""),
					"userNameAttributeName":      valOr(userNameAttr, ""),
					"jwkSetUri":                  valOr(jwkSetURI, ""),
					"clientAuthenticationMethod": valOr(clientAuthMethod, ""),
					"type":                       valOr(typ, ""),
					"comment":                    valOr(comment, ""),
					"loginButtonIcon":            valOr(icon, ""),
					"loginButtonLabel":           valOr(label, ""),
					"helpLink":                   valOr(helpLink, ""),
					"mapperConfig": map[string]interface{}{
						"type": "BASIC",
						"basic": map[string]interface{}{
							"emailAttributeKey":     valOr(emailKey, ""),
							"firstNameAttributeKey": valOr(firstNameKey, ""),
							"lastNameAttributeKey":  valOr(lastNameKey, ""),
							"tenantNameStrategy":    valOr(tenantStrategy, ""),
						},
					},
				})
			}
		}
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

// HandleTenantDashboardHomeInfo GET/POST /api/tenant/dashboard/home/info —
// the tenant's configured home dashboard. TB shape: {dashboardId, hideDashboardToolbar}.
// TB-classic convention stores this inside tenant.additional_info as
// homeDashboardId/homeDashboardHideToolbar (the same column
// internal/tenant.HandleTenantSave already reads and writes) — GET now reads
// it back instead of always answering "none configured," and POST persists a
// merge into the existing JSON rather than a blind overwrite, so it doesn't
// clobber any other key a tenant's additionalInfo may carry.
func HandleTenantDashboardHomeInfo(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var additionalInfo map[string]interface{}
	loadAdditionalInfo := func() {
		var raw sql.NullString
		if dbpkg.Pool.QueryRow(
			"SELECT additional_info FROM tenant WHERE id = $1", tenantId,
		).Scan(&raw) == nil && raw.Valid && raw.String != "" {
			_ = json.Unmarshal([]byte(raw.String), &additionalInfo)
		}
		if additionalInfo == nil {
			additionalInfo = map[string]interface{}{}
		}
	}

	if r.Method == http.MethodPost {
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}
		loadAdditionalInfo()
		additionalInfo["homeDashboardId"] = body["dashboardId"]
		additionalInfo["homeDashboardHideToolbar"] = body["hideDashboardToolbar"]
		encoded, _ := json.Marshal(additionalInfo)
		if _, err := dbpkg.Pool.Exec(
			"UPDATE tenant SET additional_info = $1 WHERE id = $2", string(encoded), tenantId,
		); err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to save home dashboard")
			return
		}
	} else {
		loadAdditionalInfo()
	}

	dashboardId := additionalInfo["homeDashboardId"]
	hideToolbar, ok := additionalInfo["homeDashboardHideToolbar"].(bool)
	if !ok {
		hideToolbar = true // TB default when never configured
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"dashboardId":          dashboardId,
		"hideDashboardToolbar": hideToolbar,
	})
}

// HandleMobileBundleInfos GET /api/mobile/bundle/infos — used by the Mobile Center.
func HandleMobileBundleInfos(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pageSize := httputil.PageSize(r, 10)
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
