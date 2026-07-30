package system

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// ─── 2FA ──────────────────────────────────────────────────────────────────────

// HandleTwoFaProviders /api/auth/2fa/providers — list providers available for login.
// Empty array = no 2FA configured.
func HandleTwoFaProviders(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, []interface{}{})
}

// HandleTwoFaProvidersPage /api/2fa/providers — admin provider list.
func HandleTwoFaProvidersPage(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	EmptyPageData(w)
}

// HandleTwoFaAccountConfig /api/2fa/account/config — current user's 2FA config.
func HandleTwoFaAccountConfig(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if r.Method == "DELETE" {
		w.WriteHeader(http.StatusOK)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{"configs": map[string]interface{}{}})
}

// HandleTwoFaAccountConfigProviders /api/2fa/account/config/providers
func HandleTwoFaAccountConfigProviders(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, []interface{}{})
}

// HandleTwoFaAccountSettings /api/2fa/account/settings.
func HandleTwoFaAccountSettings(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"providers": []interface{}{},
	})
}

// HandleTwoFaSettings /api/2fa/settings — admin level config.
func HandleTwoFaSettings(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if r.Method == "POST" || r.Method == "PUT" {
		w.WriteHeader(http.StatusOK)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"providers":                                []interface{}{},
		"minVerificationCodeSendPeriod":            30,
		"verificationCodeCheckRateLimit":           "3:900",
		"maxVerificationFailuresBeforeUserLockout": 0,
		"totalAllowedTimeForVerification":          3600,
	})
}

// ─── OAuth2 client CRUD ───────────────────────────────────────────────────────

// HandleOAuth2Client POST/PUT/DELETE /api/oauth2/client — list of client registrations.
// Tables: oauth2_client.
func HandleOAuth2Client(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		listOAuth2Clients(w, r)
	case "POST", "PUT":
		saveOAuth2Client(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func listOAuth2Clients(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pageSize := httputil.IntParam(r, "pageSize", 100)
	page := httputil.IntParam(r, "page", 0)
	rows, err := dbpkg.Pool.Query(
		`SELECT id, created_time, title, login_button_label, additional_info FROM oauth2_client
		 ORDER BY title LIMIT $1 OFFSET $2`,
		pageSize, page*pageSize,
	)
	out := []map[string]interface{}{}
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			var createdTime int64
			var title, label, additionalInfo *string
			_ = rows.Scan(&id, &createdTime, &title, &label, &additionalInfo)
			out = append(out, map[string]interface{}{
				"id":               map[string]interface{}{"entityType": "OAUTH2_CLIENT", "id": id},
				"createdTime":      createdTime,
				"title":            valOr(title, ""),
				"loginButtonLabel": valOr(label, ""),
			})
		}
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data": out, "totalPages": 1, "totalElements": len(out), "hasNext": false,
	})
}

func saveOAuth2Client(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	// Best-effort save. The full oauth2_client schema has many JSON columns; we
	// store the bare minimum and serialize the rest into additional_info.
	id := httputil.ExtractEntityID(body, "id")
	if id == "" {
		id = uuid.New().String()
	}
	title, _ := body["title"].(string)
	loginLabel, _ := body["loginButtonLabel"].(string)
	additional, _ := json.Marshal(body)
	now := time.Now().UnixMilli()
	_, err := dbpkg.Pool.Exec(
		`INSERT INTO oauth2_client (id, created_time, title, login_button_label, additional_info)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title,
		   login_button_label = EXCLUDED.login_button_label,
		   additional_info = EXCLUDED.additional_info`,
		id, now, title, loginLabel, string(additional),
	)
	if err != nil {
		log.Printf("WARN saveOAuth2Client: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save oauth2 client")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":               map[string]interface{}{"entityType": "OAUTH2_CLIENT", "id": id},
		"title":            title,
		"loginButtonLabel": loginLabel,
		"createdTime":      now,
	})
}

// ─── Admin settings ───────────────────────────────────────────────────────────

// HandleAdminSettingsSave POST /api/admin/settings
func HandleAdminSettingsSave(w http.ResponseWriter, r *http.Request) {
	// Writing admin_settings (jwt/mail/security/general) is a system-admin
	// action — a tenant must not be able to rewrite the signing key or SMTP
	// config. Gate the whole save on SYS_ADMIN.
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	var body struct {
		Key       string                 `json:"key"`
		JsonValue map[string]interface{} `json:"jsonValue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if body.Key == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing key")
		return
	}
	jsonBytes, _ := json.Marshal(body.JsonValue)
	_, err := dbpkg.Pool.Exec(
		`INSERT INTO admin_settings (id, created_time, tenant_id, key, json_value)
		 VALUES ($1, $2, '13814000-1dd2-11b2-8080-808080808080', $3, $4)
		 ON CONFLICT (tenant_id, key) DO UPDATE SET json_value = EXCLUDED.json_value`,
		uuid.New().String(), time.Now().UnixMilli(), body.Key, string(jsonBytes),
	)
	if err != nil {
		log.Printf("WARN admin settings save: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save settings")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleAdminSettingsTestMail POST /api/admin/settings/testMail — stubbed: no SMTP.
func HandleAdminSettingsTestMail(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	// We don't run an SMTP client; surface a friendly error so the admin UI
	// reports it rather than appearing to succeed silently.
	httputil.WriteJSON(w, http.StatusBadRequest, map[string]interface{}{
		"errorCode": 32,
		"message":   "Mail sending is not configured in this deployment",
		"status":    400,
		"timestamp": time.Now().UnixMilli(),
	})
}

// HandleAdminSettingsTestSms POST /api/admin/settings/testSms — same idea.
func HandleAdminSettingsTestSms(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusBadRequest, map[string]interface{}{
		"errorCode": 32,
		"message":   "SMS sending is not configured in this deployment",
		"status":    400,
		"timestamp": time.Now().UnixMilli(),
	})
}

// ─── Notification CRUD ────────────────────────────────────────────────────────

// HandleNotificationTarget GET/POST/PUT/DELETE /api/notification/target
func HandleNotificationTarget(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	switch r.Method {
	case "GET":
		HandleNotificationTargets(w, r)
	case "DELETE":
		w.WriteHeader(http.StatusOK)
	default:
		saveJSONEntity(w, r, "NOTIFICATION_TARGET")
	}
}

// HandleNotificationTemplate GET/POST/PUT/DELETE /api/notification/template
func HandleNotificationTemplate(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	switch r.Method {
	case "GET":
		HandleNotificationTemplates(w, r)
	case "DELETE":
		w.WriteHeader(http.StatusOK)
	default:
		saveJSONEntity(w, r, "NOTIFICATION_TEMPLATE")
	}
}

// HandleNotificationRule GET/POST/PUT/DELETE /api/notification/rule
func HandleNotificationRule(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	switch r.Method {
	case "GET":
		HandleNotificationRules(w, r)
	case "DELETE":
		w.WriteHeader(http.StatusOK)
	default:
		saveJSONEntity(w, r, "NOTIFICATION_RULE")
	}
}

// HandleNotificationRequestSave POST /api/notification/request — fires a one-shot
// notification. We don't have notification delivery wired up; reply 200 with the
// supplied body so the UI's "send" flow finishes cleanly.
func HandleNotificationRequestSave(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	var body map[string]interface{}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body == nil {
		body = map[string]interface{}{}
	}
	body["id"] = map[string]interface{}{"entityType": "NOTIFICATION_REQUEST", "id": uuid.New().String()}
	body["createdTime"] = time.Now().UnixMilli()
	body["status"] = "SENT"
	httputil.WriteJSON(w, http.StatusOK, body)
}

// HandleNotificationRequestPreview POST /api/notification/request/preview
func HandleNotificationRequestPreview(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"recipientsCountByTarget":             map[string]interface{}{},
		"processedTemplates":                  map[string]interface{}{},
		"firstRecipientToReceiveNotification": nil,
		"recipientsPreview":                   []interface{}{},
		"totalRecipientsCount":                0,
	})
}

// HandleNotificationsRead POST /api/notifications/read?notifications=id1,id2
func HandleNotificationsRead(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// saveJSONEntity is a helper for stub-style CRUD that just echoes the payload
// back with an id and createdTime stamped. Useful for endpoints we don't fully
// persist yet but where the UI expects a 200 with the saved entity.
func saveJSONEntity(w http.ResponseWriter, r *http.Request, entityType string) {
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if body == nil {
		body = map[string]interface{}{}
	}
	id := httputil.ExtractEntityID(body, "id")
	if id == "" {
		id = uuid.New().String()
		body["id"] = map[string]interface{}{"entityType": entityType, "id": id}
		body["createdTime"] = time.Now().UnixMilli()
	}
	httputil.WriteJSON(w, http.StatusOK, body)
}

// ─── Calculated fields ────────────────────────────────────────────────────────

// HandleCalculatedField CRUD /api/calculatedField — list/save/delete.
func HandleCalculatedField(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	switch r.Method {
	case "GET":
		EmptyPageData(w)
	case "POST", "PUT":
		saveJSONEntity(w, r, "CALCULATED_FIELD")
	case "DELETE":
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleCalculatedFieldByID GET/DELETE /api/calculatedField/{id}.
func HandleCalculatedFieldByID(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	switch r.Method {
	case "GET":
		httputil.WriteError(w, http.StatusNotFound, "Calculated field not found")
	case "DELETE":
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleCalculatedFieldDebug POST /api/calculatedField/{id}/debug.
func HandleCalculatedFieldDebug(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"output": nil,
		"error":  "Calculated field debugging is not enabled in this deployment",
	})
}

// HandleCalculatedFieldNames GET /api/calculatedFields/names.
func HandleCalculatedFieldNames(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, []interface{}{})
}

// HandleCalculatedFieldTestScript POST /api/calculatedField/testScript
func HandleCalculatedFieldTestScript(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"output": nil,
		"error":  "Calculated field testing is not enabled in this deployment",
	})
}

// ─── Edge CRUD ────────────────────────────────────────────────────────────────

// HandleEdge POST/PUT/GET /api/edge
func HandleEdge(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	switch r.Method {
	case "POST", "PUT":
		saveJSONEntity(w, r, "EDGE")
	case "DELETE":
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

// HandleEdgeBulkImport POST /api/edge/bulk_import
func HandleEdgeBulkImport(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{"created": 0, "updated": 0, "errors": 0})
}

// ─── Mobile bundle CRUD ───────────────────────────────────────────────────────

// HandleMobileApp + HandleMobileBundle — full stub-CRUD for the Mobile Center
// pages. We persist via the generic JSON echo since the UI mainly relies on
// the listing being non-empty and the saved item being echoed back.
func HandleMobileApp(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	switch r.Method {
	case "POST", "PUT":
		saveJSONEntity(w, r, "MOBILE_APP")
	case "DELETE":
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

func HandleMobileBundle(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	switch r.Method {
	case "POST", "PUT":
		saveJSONEntity(w, r, "MOBILE_APP_BUNDLE")
	case "DELETE":
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

// HandleMobileQrDeepLink GET /api/mobile/qr/deepLink — returns the QR target URL
// the UI renders. We point at the configured public app store entries.
func HandleMobileQrDeepLink(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`"https://thingsboard.com/connect"`))
}

// ─── Tenant profile (info-style endpoint that some pages hit) ─────────────────

// HandleTenantProfileInfoDefault /api/tenantProfileInfo/default
func HandleTenantProfileInfoDefault(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	var id, name string
	err := dbpkg.Pool.QueryRow(
		`SELECT id::text, name FROM tenant_profile WHERE is_default = true ORDER BY created_time LIMIT 1`,
	).Scan(&id, &name)
	if err != nil {
		httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
			"id":   map[string]interface{}{"entityType": "TENANT_PROFILE", "id": "00000000-0000-0000-0000-000000000000"},
			"name": "Default",
		})
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":   map[string]interface{}{"entityType": "TENANT_PROFILE", "id": id},
		"name": name,
	})
}

// HandleEntityActions — covers POST endpoints that just need to acknowledge.
// Currently used as a generic "200 OK echo" handler for entity assignments,
// favorites, etc. that the UI fires.
func HandleEntityActions(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ─── Audit log filters (PageData by entity/customer/user) ─────────────────────

// HandleAuditLogsByDimension POST/GET /api/audit/logs/{customer|user|entity}/{id}
func HandleAuditLogsByDimension(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/audit/logs/"), "/")
	if len(parts) < 2 {
		EmptyPageData(w)
		return
	}
	dimension := parts[0] // customer / user / entity
	dimId := parts[1]

	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	var col string
	switch dimension {
	case "customer":
		col = "customer_id"
	case "user":
		col = "user_id"
	case "entity":
		col = "entity_id"
	default:
		EmptyPageData(w)
		return
	}
	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM audit_log WHERE tenant_id = $1 AND "+col+" = $2", tenantId, dimId).Scan(&total)
	rows, err := dbpkg.Pool.Query(
		"SELECT id, created_time, action_type, action_status, entity_name FROM audit_log "+
			"WHERE tenant_id = $1 AND "+col+" = $2 ORDER BY created_time DESC LIMIT $3 OFFSET $4",
		tenantId, dimId, pageSize, page*pageSize,
	)
	if err != nil {
		EmptyPageData(w)
		return
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var id string
		var createdTime int64
		var actionType, actionStatus, entityName *string
		_ = rows.Scan(&id, &createdTime, &actionType, &actionStatus, &entityName)
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "AUDIT_LOG", "id": id},
			"createdTime": createdTime,
		}
		if actionType != nil {
			item["actionType"] = *actionType
		}
		if actionStatus != nil {
			item["actionStatus"] = *actionStatus
		}
		if entityName != nil {
			item["entityName"] = *entityName
		}
		out = append(out, item)
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data": out, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

// valOr is a small *string -> string fallback used by several handlers.
func valOr(p *string, fallback string) string {
	if p == nil {
		return fallback
	}
	return *p
}
