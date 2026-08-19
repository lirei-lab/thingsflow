package system

import (
	"database/sql"
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

// Real persistence for the notification entities.
//
// These four writers used to route through saveJSONEntity, which echoes the
// posted JSON back with a generated id and never touches the database — so
// every list endpoint correctly reported empty, because nothing had ever been
// written. The tables (notification_target, notification_template,
// notification_rule, notification_request) have been in the schema all along;
// only the wiring was missing (docs/UI_CONTRACT_DATA_FIDELITY.md P2).
//
// Delivery is still out of scope: this platform has no email/SMS transport,
// so a request is recorded rather than sent, and its status says so.

// notifPage reads the standard page params both notification lists use.
func notifPage(r *http.Request) (pageSize, page, offset int) {
	pageSize = httputil.PageSize(r, 10)
	page = httputil.IntParam(r, "page", 0)
	return pageSize, page, page * pageSize
}

func writeNotifPage(w http.ResponseWriter, data []map[string]interface{}, total, pageSize, page int) {
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page + 1) < totalPages,
	})
}

// jsonTextOrEmptyObject renders a body field as JSON text for a NOT NULL
// column, defaulting to "{}" so a payload omitting it still persists.
func jsonTextOrEmptyObject(v interface{}) string {
	if v == nil {
		return "{}"
	}
	encoded, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func decodeJSONColumn(raw *string) interface{} {
	if raw == nil || *raw == "" {
		return map[string]interface{}{}
	}
	var out interface{}
	if json.Unmarshal([]byte(*raw), &out) != nil {
		return map[string]interface{}{}
	}
	return out
}

// ─── Targets ─────────────────────────────────────────────────────────────────

func saveNotificationTarget(w http.ResponseWriter, r *http.Request, tenantId string) {
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	name, _ := body["name"].(string)
	if strings.TrimSpace(name) == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing notification target name")
		return
	}
	configuration := jsonTextOrEmptyObject(body["configuration"])
	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		if !notifRowBelongsToTenant(w, "notification_target", id, tenantId) {
			return
		}
		if _, err := dbpkg.Pool.Exec(
			`UPDATE notification_target SET name = $1, configuration = $2 WHERE id = $3`,
			name, configuration, id); err != nil {
			log.Printf("ERROR updating notification_target %s: %v", id, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update notification target")
			return
		}
	} else {
		id = uuid.New().String()
		if _, err := dbpkg.Pool.Exec(`
			INSERT INTO notification_target (id, created_time, tenant_id, name, configuration)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (tenant_id, name) DO UPDATE SET configuration = EXCLUDED.configuration`,
			id, now, tenantId, name, configuration); err != nil {
			log.Printf("ERROR inserting notification_target %s: %v", name, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to save notification target")
			return
		}
		// The ON CONFLICT branch keeps the existing row's id.
		var stored string
		if dbpkg.Pool.QueryRow(
			"SELECT id::text FROM notification_target WHERE tenant_id = $1 AND name = $2", tenantId, name,
		).Scan(&stored) == nil && stored != "" {
			id = stored
		}
	}
	writeNotificationTargetByID(w, id, tenantId)
}

func writeNotificationTargetByID(w http.ResponseWriter, id, tenantId string) {
	var createdTime int64
	var name string
	var configuration *string
	if err := dbpkg.Pool.QueryRow(
		`SELECT created_time, name, configuration FROM notification_target WHERE id = $1 AND tenant_id = $2`,
		id, tenantId).Scan(&createdTime, &name, &configuration); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Notification target saved but could not be read back")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":            map[string]interface{}{"entityType": "NOTIFICATION_TARGET", "id": id},
		"createdTime":   createdTime,
		"tenantId":      map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"name":          name,
		"configuration": decodeJSONColumn(configuration),
	})
}

func listNotificationTargets(w http.ResponseWriter, r *http.Request, tenantId string) {
	pageSize, page, offset := notifPage(r)
	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM notification_target WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, name, configuration FROM notification_target
		 WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		log.Printf("WARN notification_target list: %v", err)
		EmptyPageData(w)
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name string
		var createdTime int64
		var configuration *string
		if rows.Scan(&id, &createdTime, &name, &configuration) != nil {
			continue
		}
		data = append(data, map[string]interface{}{
			"id":            map[string]interface{}{"entityType": "NOTIFICATION_TARGET", "id": id},
			"createdTime":   createdTime,
			"tenantId":      map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"name":          name,
			"configuration": decodeJSONColumn(configuration),
		})
	}
	writeNotifPage(w, data, total, pageSize, page)
}

// ─── Templates ───────────────────────────────────────────────────────────────

func saveNotificationTemplate(w http.ResponseWriter, r *http.Request, tenantId string) {
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	name, _ := body["name"].(string)
	if strings.TrimSpace(name) == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing notification template name")
		return
	}
	notificationType, _ := body["notificationType"].(string)
	if notificationType == "" {
		notificationType = "GENERAL"
	}
	configuration := jsonTextOrEmptyObject(body["configuration"])
	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		if !notifRowBelongsToTenant(w, "notification_template", id, tenantId) {
			return
		}
		if _, err := dbpkg.Pool.Exec(
			`UPDATE notification_template SET name = $1, notification_type = $2, configuration = $3 WHERE id = $4`,
			name, notificationType, configuration, id); err != nil {
			log.Printf("ERROR updating notification_template %s: %v", id, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update notification template")
			return
		}
	} else {
		id = uuid.New().String()
		if _, err := dbpkg.Pool.Exec(`
			INSERT INTO notification_template (id, created_time, tenant_id, name, notification_type, configuration)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (tenant_id, name) DO UPDATE SET
			  notification_type = EXCLUDED.notification_type, configuration = EXCLUDED.configuration`,
			id, now, tenantId, name, notificationType, configuration); err != nil {
			log.Printf("ERROR inserting notification_template %s: %v", name, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to save notification template")
			return
		}
		var stored string
		if dbpkg.Pool.QueryRow(
			"SELECT id::text FROM notification_template WHERE tenant_id = $1 AND name = $2", tenantId, name,
		).Scan(&stored) == nil && stored != "" {
			id = stored
		}
	}
	writeNotificationTemplateByID(w, id, tenantId)
}

func writeNotificationTemplateByID(w http.ResponseWriter, id, tenantId string) {
	var createdTime int64
	var name, notificationType string
	var configuration *string
	if err := dbpkg.Pool.QueryRow(
		`SELECT created_time, name, notification_type, configuration
		   FROM notification_template WHERE id = $1 AND tenant_id = $2`,
		id, tenantId).Scan(&createdTime, &name, &notificationType, &configuration); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Notification template saved but could not be read back")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":               map[string]interface{}{"entityType": "NOTIFICATION_TEMPLATE", "id": id},
		"createdTime":      createdTime,
		"tenantId":         map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"name":             name,
		"notificationType": notificationType,
		"configuration":    decodeJSONColumn(configuration),
	})
}

func listNotificationTemplates(w http.ResponseWriter, r *http.Request, tenantId string) {
	pageSize, page, offset := notifPage(r)
	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM notification_template WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, name, notification_type, configuration FROM notification_template
		 WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		log.Printf("WARN notification_template list: %v", err)
		EmptyPageData(w)
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, notificationType string
		var createdTime int64
		var configuration *string
		if rows.Scan(&id, &createdTime, &name, &notificationType, &configuration) != nil {
			continue
		}
		data = append(data, map[string]interface{}{
			"id":               map[string]interface{}{"entityType": "NOTIFICATION_TEMPLATE", "id": id},
			"createdTime":      createdTime,
			"tenantId":         map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"name":             name,
			"notificationType": notificationType,
			"configuration":    decodeJSONColumn(configuration),
		})
	}
	writeNotifPage(w, data, total, pageSize, page)
}

// ─── Rules ───────────────────────────────────────────────────────────────────

func saveNotificationRule(w http.ResponseWriter, r *http.Request, tenantId string) {
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	name, _ := body["name"].(string)
	if strings.TrimSpace(name) == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing notification rule name")
		return
	}
	// template_id is NOT NULL with an FK, so a rule cannot be stored without
	// a template that exists in this tenant. Refusing up front beats a
	// constraint-violation 500.
	templateId := httputil.ExtractEntityID(body, "templateId")
	if templateId == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing templateId")
		return
	}
	var templateTenant string
	if err := dbpkg.Pool.QueryRow(
		"SELECT tenant_id::text FROM notification_template WHERE id = $1", templateId).Scan(&templateTenant); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Unknown templateId")
		return
	}
	if templateTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant template reference denied")
		return
	}

	triggerType, _ := body["triggerType"].(string)
	if triggerType == "" {
		triggerType = "GENERAL"
	}
	enabled := true
	if v, ok := body["enabled"].(bool); ok {
		enabled = v
	}
	triggerConfig := jsonTextOrEmptyObject(body["triggerConfig"])
	recipientsConfig := jsonTextOrEmptyObject(body["recipientsConfig"])
	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		if !notifRowBelongsToTenant(w, "notification_rule", id, tenantId) {
			return
		}
		if _, err := dbpkg.Pool.Exec(`
			UPDATE notification_rule SET name = $1, enabled = $2, template_id = $3,
			    trigger_type = $4, trigger_config = $5, recipients_config = $6
			WHERE id = $7`,
			name, enabled, templateId, triggerType, triggerConfig, recipientsConfig, id); err != nil {
			log.Printf("ERROR updating notification_rule %s: %v", id, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update notification rule")
			return
		}
	} else {
		id = uuid.New().String()
		if _, err := dbpkg.Pool.Exec(`
			INSERT INTO notification_rule (id, created_time, tenant_id, name, enabled,
			                               template_id, trigger_type, trigger_config, recipients_config)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (tenant_id, name) DO UPDATE SET
			  enabled = EXCLUDED.enabled, template_id = EXCLUDED.template_id,
			  trigger_type = EXCLUDED.trigger_type, trigger_config = EXCLUDED.trigger_config,
			  recipients_config = EXCLUDED.recipients_config`,
			id, now, tenantId, name, enabled, templateId, triggerType, triggerConfig, recipientsConfig); err != nil {
			log.Printf("ERROR inserting notification_rule %s: %v", name, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to save notification rule")
			return
		}
		var stored string
		if dbpkg.Pool.QueryRow(
			"SELECT id::text FROM notification_rule WHERE tenant_id = $1 AND name = $2", tenantId, name,
		).Scan(&stored) == nil && stored != "" {
			id = stored
		}
	}
	writeNotificationRuleByID(w, id, tenantId)
}

func writeNotificationRuleByID(w http.ResponseWriter, id, tenantId string) {
	var createdTime int64
	var name, triggerType, templateId string
	var enabled bool
	var triggerConfig, recipientsConfig *string
	if err := dbpkg.Pool.QueryRow(`
		SELECT created_time, name, enabled, template_id::text, trigger_type, trigger_config, recipients_config
		  FROM notification_rule WHERE id = $1 AND tenant_id = $2`, id, tenantId).Scan(
		&createdTime, &name, &enabled, &templateId, &triggerType, &triggerConfig, &recipientsConfig); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Notification rule saved but could not be read back")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":               map[string]interface{}{"entityType": "NOTIFICATION_RULE", "id": id},
		"createdTime":      createdTime,
		"tenantId":         map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"name":             name,
		"enabled":          enabled,
		"templateId":       map[string]interface{}{"entityType": "NOTIFICATION_TEMPLATE", "id": templateId},
		"triggerType":      triggerType,
		"triggerConfig":    decodeJSONColumn(triggerConfig),
		"recipientsConfig": decodeJSONColumn(recipientsConfig),
	})
}

func listNotificationRules(w http.ResponseWriter, r *http.Request, tenantId string) {
	pageSize, page, offset := notifPage(r)
	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM notification_rule WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, name, enabled, template_id::text, trigger_type, trigger_config, recipients_config
		  FROM notification_rule WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`,
		tenantId, pageSize, offset)
	if err != nil {
		log.Printf("WARN notification_rule list: %v", err)
		EmptyPageData(w)
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, templateId, triggerType string
		var createdTime int64
		var enabled bool
		var triggerConfig, recipientsConfig *string
		if rows.Scan(&id, &createdTime, &name, &enabled, &templateId, &triggerType,
			&triggerConfig, &recipientsConfig) != nil {
			continue
		}
		data = append(data, map[string]interface{}{
			"id":               map[string]interface{}{"entityType": "NOTIFICATION_RULE", "id": id},
			"createdTime":      createdTime,
			"tenantId":         map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"name":             name,
			"enabled":          enabled,
			"templateId":       map[string]interface{}{"entityType": "NOTIFICATION_TEMPLATE", "id": templateId},
			"triggerType":      triggerType,
			"triggerConfig":    decodeJSONColumn(triggerConfig),
			"recipientsConfig": decodeJSONColumn(recipientsConfig),
		})
	}
	writeNotifPage(w, data, total, pageSize, page)
}

// ─── Requests ────────────────────────────────────────────────────────────────

// saveNotificationRequest records a send request. Status is SCHEDULED rather
// than SENT: this platform ships no email/SMS transport, so claiming delivery
// would be the same kind of fabricated success this audit exists to remove.
func saveNotificationRequest(w http.ResponseWriter, r *http.Request, tenantId string) {
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	targets := jsonTextOrEmptyObject(body["targets"])
	template := jsonTextOrEmptyObject(body["template"])
	info := jsonTextOrEmptyObject(body["info"])
	additionalConfig := jsonTextOrEmptyObject(body["additionalConfig"])
	templateId := httputil.ExtractEntityID(body, "templateId")
	now := time.Now().UnixMilli()
	id := uuid.New().String()

	var templateIdArg interface{}
	if templateId != "" {
		templateIdArg = templateId
	}

	if _, err := dbpkg.Pool.Exec(`
		INSERT INTO notification_request (id, created_time, tenant_id, targets, template_id,
		                                  template, info, additional_config, status, stats)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'SCHEDULED', '{}')`,
		id, now, tenantId, targets, templateIdArg, template, info, additionalConfig); err != nil {
		log.Printf("ERROR inserting notification_request: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save notification request")
		return
	}
	writeNotificationRequestByID(w, id, tenantId)
}

func writeNotificationRequestByID(w http.ResponseWriter, id, tenantId string) {
	var createdTime int64
	var status string
	var targets, template, info, stats *string
	var templateId *string
	if err := dbpkg.Pool.QueryRow(`
		SELECT created_time, targets, template_id::text, template, info, status, stats
		  FROM notification_request WHERE id = $1 AND tenant_id = $2`, id, tenantId).Scan(
		&createdTime, &targets, &templateId, &template, &info, &status, &stats); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Notification request saved but could not be read back")
		return
	}
	out := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "NOTIFICATION_REQUEST", "id": id},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"targets":     decodeJSONColumn(targets),
		"template":    decodeJSONColumn(template),
		"info":        decodeJSONColumn(info),
		"status":      status,
		"stats":       decodeJSONColumn(stats),
	}
	if templateId != nil && *templateId != "" {
		out["templateId"] = map[string]interface{}{"entityType": "NOTIFICATION_TEMPLATE", "id": *templateId}
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

func listNotificationRequests(w http.ResponseWriter, r *http.Request, tenantId string) {
	pageSize, page, offset := notifPage(r)
	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM notification_request WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, targets, template_id::text, template, info, status, stats
		  FROM notification_request WHERE tenant_id = $1
		 ORDER BY created_time DESC LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		log.Printf("WARN notification_request list: %v", err)
		EmptyPageData(w)
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, status string
		var createdTime int64
		var targets, templateId, template, info, stats *string
		if rows.Scan(&id, &createdTime, &targets, &templateId, &template, &info, &status, &stats) != nil {
			continue
		}
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "NOTIFICATION_REQUEST", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"targets":     decodeJSONColumn(targets),
			"template":    decodeJSONColumn(template),
			"info":        decodeJSONColumn(info),
			"status":      status,
			"stats":       decodeJSONColumn(stats),
		}
		if templateId != nil && *templateId != "" {
			item["templateId"] = map[string]interface{}{"entityType": "NOTIFICATION_TEMPLATE", "id": *templateId}
		}
		data = append(data, item)
	}
	writeNotifPage(w, data, total, pageSize, page)
}

// ─── Recipient resolution ────────────────────────────────────────────────────

// resolveRecipientCount counts the users a notification target resolves to,
// for the request-preview screen.
//
// The `usersFilter` vocabulary and field names are not guessed: they were
// read out of the deployed UI bundle (thingsboard/tb-web-ui:4.3.1.1,
// `configuration.usersFilter.{type,usersIds,customerId,filterByTenants,
// tenantsIds,tenantProfilesIds}`), the same source the endpoint catalogue in
// docs/UI_CONTRACT_COVERAGE.md was extracted from.
//
// An unrecognised filter returns ok=false and the caller keeps reporting 0,
// rather than inventing a number. That distinction matters: 0 reads as "not
// computed", while a wrong count reads as authoritative — the exact failure
// this audit exists to remove. Filters that depend on subsystems this
// platform does not have (system administrators, tenant-profile fan-out) are
// deliberately left unresolved for the same reason.
func resolveRecipientCount(tenantId string, configuration interface{}) (int, bool) {
	config, ok := configuration.(map[string]interface{})
	if !ok || dbpkg.Pool == nil {
		return 0, false
	}
	usersFilter, ok := config["usersFilter"].(map[string]interface{})
	if !ok {
		return 0, false
	}
	filterType, _ := usersFilter["type"].(string)

	switch filterType {
	case "ALL_USERS", "TENANT_ADMINISTRATORS":
		// Both are tenant-wide here: this platform has one authority per
		// user and no cross-tenant fan-out, so ALL_USERS within a tenant and
		// its administrators are the same population unless the authority
		// column distinguishes them.
		query := "SELECT count(*) FROM tb_user WHERE tenant_id = $1"
		args := []interface{}{tenantId}
		if filterType == "TENANT_ADMINISTRATORS" {
			query += " AND authority = 'TENANT_ADMIN'"
		}
		// filterByTenants/tenantsIds/tenantProfilesIds are a sysadmin
		// cross-tenant fan-out; a tenant-scoped caller cannot use them, and
		// honouring them here would cross the tenant boundary every other
		// read in this package enforces.
		if v, _ := usersFilter["filterByTenants"].(bool); v {
			return 0, false
		}
		var count int
		if dbpkg.Pool.QueryRow(query, args...).Scan(&count) != nil {
			return 0, false
		}
		return count, true

	case "CUSTOMER_USERS":
		customerId := httputil.ExtractEntityID(usersFilter, "customerId")
		if customerId == "" {
			return 0, false
		}
		var count int
		if dbpkg.Pool.QueryRow(
			"SELECT count(*) FROM tb_user WHERE tenant_id = $1 AND customer_id = $2",
			tenantId, customerId).Scan(&count) != nil {
			return 0, false
		}
		return count, true

	case "USER_LIST":
		raw, ok := usersFilter["usersIds"].([]interface{})
		if !ok {
			return 0, false
		}
		ids := make([]string, 0, len(raw))
		for _, item := range raw {
			if id, ok := item.(string); ok && httputil.LooksLikeUUID(id) {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return 0, true
		}
		// Counted through the database rather than by len(ids): the preview
		// should reflect users that actually exist in this tenant, so a
		// stale or foreign id does not inflate it.
		count := 0
		for _, id := range ids {
			var exists int
			if dbpkg.Pool.QueryRow(
				"SELECT count(*) FROM tb_user WHERE id = $1 AND tenant_id = $2",
				id, tenantId).Scan(&exists) == nil {
				count += exists
			}
		}
		return count, true
	}
	return 0, false
}

// ─── Read markers ────────────────────────────────────────────────────────────

// markNotificationsRead performs the real UPDATE for
// PUT /api/notifications/read, scoped to the calling user so one recipient
// cannot mark another's notifications.
//
// Nothing produces rows in `notification` yet — delivery is unimplemented —
// so today this legitimately affects zero rows. That is a different thing
// from the unconditional 200 it replaces: this one becomes correct on its
// own the moment a producer exists.
func markNotificationsRead(w http.ResponseWriter, r *http.Request, userId string) {
	ids := notificationIdsFromRequest(r)
	if len(ids) == 0 {
		// TB's "mark all read" form.
		if _, err := dbpkg.Pool.Exec(
			`UPDATE notification SET status = 'READ' WHERE recipient_id = $1 AND status <> 'READ'`,
			userId); err != nil {
			log.Printf("WARN notifications read-all: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	for _, id := range ids {
		if _, err := dbpkg.Pool.Exec(
			`UPDATE notification SET status = 'READ' WHERE id = $1 AND recipient_id = $2`,
			id, userId); err != nil {
			log.Printf("WARN notification read %s: %v", id, err)
		}
	}
	w.WriteHeader(http.StatusOK)
}

// notificationIdsFromRequest accepts the ids as a repeated/comma-joined query
// param (TB's shape) or as a JSON array body, ignoring anything that is not a
// UUID so a malformed id cannot reach postgres as an invalid cast.
func notificationIdsFromRequest(r *http.Request) []string {
	var out []string
	seen := map[string]bool{}
	add := func(candidate string) {
		candidate = strings.TrimSpace(candidate)
		if httputil.LooksLikeUUID(candidate) && !seen[candidate] {
			seen[candidate] = true
			out = append(out, candidate)
		}
	}
	for _, raw := range r.URL.Query()["notifications"] {
		for _, part := range strings.Split(raw, ",") {
			add(part)
		}
	}
	if r.Body != nil {
		var body []string
		if json.NewDecoder(r.Body).Decode(&body) == nil {
			for _, candidate := range body {
				add(candidate)
			}
		}
	}
	return out
}

// notifRowBelongsToTenant guards an update: the row must exist and belong to
// the caller's tenant, otherwise one tenant could overwrite another's.
func notifRowBelongsToTenant(w http.ResponseWriter, table, id, tenantId string) bool {
	// table is a package-internal literal, never request data.
	var owner string
	err := dbpkg.Pool.QueryRow("SELECT tenant_id::text FROM "+table+" WHERE id = $1", id).Scan(&owner)
	if err == sql.ErrNoRows {
		httputil.WriteError(w, http.StatusNotFound, "Not found")
		return false
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return false
	}
	if owner != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
		return false
	}
	return true
}
