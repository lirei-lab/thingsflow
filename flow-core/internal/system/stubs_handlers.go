package system

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
	"flow-core/internal/ota"
)

// This file groups feature-disabled stubs: endpoints the UI calls during
// bootstrap but whose functionality (OAuth, version control, edges, mobile QR,
// trendz analytics) we don't implement in the bridge. Each returns the empty
// shape TB-Java would emit when the feature is configured but inactive, so the
// UI's RxJS pipelines see well-formed JSON and don't error.

// HandleRepositorySettingsInfo /api/admin/repositorySettings/info
func HandleRepositorySettingsInfo(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"configured": false,
		"readOnly":   nil,
	})
}

// HandleRepositorySettingsExists /api/admin/repositorySettings/exists
func HandleRepositorySettingsExists(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("false"))
}

// HandleRepositorySettings /api/admin/repositorySettings.
func HandleRepositorySettings(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
			"configured": false,
			"readOnly":   nil,
		})
		return
	}
	HandleRepositorySettingsInfo(w, r)
}

// HandleRepositorySettingsCheckAccess /api/admin/repositorySettings/checkAccess.
func HandleRepositorySettingsCheckAccess(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"accessAllowed": false,
		"message":       "Version control repository is disabled in this deployment",
	})
}

// HandleAutoCommitSettingsExists /api/admin/autoCommitSettings/exists
func HandleAutoCommitSettingsExists(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("false"))
}

// HandleUserPasswordPolicy /api/noauth/userPasswordPolicy
func HandleUserPasswordPolicy(w http.ResponseWriter, r *http.Request) {
	// Public endpoint — no auth required.
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"minimumLength":                      6,
		"maximumLength":                      72,
		"minimumUppercaseLetters":            0,
		"minimumLowercaseLetters":            0,
		"minimumDigits":                      0,
		"minimumSpecialCharacters":           0,
		"passwordExpirationPeriodDays":       0,
		"passwordReuseFrequencyDays":         0,
		"allowWhitespaces":                   true,
		"forceUserToResetPasswordIfNotValid": false,
	})
}

// HandleUiHelpBaseUrl /api/uiSettings/helpBaseUrl
func HandleUiHelpBaseUrl(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// TB returns the URL as plain text (not quoted JSON). Path includes the
	// "-help/release-4.3" suffix that the UI version expects.
	w.Write([]byte("https://raw.githubusercontent.com/thingsboard/thingsboard-ui-help/release-4.3"))
}

// HandleOAuth2LoginProcessingUrl /api/oauth2/loginProcessingUrl
func HandleOAuth2LoginProcessingUrl(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`"/login/oauth2/code/"`))
}

// HandleTrendzSettings /api/trendz/settings
func HandleTrendzSettings(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"enabled": false,
		"baseUrl": nil,
		"apiKey":  nil,
	})
}

// HandleNotificationSettings /api/notification/settings
func HandleNotificationSettings(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"deliveryMethodsConfigs": map[string]interface{}{},
	})
}

// HandleNotificationSettingsUser /api/notification/settings/user
//
// TB seeds default preferences for every notification category so the user
// settings page can render its toggle list. We mirror that seed.
func HandleNotificationSettingsUser(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	defaultPref := map[string]interface{}{
		"enabled": true,
		"enabledDeliveryMethods": map[string]interface{}{
			"WEB":        true,
			"EMAIL":      true,
			"SMS":        true,
			"MOBILE_APP": true,
		},
	}
	categories := []string{
		"GENERAL", "ALARM", "DEVICE_ACTIVITY", "ENTITY_ACTION",
		"ALARM_ASSIGNMENT", "ALARM_COMMENT",
		"RULE_ENGINE_COMPONENT_LIFECYCLE_EVENT", "RULE_NODE",
		"API_USAGE_LIMIT", "ENTITIES_LIMIT", "ENTITIES_LIMIT_INCREASE_REQUEST",
		"NEW_PLATFORM_VERSION", "RATE_LIMITS",
		"RESOURCES_SHORTAGE", "TASK_PROCESSING_FAILURE",
		"EDGE_CONNECTION", "EDGE_COMMUNICATION_FAILURE",
	}
	prefs := map[string]interface{}{}
	for _, c := range categories {
		// Each entry must be its own map (different runtime instances).
		entry := map[string]interface{}{
			"enabled": true,
			"enabledDeliveryMethods": map[string]interface{}{
				"WEB":        true,
				"EMAIL":      true,
				"SMS":        true,
				"MOBILE_APP": true,
			},
		}
		_ = defaultPref // template kept for clarity
		prefs[c] = entry
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{"prefs": prefs})
}

// HandleNotificationDeliveryMethods /api/notification/deliveryMethods
func HandleNotificationDeliveryMethods(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, []string{"WEB"})
}

// HandleNotificationTargets /api/notification/targets — list (PageData)
func HandleNotificationTargets(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	EmptyPageData(w)
}

// HandleNotificationTemplates /api/notification/templates — list (PageData)
func HandleNotificationTemplates(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	EmptyPageData(w)
}

// HandleDeviceProfileInfoDefault /api/deviceProfileInfo/default
func HandleDeviceProfileInfoDefault(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	var id, name string
	var transportType, image, defaultDashboardId *string
	err = dbpkg.Pool.QueryRow(
		`SELECT id, name, transport_type, image, default_dashboard_id::text FROM device_profile
		 WHERE tenant_id = $1 AND is_default = true LIMIT 1`,
		tenantId,
	).Scan(&id, &name, &transportType, &image, &defaultDashboardId)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Default device profile not found")
		return
	}
	resp := map[string]interface{}{
		"id":            map[string]interface{}{"entityType": "DEVICE_PROFILE", "id": id},
		"tenantId":      map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"name":          name,
		"type":          "DEFAULT",
		"transportType": httputil.CoalesceStr(transportType, "DEFAULT"),
	}
	if image != nil {
		resp["image"] = *image
	} else {
		resp["image"] = nil
	}
	if defaultDashboardId != nil && *defaultDashboardId != "" {
		resp["defaultDashboardId"] = map[string]interface{}{"entityType": "DASHBOARD", "id": *defaultDashboardId}
	} else {
		resp["defaultDashboardId"] = nil
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

// HandleAssetProfileInfoDefault /api/assetProfileInfo/default
func HandleAssetProfileInfoDefault(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	var id, name string
	var image, defaultDashboardId *string
	err = dbpkg.Pool.QueryRow(
		`SELECT id, name, image, default_dashboard_id::text FROM asset_profile
		 WHERE tenant_id = $1 AND is_default = true LIMIT 1`,
		tenantId,
	).Scan(&id, &name, &image, &defaultDashboardId)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Default asset profile not found")
		return
	}
	resp := map[string]interface{}{
		"id":       map[string]interface{}{"entityType": "ASSET_PROFILE", "id": id},
		"tenantId": map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"name":     name,
	}
	if image != nil {
		resp["image"] = *image
	} else {
		resp["image"] = nil
	}
	if defaultDashboardId != nil && *defaultDashboardId != "" {
		resp["defaultDashboardId"] = map[string]interface{}{"entityType": "DASHBOARD", "id": *defaultDashboardId}
	} else {
		resp["defaultDashboardId"] = nil
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

// HandleAssetTypes /api/asset/types
func HandleAssetTypes(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	rows, err := dbpkg.Pool.Query(
		"SELECT DISTINCT type FROM asset WHERE tenant_id = $1 AND type IS NOT NULL ORDER BY type",
		tenantId,
	)
	if err != nil {
		httputil.WriteJSON(w, http.StatusOK, []interface{}{})
		return
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err == nil {
			out = append(out, map[string]interface{}{
				"tenantId":   map[string]interface{}{"entityType": "TENANT", "id": tenantId},
				"entityType": "ASSET",
				"type":       t,
			})
		}
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

// HandleEntityViewTypes /api/entityView/types
func HandleEntityViewTypes(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	rows, err := dbpkg.Pool.Query(
		"SELECT DISTINCT type FROM entity_view WHERE tenant_id = $1 AND type IS NOT NULL ORDER BY type",
		tenantId,
	)
	if err != nil {
		httputil.WriteJSON(w, http.StatusOK, []interface{}{})
		return
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err == nil {
			out = append(out, map[string]interface{}{
				"tenantId":   map[string]interface{}{"entityType": "TENANT", "id": tenantId},
				"entityType": "ENTITY_VIEW",
				"type":       t,
			})
		}
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

// HandleEdgeTypes /api/edge/types
func HandleEdgeTypes(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, []interface{}{})
}

// HandleEdges /api/edges?pageSize=...
func HandleEdges(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	EmptyPageData(w)
}

// HandleOtaPackages — kept as alias delegating to internal/ota.List.
func HandleOtaPackages(w http.ResponseWriter, r *http.Request) {
	ota.List(w, r)
}

// HandleQueues /api/queues?serviceType=...
func HandleQueues(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	rows, err := dbpkg.Pool.Query(
		`SELECT id, created_time, name, topic, poll_interval, partitions, consumer_per_partition,
		        pack_processing_timeout, submit_strategy, processing_strategy, additional_info
		 FROM queue ORDER BY name`,
	)
	if err != nil {
		EmptyPageData(w)
		return
	}
	defer rows.Close()
	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name string
		var createdTime int64
		var topic, submitStrategy, processingStrategy, additionalInfo *string
		var pollInterval, partitions, packTimeout int
		var consumerPerPartition bool
		_ = rows.Scan(&id, &createdTime, &name, &topic, &pollInterval, &partitions, &consumerPerPartition,
			&packTimeout, &submitStrategy, &processingStrategy, &additionalInfo)
		item := map[string]interface{}{
			"id":                    map[string]interface{}{"entityType": "QUEUE", "id": id},
			"createdTime":           createdTime,
			"tenantId":              map[string]interface{}{"entityType": "TENANT", "id": "13814000-1dd2-11b2-8080-808080808080"},
			"name":                  name,
			"pollInterval":          pollInterval,
			"partitions":            partitions,
			"consumerPerPartition":  consumerPerPartition,
			"packProcessingTimeout": packTimeout,
		}
		if topic != nil {
			item["topic"] = *topic
		}
		// Pass JSON columns through as RawMessage so number formatting (0 vs 0.0)
		// is preserved byte-for-byte from Postgres.
		if submitStrategy != nil && *submitStrategy != "" {
			item["submitStrategy"] = json.RawMessage(*submitStrategy)
		}
		if processingStrategy != nil && *processingStrategy != "" {
			item["processingStrategy"] = json.RawMessage(*processingStrategy)
		}
		if additionalInfo != nil && *additionalInfo != "" {
			var a interface{}
			_ = json.Unmarshal([]byte(*additionalInfo), &a)
			item["additionalInfo"] = a
		} else {
			item["additionalInfo"] = nil
		}
		_ = tenantId
		data = append(data, item)
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data": data, "totalPages": 1, "totalElements": len(data), "hasNext": false,
	})
}

// HandleRuleChainAutoAssign /api/ruleChain/autoAssignToEdgeRuleChains
func HandleRuleChainAutoAssign(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, []interface{}{})
}

// HandleMobileQrSettings /api/mobile/qr/settings
func HandleMobileQrSettings(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":                nil,
		"createdTime":       0,
		"tenantId":          nil,
		"useSystemSettings": false,
		"useDefaultApp":     true,
		"mobileAppBundleId": nil,
		"qrCodeConfig": map[string]interface{}{
			"showOnHomePage":     true,
			"badgeEnabled":       true,
			"qrCodeLabelEnabled": true,
			"badgePosition":      "RIGHT",
			"qrCodeLabel":        "Scan to connect or download mobile app",
		},
		"androidEnabled": true,
		"iosEnabled":     true,
		"googlePlayLink": "https://play.google.com/store/apps/details?id=org.thingsboard.demo.app",
		"appStoreLink":   "https://apps.apple.com/us/app/thingsboard-live/id1594355695",
	})
}

// HandleRelationsInfo /api/relations/info?fromId=...&fromType=... — like /api/relations
// but each entry is enriched with the target entity's name.
func HandleRelationsInfo(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	if tenantId == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	q := r.URL.Query()
	fromId := q.Get("fromId")
	fromType := q.Get("fromType")
	toId := q.Get("toId")
	toType := q.Get("toType")
	relationType := q.Get("relationType")
	relationGroup := q.Get("relationTypeGroup")
	if relationGroup == "" {
		relationGroup = "COMMON"
	}

	// The `relation` table carries no tenant_id column, so we tenant-scope by
	// ownership of the anchor entity (fromId/toId). An unanchored query would
	// dump every tenant's relations, which is the leak we are closing — refuse
	// it. A SYS_ADMIN can inspect any anchor.
	anchorId, anchorType := fromId, fromType
	if anchorId == "" {
		anchorId, anchorType = toId, toType
	}
	if anchorId == "" {
		// No anchor entity: the UI polls this shape and expects an empty list,
		// not an error. Returning empty (rather than 400) keeps the contract
		// green without leaking anything — an unanchored query is simply not
		// answered with cross-tenant rows, it is answered with none.
		httputil.WriteJSON(w, http.StatusOK, []interface{}{})
		return
	}
	if !callerIsSysAdmin(claims) && !entityBelongsToTenant(anchorType, anchorId, tenantId) {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
		return
	}
	conds := []string{"relation_type_group = $1"}
	args := []interface{}{relationGroup}
	idx := 2
	if fromId != "" {
		conds = append(conds, "from_id = $"+strconv.Itoa(idx))
		args = append(args, fromId)
		idx++
	}
	if fromType != "" {
		conds = append(conds, "from_type = $"+strconv.Itoa(idx))
		args = append(args, fromType)
		idx++
	}
	if toId != "" {
		conds = append(conds, "to_id = $"+strconv.Itoa(idx))
		args = append(args, toId)
		idx++
	}
	if toType != "" {
		conds = append(conds, "to_type = $"+strconv.Itoa(idx))
		args = append(args, toType)
		idx++
	}
	if relationType != "" {
		conds = append(conds, "relation_type = $"+strconv.Itoa(idx))
		args = append(args, relationType)
		idx++
	}
	// Bounded result set: even scoped to one anchor entity, cap the dump so a
	// pathological relation graph cannot stream unbounded rows.
	query := "SELECT from_id, from_type, to_id, to_type, relation_type_group, relation_type, additional_info FROM relation WHERE " + strings.Join(conds, " AND ") + " LIMIT 10000"
	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		httputil.WriteJSON(w, http.StatusOK, []interface{}{})
		return
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var fid, ft, tid, tt, group, rtype string
		var ai *string
		if err := rows.Scan(&fid, &ft, &tid, &tt, &group, &rtype, &ai); err != nil {
			continue
		}
		fromName := lookupEntityName(ft, fid)
		toName := lookupEntityName(tt, tid)
		rel := map[string]interface{}{
			"from":      map[string]interface{}{"entityType": ft, "id": fid},
			"to":        map[string]interface{}{"entityType": tt, "id": tid},
			"type":      rtype,
			"typeGroup": group,
			"fromName":  fromName,
			"toName":    toName,
		}
		if ai != nil && *ai != "" {
			var aiv interface{}
			_ = json.Unmarshal([]byte(*ai), &aiv)
			rel["additionalInfo"] = aiv
		}
		out = append(out, rel)
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

// lookupEntityName tries to fetch the human name for an entity id+type.
// Returns empty string when the entity isn't found or the type is unsupported.
func lookupEntityName(entityType, entityId string) string {
	if dbpkg.Pool == nil || entityId == "" {
		return ""
	}
	var name string
	switch entityType {
	case "DEVICE":
		_ = dbpkg.Pool.QueryRow("SELECT name FROM device WHERE id = $1", entityId).Scan(&name)
	case "ASSET":
		_ = dbpkg.Pool.QueryRow("SELECT name FROM asset WHERE id = $1", entityId).Scan(&name)
	case "CUSTOMER":
		_ = dbpkg.Pool.QueryRow("SELECT title FROM customer WHERE id = $1", entityId).Scan(&name)
	case "DASHBOARD":
		_ = dbpkg.Pool.QueryRow("SELECT title FROM dashboard WHERE id = $1", entityId).Scan(&name)
	case "USER":
		_ = dbpkg.Pool.QueryRow("SELECT email FROM tb_user WHERE id = $1", entityId).Scan(&name)
	case "TENANT":
		_ = dbpkg.Pool.QueryRow("SELECT title FROM tenant WHERE id = $1", entityId).Scan(&name)
	case "ENTITY_VIEW":
		_ = dbpkg.Pool.QueryRow("SELECT name FROM entity_view WHERE id = $1", entityId).Scan(&name)
	}
	return name
}

// callerIsSysAdmin reports whether the JWT carries the SYS_ADMIN scope (mirrors
// httputil.RequireSysAdmin). A SYS_ADMIN reads across tenants; every other
// authority is confined to its own tenant.
func callerIsSysAdmin(claims map[string]interface{}) bool {
	scopes, _ := claims["scopes"].([]interface{})
	for _, s := range scopes {
		if str, ok := s.(string); ok && str == "SYS_ADMIN" {
			return true
		}
	}
	return false
}

// entityBelongsToTenant verifies that the given entity is owned by tenantId.
// Used to tenant-scope reads over tables that carry no tenant_id column of
// their own (e.g. `relation`) by checking the anchor entity instead. Unknown
// or missing entities return false (fail-closed).
func entityBelongsToTenant(entityType, entityId, tenantId string) bool {
	if dbpkg.Pool == nil || entityId == "" || tenantId == "" {
		return false
	}
	// A TENANT entity owns itself.
	if strings.EqualFold(entityType, "TENANT") {
		return entityId == tenantId
	}
	table := ""
	switch strings.ToUpper(entityType) {
	case "DEVICE":
		table = "device"
	case "ASSET":
		table = "asset"
	case "CUSTOMER":
		table = "customer"
	case "DASHBOARD":
		table = "dashboard"
	case "USER":
		table = "tb_user"
	case "ENTITY_VIEW":
		table = "entity_view"
	default:
		return false
	}
	var owner string
	if err := dbpkg.Pool.QueryRow(
		"SELECT COALESCE(tenant_id::text, '') FROM "+table+" WHERE id = $1", entityId,
	).Scan(&owner); err != nil {
		return false
	}
	return owner == tenantId
}

func EmptyPageData(w http.ResponseWriter) {
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          []interface{}{},
		"totalPages":    0,
		"totalElements": 0,
		"hasNext":       false,
	})
}

// HandleProfileInfoById serves /api/{device,asset}ProfileInfo/{id} —
// slim projection used by entity edit dialogs to fill the profile drop-
// down. `table` is the postgres table name; `entityType` the TB-style
// entityType in the response id object.
func HandleProfileInfoById(w http.ResponseWriter, r *http.Request, table, entityType, id string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	if !httputil.LooksLikeUUID(id) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid profile id")
		return
	}
	var name string
	var tenantId, image *string
	err := dbpkg.Pool.QueryRow(
		"SELECT name, tenant_id::text, image FROM "+table+" WHERE id = $1", id,
	).Scan(&name, &tenantId, &image)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Profile not found")
		return
	}
	tid := "13814000-1dd2-11b2-8080-808080808080"
	if tenantId != nil {
		tid = *tenantId
	}
	out := map[string]interface{}{
		"id":       map[string]interface{}{"entityType": entityType, "id": id},
		"name":     name,
		"tenantId": map[string]interface{}{"entityType": "TENANT", "id": tid},
	}
	if image != nil && *image != "" {
		out["image"] = *image
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}
