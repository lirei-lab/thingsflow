package system

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// ─── Device Profiles ─────────────────────────────────────────────────────────

// deviceProfileRow holds the columns both device-profile reads select, so the TB-shaped
// response is built once and stays identical between the list and the by-id handler.
type deviceProfileRow struct {
	ID                 string
	CreatedTime        int64
	Name               string
	Description        *string
	IsDefault          bool
	TenantID           *string
	Version            *int64
	Type               *string
	TransportType      *string
	ProvisionType      string
	ProvisionDeviceKey *string
	ProfileData        []byte
}

// deviceProfileSelectColumns is the shared column list for deviceProfileRow scans.
const deviceProfileSelectColumns = `id, created_time, name, description, is_default, tenant_id, version,
	       type, transport_type, COALESCE(provision_type, 'DISABLED'), provision_device_key, profile_data`

// scanTargets must use a pointer receiver: a value receiver would hand Scan pointers into a
// copy and the scanned values would be silently discarded.
func (p *deviceProfileRow) scanTargets() []interface{} {
	return []interface{}{&p.ID, &p.CreatedTime, &p.Name, &p.Description, &p.IsDefault, &p.TenantID,
		&p.Version, &p.Type, &p.TransportType, &p.ProvisionType, &p.ProvisionDeviceKey, &p.ProfileData}
}

// toTBProfile renders the ThingsBoard device-profile shape. Previously `type` and
// `transportType` were hardcoded "DEFAULT" and profileData was omitted entirely, so the UI's
// provisioning/transport tabs could not render the saved configuration (a saved provisioning
// strategy+key looked blank on reload). Return the REAL stored values and inject
// profileData.provisionConfiguration from the provisioning columns. The secret is never
// returned — only its bcrypt hash is stored, and TB masks it the same way.
func (p deviceProfileRow) toTBProfile() map[string]interface{} {
	tid := "13814000-1dd2-11b2-8080-808080808080"
	if p.TenantID != nil {
		tid = *p.TenantID
	}
	profileType := "DEFAULT"
	if p.Type != nil && strings.TrimSpace(*p.Type) != "" {
		profileType = *p.Type
	}
	transportType := "DEFAULT"
	if p.TransportType != nil && strings.TrimSpace(*p.TransportType) != "" {
		transportType = *p.TransportType
	}

	profileData := map[string]interface{}{}
	if len(p.ProfileData) > 0 {
		_ = json.Unmarshal(p.ProfileData, &profileData)
	}
	// The UI dereferences profileData.configuration.type / transportConfiguration.type when
	// rendering the profile tabs, so guarantee both objects exist.
	if _, ok := profileData["configuration"].(map[string]interface{}); !ok {
		profileData["configuration"] = map[string]interface{}{"type": profileType}
	}
	if _, ok := profileData["transportConfiguration"].(map[string]interface{}); !ok {
		profileData["transportConfiguration"] = map[string]interface{}{"type": transportType}
	}
	provisionConfiguration := map[string]interface{}{"type": p.ProvisionType}
	if p.ProvisionDeviceKey != nil && *p.ProvisionDeviceKey != "" {
		provisionConfiguration["provisionDeviceKey"] = *p.ProvisionDeviceKey
	}
	profileData["provisionConfiguration"] = provisionConfiguration

	out := map[string]interface{}{
		"id":            map[string]interface{}{"entityType": "DEVICE_PROFILE", "id": p.ID},
		"createdTime":   p.CreatedTime,
		"tenantId":      map[string]interface{}{"entityType": "TENANT", "id": tid},
		"name":          p.Name,
		"default":       p.IsDefault,
		"type":          profileType,
		"transportType": transportType,
		"provisionType": p.ProvisionType,
		"profileData":   profileData,
	}
	if p.ProvisionDeviceKey != nil {
		out["provisionDeviceKey"] = *p.ProvisionDeviceKey
	}
	if p.Description != nil {
		out["description"] = *p.Description
	}
	if p.Version != nil {
		out["version"] = *p.Version
	}
	return out
}

func HandleTenantDeviceProfiles(w http.ResponseWriter, r *http.Request) {
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
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize

	// Scope to the caller's tenant: this projection includes provision_device_key,
	// so a tenant-blind list leaks every tenant's device provisioning secret.
	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM device_profile WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`SELECT `+deviceProfileSelectColumns+`
		FROM device_profile WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var p deviceProfileRow
		if err := rows.Scan(p.scanTargets()...); err != nil {
			continue
		}
		data = append(data, p.toTBProfile())
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

func HandleDeviceProfileById(w http.ResponseWriter, r *http.Request, id string) {
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
	// tenant_id predicate closes the IDOR: without it any authenticated tenant
	// could fetch another tenant's device profile (incl. provision_device_key)
	// by its UUID.
	var p deviceProfileRow
	err = dbpkg.Pool.QueryRow(`SELECT `+deviceProfileSelectColumns+`
		FROM device_profile WHERE id = $1 AND tenant_id = $2`, id, tenantId).Scan(p.scanTargets()...)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device profile not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(p.toTBProfile())
}

func HandleDefaultDeviceProfileInfo(w http.ResponseWriter, r *http.Request) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	var id, name string
	var createdTime int64
	err = dbpkg.Pool.QueryRow(`SELECT id, created_time, name FROM device_profile WHERE is_default = true LIMIT 1`).
		Scan(&id, &createdTime, &name)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Default device profile not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":            map[string]interface{}{"entityType": "DEVICE_PROFILE", "id": id},
		"createdTime":   createdTime,
		"name":          name,
		"type":          "DEFAULT",
		"transportType": "DEFAULT",
	})
}

// ─── Asset Profiles ───────────────────────────────────────────────────────────

func HandleTenantAssetProfiles(w http.ResponseWriter, r *http.Request) {
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
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize

	// Scope to the caller's tenant — the sibling info handlers already do.
	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM asset_profile WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, name, description, is_default, tenant_id
		FROM asset_profile WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name string
		var createdTime int64
		var isDefault bool
		var desc, tenantId *string
		if err := rows.Scan(&id, &createdTime, &name, &desc, &isDefault, &tenantId); err != nil {
			continue
		}
		tid := "13814000-1dd2-11b2-8080-808080808080"
		if tenantId != nil {
			tid = *tenantId
		}
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "ASSET_PROFILE", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"name":        name,
			"default":     isDefault,
		}
		if desc != nil {
			item["description"] = *desc
		}
		data = append(data, item)
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

// ─── Assets ───────────────────────────────────────────────────────────────────

func HandleTenantAssets(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize
	tenantId, _ := claims["tenantId"].(string)

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM asset WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, name, label, type, tenant_id, customer_id
		FROM asset WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`,
		tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, assetType, tid string
		var createdTime int64
		var label, customerId *string
		if err := rows.Scan(&id, &createdTime, &name, &label, &assetType, &tid, &customerId); err != nil {
			continue
		}
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "ASSET", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"customerId":  map[string]interface{}{"entityType": "CUSTOMER", "id": "13814000-1dd2-11b2-8080-808080808080"},
			"name":        name,
			"type":        assetType,
		}
		if label != nil {
			item["label"] = *label
		}
		if customerId != nil {
			item["customerId"] = map[string]interface{}{"entityType": "CUSTOMER", "id": *customerId}
		}
		data = append(data, item)
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

// ─── Entity Views ─────────────────────────────────────────────────────────────

func HandleTenantEntityViews(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize
	tenantId, _ := claims["tenantId"].(string)

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM entity_view WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, name, type, entity_id, entity_type, tenant_id
		FROM entity_view WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`,
		tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, viewType, entityId, entityType, tid string
		var createdTime int64
		if err := rows.Scan(&id, &createdTime, &name, &viewType, &entityId, &entityType, &tid); err != nil {
			continue
		}
		data = append(data, map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "ENTITY_VIEW", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"entityId":    map[string]interface{}{"entityType": entityType, "id": entityId},
			"name":        name,
			"type":        viewType,
		})
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

// ─── Customers ────────────────────────────────────────────────────────────────

func HandleTenantCustomers(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize
	tenantId, _ := claims["tenantId"].(string)

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM customer WHERE tenant_id = $1 AND title != 'Public'", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, title, email, phone, country, state, city,
		       address, address2, zip, additional_info, external_id, version, tenant_id
		FROM customer WHERE tenant_id = $1 AND title != 'Public'
		ORDER BY created_time DESC LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, title, tid string
		var createdTime int64
		var email, phone, country, state, city, address, address2, zip, additionalInfo, externalId *string
		var version *int64
		if err := rows.Scan(&id, &createdTime, &title, &email, &phone, &country, &state, &city,
			&address, &address2, &zip, &additionalInfo, &externalId, &version, &tid); err != nil {
			continue
		}
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "CUSTOMER", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"title":       title,
			"name":        title,
		}
		httputil.SetOptional(item, "email", email)
		httputil.SetOptional(item, "phone", phone)
		httputil.SetOptional(item, "country", country)
		httputil.SetOptional(item, "state", state)
		httputil.SetOptional(item, "city", city)
		httputil.SetOptional(item, "address", address)
		httputil.SetOptional(item, "address2", address2)
		httputil.SetOptional(item, "zip", zip)
		if version != nil {
			item["version"] = *version
		} else {
			item["version"] = 1
		}
		if additionalInfo != nil && *additionalInfo != "" {
			var info interface{}
			if err := json.Unmarshal([]byte(*additionalInfo), &info); err == nil {
				item["additionalInfo"] = info
			} else {
				item["additionalInfo"] = nil
			}
		} else {
			item["additionalInfo"] = nil
		}
		if externalId != nil && *externalId != "" {
			item["externalId"] = map[string]interface{}{"entityType": "CUSTOMER", "id": *externalId}
		} else {
			item["externalId"] = nil
		}
		data = append(data, item)
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	if totalPages == 0 && total > 0 {
		totalPages = 1
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

// ─── Alarm Types ──────────────────────────────────────────────────────────────

func HandleAlarmTypes(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	pageSize := httputil.IntParam(r, "pageSize", 100)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize

	rows, err := dbpkg.Pool.Query(
		`SELECT DISTINCT type FROM alarm WHERE tenant_id = $1 ORDER BY type LIMIT $2 OFFSET $3`,
		tenantId, pageSize, offset,
	)
	types := []map[string]interface{}{}
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err == nil {
				types = append(types, map[string]interface{}{
					"tenantId": map[string]interface{}{"entityType": "TENANT", "id": tenantId},
					"type":     t,
				})
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          types,
		"totalPages":    1,
		"totalElements": len(types),
		"hasNext":       false,
	})
}

// ─── Tenant Profile ───────────────────────────────────────────────────────────

func HandleTenantProfile(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	// Return the tenant's profile info
	var id, title string
	var createdTime int64
	var email, phone, country, city *string
	err = dbpkg.Pool.QueryRow(`SELECT id, created_time, title, email, phone, country, city FROM tenant WHERE id = $1`, tenantId).
		Scan(&id, &createdTime, &title, &email, &phone, &country, &city)
	if err != nil {
		// Return minimal tenant profile if not found
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":              map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"tenantProfileId": map[string]interface{}{"entityType": "TENANT_PROFILE", "id": ""},
			"createdTime":     time.Now().UnixMilli(),
			"title":           "Tenant",
			"region":          "Global",
			"additionalInfo":  map[string]interface{}{},
		})
		return
	}

	result := map[string]interface{}{
		"id":             map[string]interface{}{"entityType": "TENANT", "id": id},
		"createdTime":    createdTime,
		"title":          title,
		"region":         "Global",
		"additionalInfo": map[string]interface{}{},
	}
	if email != nil {
		result["email"] = *email
	}
	if phone != nil {
		result["phone"] = *phone
	}
	if country != nil {
		result["country"] = *country
	}
	if city != nil {
		result["city"] = *city
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// ─── User Token Access Enabled ────────────────────────────────────────────────

func HandleUserTokenAccessEnabled(w http.ResponseWriter, r *http.Request) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(true)
}

// ─── Usage Stats ──────────────────────────────────────────────────────────────

func HandleUsage(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var devices, assets, customers, dashboards, users, alarms int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM device WHERE tenant_id = $1", tenantId).Scan(&devices)
	dbpkg.Pool.QueryRow("SELECT count(*) FROM asset WHERE tenant_id = $1", tenantId).Scan(&assets)
	dbpkg.Pool.QueryRow("SELECT count(*) FROM customer WHERE tenant_id = $1 AND title != 'Public'", tenantId).Scan(&customers)
	dbpkg.Pool.QueryRow("SELECT count(*) FROM dashboard WHERE tenant_id = $1", tenantId).Scan(&dashboards)
	dbpkg.Pool.QueryRow("SELECT count(*) FROM tb_user WHERE tenant_id = $1", tenantId).Scan(&users)
	dbpkg.Pool.QueryRow("SELECT count(*) FROM alarm WHERE tenant_id = $1", tenantId).Scan(&alarms)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"devices":              devices,
		"maxDevices":           0,
		"assets":               assets,
		"maxAssets":            0,
		"customers":            customers,
		"maxCustomers":         0,
		"users":                users,
		"maxUsers":             0,
		"dashboards":           dashboards,
		"maxDashboards":        0,
		"edges":                0,
		"maxEdges":             0,
		"transportMessages":    0,
		"maxTransportMessages": 0,
		"jsExecutions":         0,
		"tbelExecutions":       0,
		"maxJsExecutions":      0,
		"maxTbelExecutions":    0,
		"emails":               0,
		"maxEmails":            0,
		"sms":                  0,
		"maxSms":               0,
		"smsEnabled":           nil,
		"alarms":               alarms,
		"maxAlarms":            0,
	})
}

// ─── Entity Count (for WS entitiesQuery/count) ────────────────────────────────

func HandleEntitiesQueryCount(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	entityFilter, _ := body["entityFilter"].(map[string]interface{})
	filterType, _ := entityFilter["type"].(string)
	entityType, _ := entityFilter["entityType"].(string)

	var count int
	switch strings.ToLower(entityType) {
	case "device":
		dbpkg.Pool.QueryRow("SELECT count(*) FROM device WHERE tenant_id = $1", tenantId).Scan(&count)
	case "asset":
		dbpkg.Pool.QueryRow("SELECT count(*) FROM asset WHERE tenant_id = $1", tenantId).Scan(&count)
	case "customer":
		dbpkg.Pool.QueryRow("SELECT count(*) FROM customer WHERE tenant_id = $1 AND title != 'Public'", tenantId).Scan(&count)
	case "dashboard":
		dbpkg.Pool.QueryRow("SELECT count(*) FROM dashboard WHERE tenant_id = $1", tenantId).Scan(&count)
	case "user":
		dbpkg.Pool.QueryRow("SELECT count(*) FROM tb_user WHERE tenant_id = $1", tenantId).Scan(&count)
	default:
		log.Printf("WARN entitiesQuery/count: unknown entityType=%s filterType=%s", entityType, filterType)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(count)
}

// ─── Rule Chains ──────────────────────────────────────────────────────────────

func HandleRuleChains(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize
	tenantId, _ := claims["tenantId"].(string)

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM rule_chain WHERE tenant_id = $1", tenantId).Scan(&total)

	rows, err := dbpkg.Pool.Query(`SELECT id, created_time, name, root, tenant_id, debug_mode,
		first_rule_node_id, configuration, additional_info, external_id, version, type
		FROM rule_chain WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`, tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, tid string
		var createdTime int64
		var root bool
		var debugMode *bool
		var firstRuleNodeId, configuration, additionalInfo, externalId, rcType *string
		var version *int64
		if err := rows.Scan(&id, &createdTime, &name, &root, &tid, &debugMode,
			&firstRuleNodeId, &configuration, &additionalInfo, &externalId, &version, &rcType); err != nil {
			continue
		}
		typeVal := "CORE"
		if rcType != nil && *rcType != "" {
			typeVal = *rcType
		}
		debug := false
		if debugMode != nil {
			debug = *debugMode
		}
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "RULE_CHAIN", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"name":        name,
			"root":        root,
			"type":        typeVal,
			"debugMode":   debug,
		}
		if firstRuleNodeId != nil && *firstRuleNodeId != "" {
			item["firstRuleNodeId"] = map[string]interface{}{"entityType": "RULE_NODE", "id": *firstRuleNodeId}
		} else {
			item["firstRuleNodeId"] = nil
		}
		if configuration != nil && *configuration != "" {
			var cfg interface{}
			if err := json.Unmarshal([]byte(*configuration), &cfg); err == nil {
				item["configuration"] = cfg
			} else {
				item["configuration"] = nil
			}
		} else {
			item["configuration"] = nil
		}
		if additionalInfo != nil && *additionalInfo != "" {
			var ai interface{}
			if err := json.Unmarshal([]byte(*additionalInfo), &ai); err == nil {
				item["additionalInfo"] = ai
			} else {
				item["additionalInfo"] = nil
			}
		} else {
			item["additionalInfo"] = nil
		}
		if externalId != nil && *externalId != "" {
			item["externalId"] = map[string]interface{}{"entityType": "RULE_CHAIN", "id": *externalId}
		} else {
			item["externalId"] = nil
		}
		if version != nil {
			item["version"] = *version
		} else {
			item["version"] = 1
		}
		data = append(data, item)
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

// ruleChainRow projects a rule_chain row into the TB JSON envelope. Shared
// by HandleRuleChains (list) and HandleRuleChainByID (detail).
func ruleChainRow(id, name, tid string, createdTime int64, root bool,
	debugMode *bool, firstRuleNodeId, configuration, additionalInfo, externalId, rcType *string,
	version *int64,
) map[string]interface{} {
	typeVal := "CORE"
	if rcType != nil && *rcType != "" {
		typeVal = *rcType
	}
	debug := false
	if debugMode != nil {
		debug = *debugMode
	}
	item := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "RULE_CHAIN", "id": id},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
		"name":        name,
		"root":        root,
		"type":        typeVal,
		"debugMode":   debug,
	}
	if firstRuleNodeId != nil && *firstRuleNodeId != "" {
		item["firstRuleNodeId"] = map[string]interface{}{"entityType": "RULE_NODE", "id": *firstRuleNodeId}
	} else {
		item["firstRuleNodeId"] = nil
	}
	if configuration != nil && *configuration != "" {
		var c interface{}
		if err := json.Unmarshal([]byte(*configuration), &c); err == nil {
			item["configuration"] = c
		}
	} else {
		item["configuration"] = nil
	}
	if additionalInfo != nil && *additionalInfo != "" {
		var ai interface{}
		if err := json.Unmarshal([]byte(*additionalInfo), &ai); err == nil {
			item["additionalInfo"] = ai
		}
	} else {
		item["additionalInfo"] = nil
	}
	if externalId != nil && *externalId != "" {
		item["externalId"] = map[string]interface{}{"entityType": "RULE_CHAIN", "id": *externalId}
	} else {
		item["externalId"] = nil
	}
	if version != nil {
		item["version"] = *version
	} else {
		item["version"] = 1
	}
	return item
}

// HandleRuleChainByID — GET /api/ruleChain/{id}.
// Required by the UI to open the rule chain editor canvas.
func HandleRuleChainByID(w http.ResponseWriter, r *http.Request, id string) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var name, tid string
	var createdTime int64
	var root bool
	var debugMode *bool
	var firstRuleNodeId, configuration, additionalInfo, externalId, rcType *string
	var version *int64

	if err := dbpkg.Pool.QueryRow(`
		SELECT created_time, name, root, tenant_id, debug_mode,
		       first_rule_node_id::text, configuration, additional_info, external_id::text, version, type
		FROM rule_chain WHERE id = $1 AND tenant_id = $2`,
		id, tenantId,
	).Scan(&createdTime, &name, &root, &tid, &debugMode,
		&firstRuleNodeId, &configuration, &additionalInfo, &externalId, &version, &rcType); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Rule chain not found")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, ruleChainRow(id, name, tid, createdTime, root,
		debugMode, firstRuleNodeId, configuration, additionalInfo, externalId, rcType, version))
}

// HandleRuleChainMetaData — GET /api/ruleChain/{id}/metaData.
// Returns the canvas-shaped {firstNodeIndex, nodes, connections, ruleChainConnections}
// payload the UI needs to render the rule chain editor. Connections are
// derived from a hypothetical relation table — for the single-node Flow
// bridge chain we ship, connections is empty.
func HandleRuleChainMetaData(w http.ResponseWriter, r *http.Request, id string) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var owner string
	if err := dbpkg.Pool.QueryRow(
		`SELECT tenant_id::text FROM rule_chain WHERE id = $1`, id,
	).Scan(&owner); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Rule chain not found")
		return
	}
	if owner != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
		return
	}

	rows, err := dbpkg.Pool.Query(`
		SELECT id::text, created_time, additional_info, configuration_version,
		       configuration, type, name, debug_settings, COALESCE(singleton_mode, false), queue_name, external_id::text
		FROM rule_node WHERE rule_chain_id = $1
		ORDER BY created_time`, id)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	nodes := []map[string]interface{}{}
	firstNodeIndex := 0
	idx := 0
	var firstNodeID string
	_ = dbpkg.Pool.QueryRow(`SELECT first_rule_node_id::text FROM rule_chain WHERE id = $1`, id).Scan(&firstNodeID)

	for rows.Next() {
		var nID, nType, nName string
		var createdTime int64
		var configurationVersion int
		var additionalInfo, configuration, debugSettings, queueName, externalId *string
		var singletonMode bool
		if err := rows.Scan(&nID, &createdTime, &additionalInfo, &configurationVersion,
			&configuration, &nType, &nName, &debugSettings, &singletonMode, &queueName, &externalId); err != nil {
			continue
		}
		node := map[string]interface{}{
			"id":                   map[string]interface{}{"entityType": "RULE_NODE", "id": nID},
			"createdTime":          createdTime,
			"type":                 nType,
			"name":                 nName,
			"configurationVersion": configurationVersion,
			"singletonMode":        singletonMode,
		}
		if additionalInfo != nil && *additionalInfo != "" {
			var ai interface{}
			if err := json.Unmarshal([]byte(*additionalInfo), &ai); err == nil {
				node["additionalInfo"] = ai
			}
		} else {
			node["additionalInfo"] = nil
		}
		if configuration != nil && *configuration != "" {
			var c interface{}
			if err := json.Unmarshal([]byte(*configuration), &c); err == nil {
				node["configuration"] = c
			}
		} else {
			node["configuration"] = nil
		}
		if nID == firstNodeID {
			firstNodeIndex = idx
		}
		nodes = append(nodes, node)
		idx++
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"ruleChainId":          map[string]interface{}{"entityType": "RULE_CHAIN", "id": id},
		"firstNodeIndex":       firstNodeIndex,
		"nodes":                nodes,
		"connections":          []interface{}{},
		"ruleChainConnections": nil,
	})
}

// ─── Rule node component descriptors ─────────────────────────────────────────

// HandleRuleNodeComponents keeps legacy ThingsBoard rule-chain screens from
// failing hard, but ThingsFlow does not ship rule-chain execution in flow-core.
// Alarm detection is handled by Bento over NATS and materialized back into the
// dashboard-compatible alarm tables.
func HandleRuleNodeComponents(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, []map[string]interface{}{})
}

// ─── Notification Rules ───────────────────────────────────────────────────────

func HandleNotificationRules(w http.ResponseWriter, r *http.Request) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": []interface{}{}, "totalPages": 0, "totalElements": 0, "hasNext": false,
	})
}

// HandleUserSettingsByKey processes GET /api/user/settings/{KEY}.
// TB returns an empty object {} when the user hasn't customized that key. The UI
// then applies its built-in defaults; injecting placeholder values from the
// server breaks the UI's own defaults and is what surfaced as "200 OK" toasts.
func HandleUserSettingsByKey(w http.ResponseWriter, r *http.Request, key string) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	userId, _ := claims["userId"].(string)

	w.Header().Set("Content-Type", "application/json")
	if dbpkg.Pool == nil || userId == "" {
		w.Write([]byte("{}"))
		return
	}

	var settingsJSON string
	err = dbpkg.Pool.QueryRow(
		"SELECT settings::text FROM user_settings WHERE user_id = $1 AND type = $2",
		userId, key,
	).Scan(&settingsJSON)
	if err != nil || settingsJSON == "" {
		w.Write([]byte("{}"))
		return
	}
	w.Write([]byte(settingsJSON))
}

// ─── Notifications ────────────────────────────────────────────────────────────

func HandleNotificationRequests(w http.ResponseWriter, r *http.Request) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": []interface{}{}, "totalPages": 0, "totalElements": 0, "hasNext": false,
	})
}

// ─── Users list ───────────────────────────────────────────────────────────────

func HandleUsersList(w http.ResponseWriter, r *http.Request) {
	listUsers(w, r, "")
}

// HandleCustomerUsers — GET /api/customer/{customerId}/users. The users who can
// log in and see only that customer's slice of the fleet.
func HandleCustomerUsers(w http.ResponseWriter, r *http.Request, customerId string) {
	listUsers(w, r, customerId)
}

// HandleAssignableUsers — GET /api/users/assign/{alarmId}, the picker the UI
// shows when assigning an alarm. Every user in the tenant is a valid assignee,
// so the alarm id only scopes the question, not the result set.
func HandleAssignableUsers(w http.ResponseWriter, r *http.Request) {
	listUsers(w, r, "")
}

// listUsers backs the tenant and customer user listings. customerId == "" means
// the whole tenant; the tenant predicate always applies.
func listUsers(w http.ResponseWriter, r *http.Request, customerId string) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize
	tenantId, _ := claims["tenantId"].(string)

	where := "tenant_id = $1"
	filterArgs := []interface{}{tenantId}
	if customerId != "" {
		where += " AND customer_id = $2"
		filterArgs = append(filterArgs, customerId)
	}

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM tb_user WHERE "+where, filterArgs...).Scan(&total)

	queryArgs := append(append([]interface{}{}, filterArgs...), pageSize, offset)
	rows, err := dbpkg.Pool.Query(`SELECT id, created_time, email, authority, tenant_id, customer_id,
		first_name, last_name, phone, additional_info, version
		FROM tb_user WHERE `+where+
		` ORDER BY email LIMIT $`+strconv.Itoa(len(filterArgs)+1)+
		` OFFSET $`+strconv.Itoa(len(filterArgs)+2), queryArgs...)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, email, authority, tid string
		var createdTime int64
		var customerIdStr, firstName, lastName, phone, additionalInfo *string
		var version *int64
		if err := rows.Scan(&id, &createdTime, &email, &authority, &tid, &customerIdStr,
			&firstName, &lastName, &phone, &additionalInfo, &version); err != nil {
			continue
		}
		nilUUID := "13814000-1dd2-11b2-8080-808080808080"
		custId := nilUUID
		if customerIdStr != nil && *customerIdStr != "" {
			custId = *customerIdStr
		}
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "USER", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"customerId":  map[string]interface{}{"entityType": "CUSTOMER", "id": custId},
			"email":       email,
			"authority":   authority,
			"name":        email,
		}
		if firstName != nil {
			item["firstName"] = *firstName
		} else {
			item["firstName"] = nil
		}
		if lastName != nil {
			item["lastName"] = *lastName
		} else {
			item["lastName"] = nil
		}
		if phone != nil {
			item["phone"] = *phone
		} else {
			item["phone"] = nil
		}
		if additionalInfo != nil && *additionalInfo != "" {
			var info interface{}
			if err := json.Unmarshal([]byte(*additionalInfo), &info); err == nil {
				item["additionalInfo"] = info
			} else {
				item["additionalInfo"] = nil
			}
		} else {
			item["additionalInfo"] = nil
		}
		if version != nil {
			item["version"] = *version
		} else {
			item["version"] = 1
		}
		data = append(data, item)
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}
