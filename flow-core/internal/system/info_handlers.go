package system

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// ─── Asset Infos ───────────────────────────────────────────────────────────────

// HandleTenantAssetInfos processes GET /api/tenant/assetInfos.
// Mirrors deviceInfos: enriches each row with customerTitle / customerIsPublic /
// assetProfileName so the UI's listing column renderers can populate.
func HandleTenantAssetInfos(w http.ResponseWriter, r *http.Request) {
	listAssetInfos(w, r, "")
}

// HandleCustomerAssetInfos — GET /api/customer/{customerId}/assetInfos. The
// device and dashboard equivalents already existed, so a customer page rendered
// its devices and left the assets column empty with no error.
func HandleCustomerAssetInfos(w http.ResponseWriter, r *http.Request, customerId string) {
	listAssetInfos(w, r, customerId)
}

// listAssetInfos backs both listings. The tenant predicate always applies, so a
// customer filter can only narrow the result.
func listAssetInfos(w http.ResponseWriter, r *http.Request, customerId string) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)

	where := "a.tenant_id = $1"
	args := []interface{}{tenantId}
	if customerId != "" {
		where += " AND a.customer_id = $2"
		args = append(args, customerId)
	}

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM asset a WHERE "+where, args...).Scan(&total)

	offset := page * pageSize
	rows, err := dbpkg.Pool.Query(`
		SELECT a.id, a.created_time, a.name, a.label, a.type, a.customer_id,
		       a.asset_profile_id, a.additional_info, a.external_id, a.version,
		       ap.name AS profile_name,
		       c.title AS customer_title, COALESCE(c.is_public, false) AS customer_is_public
		FROM asset a
		LEFT JOIN asset_profile ap ON ap.id = a.asset_profile_id
		LEFT JOIN customer c ON c.id = a.customer_id
		WHERE `+where+` ORDER BY a.created_time DESC LIMIT $`+
		strconv.Itoa(len(args)+1)+` OFFSET $`+strconv.Itoa(len(args)+2),
		append(args, pageSize, offset)...)
	if err != nil {
		log.Printf("ERROR querying assetInfos: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	nilUUID := "13814000-1dd2-11b2-8080-808080808080"
	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, assetType string
		var createdTime int64
		var label, customerIdStr, assetProfileId, additionalInfo *string
		var externalId, customerTitle, profileName *string
		var customerIsPublic *bool
		var version *int64

		if err := rows.Scan(&id, &createdTime, &name, &label, &assetType, &customerIdStr,
			&assetProfileId, &additionalInfo, &externalId, &version,
			&profileName, &customerTitle, &customerIsPublic); err != nil {
			continue
		}

		custId := nilUUID
		if customerIdStr != nil && *customerIdStr != "" {
			custId = *customerIdStr
		}
		item := map[string]interface{}{
			"id":               map[string]interface{}{"entityType": "ASSET", "id": id},
			"createdTime":      createdTime,
			"tenantId":         map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"customerId":       map[string]interface{}{"entityType": "CUSTOMER", "id": custId},
			"name":             name,
			"type":             assetType,
			"customerIsPublic": customerIsPublic != nil && *customerIsPublic,
		}
		if label != nil {
			item["label"] = *label
		} else {
			item["label"] = nil
		}
		if assetProfileId != nil {
			item["assetProfileId"] = map[string]interface{}{"entityType": "ASSET_PROFILE", "id": *assetProfileId}
		}
		if profileName != nil {
			item["assetProfileName"] = *profileName
		} else {
			item["assetProfileName"] = nil
		}
		if customerTitle != nil {
			item["customerTitle"] = *customerTitle
		} else {
			item["customerTitle"] = nil
		}
		if externalId != nil && *externalId != "" {
			item["externalId"] = map[string]interface{}{"entityType": "ASSET", "id": *externalId}
		} else {
			item["externalId"] = nil
		}
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
		data = append(data, item)
	}

	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	if totalPages == 0 && total > 0 {
		totalPages = 1
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page + 1) < totalPages,
	})
}

// ─── Entity View Infos ─────────────────────────────────────────────────────────

func HandleTenantEntityViewInfos(w http.ResponseWriter, r *http.Request) {
	listEntityViewInfos(w, r, "")
}

// HandleCustomerEntityViewInfos — GET /api/customer/{customerId}/entityViewInfos.
func HandleCustomerEntityViewInfos(w http.ResponseWriter, r *http.Request, customerId string) {
	listEntityViewInfos(w, r, customerId)
}

func listEntityViewInfos(w http.ResponseWriter, r *http.Request, customerId string) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)

	where := "ev.tenant_id = $1"
	args := []interface{}{tenantId}
	if customerId != "" {
		where += " AND ev.customer_id = $2"
		args = append(args, customerId)
	}

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM entity_view ev WHERE "+where, args...).Scan(&total)

	offset := page * pageSize
	rows, err := dbpkg.Pool.Query(`
		SELECT ev.id, ev.created_time, ev.name, ev.type, ev.entity_id, ev.entity_type,
		       ev.customer_id, ev.start_ts, ev.end_ts, ev.additional_info,
		       c.title AS customer_title, COALESCE(c.is_public, false) AS customer_is_public
		FROM entity_view ev
		LEFT JOIN customer c ON c.id = ev.customer_id
		WHERE `+where+` ORDER BY ev.created_time DESC LIMIT $`+
		strconv.Itoa(len(args)+1)+` OFFSET $`+strconv.Itoa(len(args)+2),
		append(args, pageSize, offset)...)
	if err != nil {
		log.Printf("ERROR querying entityViewInfos: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	nilUUID := "13814000-1dd2-11b2-8080-808080808080"
	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, viewType, entityId, entityType string
		var createdTime, startTs, endTs int64
		var customerIdStr, additionalInfo, customerTitle *string
		var customerIsPublic *bool

		if err := rows.Scan(&id, &createdTime, &name, &viewType, &entityId, &entityType,
			&customerIdStr, &startTs, &endTs, &additionalInfo,
			&customerTitle, &customerIsPublic); err != nil {
			continue
		}

		custId := nilUUID
		if customerIdStr != nil && *customerIdStr != "" {
			custId = *customerIdStr
		}
		item := map[string]interface{}{
			"id":               map[string]interface{}{"entityType": "ENTITY_VIEW", "id": id},
			"createdTime":      createdTime,
			"tenantId":         map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"customerId":       map[string]interface{}{"entityType": "CUSTOMER", "id": custId},
			"entityId":         map[string]interface{}{"entityType": entityType, "id": entityId},
			"name":             name,
			"type":             viewType,
			"startTs":          startTs,
			"endTs":            endTs,
			"customerIsPublic": customerIsPublic != nil && *customerIsPublic,
		}
		if customerTitle != nil {
			item["customerTitle"] = *customerTitle
		} else {
			item["customerTitle"] = nil
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
		data = append(data, item)
	}

	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	if totalPages == 0 && total > 0 {
		totalPages = 1
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page + 1) < totalPages,
	})
}

// ─── Profile Infos (compact list for dropdowns) ────────────────────────────────

// HandleDeviceProfileInfos GET /api/deviceProfileInfos
// TB returns a lightweight list (id, name, type, transportType, image, tenantId) for selectors.
func HandleDeviceProfileInfos(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	pageSize := httputil.IntParam(r, "pageSize", 100)
	page := httputil.IntParam(r, "page", 0)

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM device_profile WHERE tenant_id = $1", tenantId).Scan(&total)

	offset := page * pageSize
	rows, err := dbpkg.Pool.Query(`
		SELECT id, name, type, transport_type, image
		FROM device_profile WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`,
		tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name string
		var profileType, transportType, image *string
		if err := rows.Scan(&id, &name, &profileType, &transportType, &image); err != nil {
			continue
		}
		item := map[string]interface{}{
			"id":            map[string]interface{}{"entityType": "DEVICE_PROFILE", "id": id},
			"tenantId":      map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"name":          name,
			"type":          httputil.CoalesceStr(profileType, "DEFAULT"),
			"transportType": httputil.CoalesceStr(transportType, "DEFAULT"),
		}
		if image != nil {
			item["image"] = *image
		} else {
			item["image"] = nil
		}
		// defaultDashboardId is shown in selectors next to the profile name
		var ddId *string
		_ = dbpkg.Pool.QueryRow("SELECT default_dashboard_id::text FROM device_profile WHERE id = $1", id).Scan(&ddId)
		if ddId != nil && *ddId != "" {
			item["defaultDashboardId"] = map[string]interface{}{"entityType": "DASHBOARD", "id": *ddId}
		} else {
			item["defaultDashboardId"] = nil
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

// HandleAssetProfileInfos GET /api/assetProfileInfos
func HandleAssetProfileInfos(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	pageSize := httputil.IntParam(r, "pageSize", 100)
	page := httputil.IntParam(r, "page", 0)

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM asset_profile WHERE tenant_id = $1", tenantId).Scan(&total)

	offset := page * pageSize
	rows, err := dbpkg.Pool.Query(`
		SELECT id, name, image, default_dashboard_id::text
		FROM asset_profile WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`,
		tenantId, pageSize, offset)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name string
		var image, defaultDashboardId *string
		if err := rows.Scan(&id, &name, &image, &defaultDashboardId); err != nil {
			continue
		}
		item := map[string]interface{}{
			"id":       map[string]interface{}{"entityType": "ASSET_PROFILE", "id": id},
			"tenantId": map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"name":     name,
		}
		if image != nil {
			item["image"] = *image
		} else {
			item["image"] = nil
		}
		if defaultDashboardId != nil && *defaultDashboardId != "" {
			item["defaultDashboardId"] = map[string]interface{}{"entityType": "DASHBOARD", "id": *defaultDashboardId}
		} else {
			item["defaultDashboardId"] = nil
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

// ─── Profile Names (used by selectors / inheritance dropdowns) ────────────────

// HandleDeviceProfileNames GET /api/deviceProfile/names
// Returns a flat array of {id, name} — NOT a PageData.
func HandleDeviceProfileNames(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	rows, err := dbpkg.Pool.Query(
		"SELECT id, name FROM device_profile WHERE tenant_id = $1 ORDER BY name",
		tenantId,
	)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"id":   map[string]interface{}{"entityType": "DEVICE_PROFILE", "id": id},
			"name": name,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// HandleAssetProfileNames GET /api/assetProfile/names
func HandleAssetProfileNames(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	rows, err := dbpkg.Pool.Query(
		"SELECT id, name FROM asset_profile WHERE tenant_id = $1 ORDER BY name",
		tenantId,
	)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"id":   map[string]interface{}{"entityType": "ASSET_PROFILE", "id": id},
			"name": name,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
