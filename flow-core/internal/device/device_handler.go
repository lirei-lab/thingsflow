package device

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
	"flow-core/internal/deviceactivity"
	"flow-core/internal/httputil"
)

// HandleDeviceById processes GET /api/device/{id}
func HandleDeviceById(w http.ResponseWriter, r *http.Request, deviceId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	device, err := queryDevice(deviceId, tenantId)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(device)
}

// HandleDeviceInfoById processes GET /api/device/info/{id}
func HandleDeviceInfoById(w http.ResponseWriter, r *http.Request, deviceId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	device, err := queryDeviceInfo(deviceId, tenantId)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(device)
}

// HandleTenantDeviceInfos processes GET /api/tenant/deviceInfos (paginated)
func HandleTenantDeviceInfos(w http.ResponseWriter, r *http.Request) {
	listDeviceInfos(w, r, "")
}

// HandleCustomerDeviceInfos processes GET /api/customer/{customerId}/deviceInfos
// (and .../devices). Same shape as the tenant listing, narrowed to one customer —
// this is how the UI shows the devices belonging to a site/client.
func HandleCustomerDeviceInfos(w http.ResponseWriter, r *http.Request, customerId string) {
	listDeviceInfos(w, r, customerId)
}

// listDeviceInfos backs both listings. customerId == "" means "whole tenant";
// the tenant predicate is always applied, so a customer filter can only narrow.
func listDeviceInfos(w http.ResponseWriter, r *http.Request, customerId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	textSearch := r.URL.Query().Get("textSearch")
	deviceType := r.URL.Query().Get("type")
	sortProperty := r.URL.Query().Get("sortProperty")
	sortOrder := r.URL.Query().Get("sortOrder")

	if sortProperty == "" {
		sortProperty = "name"
	}
	allowedSort := map[string]string{
		"name":        "d.name",
		"type":        "d.type",
		"label":       "d.label",
		"createdTime": "d.created_time",
	}
	sortCol := allowedSort[sortProperty]
	if sortCol == "" {
		sortCol = "d.name"
	}
	dir := "ASC"
	if strings.ToUpper(sortOrder) == "DESC" {
		dir = "DESC"
	}

	// Build WHERE conditions
	conditions := []string{"d.tenant_id = $1"}
	args := []interface{}{tenantId}
	argIdx := 2

	if textSearch != "" {
		conditions = append(conditions, "LOWER(d.name) LIKE $"+strconv.Itoa(argIdx))
		args = append(args, "%"+strings.ToLower(textSearch)+"%")
		argIdx++
	}
	if deviceType != "" {
		conditions = append(conditions, "d.type = $"+strconv.Itoa(argIdx))
		args = append(args, deviceType)
		argIdx++
	}
	if customerId != "" {
		conditions = append(conditions, "d.customer_id = $"+strconv.Itoa(argIdx))
		args = append(args, customerId)
		argIdx++
	}

	whereClause := strings.Join(conditions, " AND ")

	// Count
	var totalElements int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM device d WHERE "+whereClause, args...).Scan(&totalElements)

	// Fetch
	offset := page * pageSize
	query := `SELECT d.id, d.created_time, d.name, d.type, d.label, d.customer_id,
	                 d.device_profile_id, d.additional_info, d.device_data, d.version,
	                 d.firmware_id, d.software_id, d.external_id,
	                 dp.name AS profile_name,
	                 c.title AS customer_title, COALESCE(c.is_public, false) AS customer_is_public,
	                 (SELECT a.bool_v FROM attribute_kv a JOIN key_dictionary k ON a.attribute_key = k.key_id
	                    WHERE a.entity_id = d.id AND k.key = 'active' AND a.attribute_type = 2 LIMIT 1) AS active
	          FROM device d
	          LEFT JOIN device_profile dp ON dp.id = d.device_profile_id
	          LEFT JOIN customer c ON c.id = d.customer_id
	          WHERE ` + whereClause +
		" ORDER BY " + sortCol + " " + dir +
		" LIMIT $" + strconv.Itoa(argIdx) + " OFFSET $" + strconv.Itoa(argIdx+1)
	args = append(args, pageSize, offset)

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("ERROR querying devices: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, devType string
		var createdTime int64
		var label, customerIdStr, deviceProfileId, additionalInfo, profileName *string
		var deviceData, firmwareId, softwareId, externalId, customerTitle *string
		var customerIsPublic *bool
		var active *bool
		var version *int64

		if err := rows.Scan(&id, &createdTime, &name, &devType, &label, &customerIdStr,
			&deviceProfileId, &additionalInfo, &deviceData, &version,
			&firmwareId, &softwareId, &externalId,
			&profileName, &customerTitle, &customerIsPublic, &active); err != nil {
			log.Printf("ERROR scanning device: %v", err)
			continue
		}

		item := buildDeviceResponse(id, createdTime, name, devType, label, customerIdStr,
			deviceProfileId, additionalInfo, tenantId, version, profileName, true)

		// Enrich with deviceInfo-only fields
		if customerTitle != nil {
			item["customerTitle"] = *customerTitle
		} else {
			item["customerTitle"] = nil
		}
		if customerIsPublic != nil {
			item["customerIsPublic"] = *customerIsPublic
		} else {
			item["customerIsPublic"] = false
		}
		if active != nil {
			item["active"] = *active
		} else {
			item["active"] = false
		}
		if hotActive, ok := deviceactivity.ActiveFromTwin(context.Background(), tenantId, id, time.Now()); ok {
			item["active"] = hotActive
		}
		if firmwareId != nil && *firmwareId != "" {
			item["firmwareId"] = map[string]interface{}{"entityType": "OTA_PACKAGE", "id": *firmwareId}
		} else {
			item["firmwareId"] = nil
		}
		if softwareId != nil && *softwareId != "" {
			item["softwareId"] = map[string]interface{}{"entityType": "OTA_PACKAGE", "id": *softwareId}
		} else {
			item["softwareId"] = nil
		}
		if externalId != nil && *externalId != "" {
			item["externalId"] = map[string]interface{}{"entityType": "DEVICE", "id": *externalId}
		} else {
			item["externalId"] = nil
		}
		if deviceData != nil && *deviceData != "" {
			var dd interface{}
			if err := json.Unmarshal([]byte(*deviceData), &dd); err == nil {
				item["deviceData"] = dd
			} else {
				item["deviceData"] = nil
			}
		} else {
			item["deviceData"] = nil
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

// HandleDeviceTypes processes GET /api/device/types
func HandleDeviceTypes(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	rows, err := dbpkg.Pool.Query("SELECT DISTINCT type FROM device WHERE tenant_id = $1 AND type IS NOT NULL ORDER BY type", tenantId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	types := []map[string]interface{}{}
	for rows.Next() {
		var t string
		rows.Scan(&t)
		types = append(types, map[string]interface{}{
			"tenantId": map[string]interface{}{
				"entityType": "TENANT",
				"id":         tenantId,
			},
			"entityType": "DEVICE",
			"type":       t,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(types)
}

// HandleDeviceCredentials processes GET /api/device/{deviceId}/credentials
//
// SECURITY (P-1): every other CRUD handler in this package gates the
// device by tenant before returning data. The credentials read had a
// gap — only ExtractToken (any JWT) and a SELECT keyed on device_id —
// so a tenant-A admin could fetch the access token of any tenant-B
// device by guessing the UUID. Now joins device → tenant_id and
// returns 404 for cross-tenant access (404 not 403 to avoid leaking
// device existence to a different tenant).
func HandleDeviceCredentials(w http.ResponseWriter, r *http.Request, deviceId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var id, credentialsId, credentialsType string
	var credentialsValue *string

	err := dbpkg.Pool.QueryRow(`
		SELECT c.id, c.credentials_id, c.credentials_type, c.credentials_value
		FROM device_credentials c
		JOIN device d ON d.id = c.device_id
		WHERE c.device_id = $1 AND d.tenant_id = $2`, deviceId, tenantId,
	).Scan(&id, &credentialsId, &credentialsType, &credentialsValue)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Credentials not found")
		return
	}

	result := map[string]interface{}{
		"id": map[string]interface{}{
			"id": id,
		},
		"deviceId": map[string]interface{}{
			"entityType": "DEVICE",
			"id":         deviceId,
		},
		"credentialsId":   credentialsId,
		"credentialsType": credentialsType,
	}
	if credentialsValue != nil {
		result["credentialsValue"] = *credentialsValue
	} else {
		result["credentialsValue"] = nil
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// HandleTenantDeviceByName processes GET /api/tenant/devices?deviceName=X.
// Returns the single device matching that name within the caller's tenant,
// or 404 if it doesn't exist. Used by provisioning scripts/SDKs.
func HandleTenantDeviceByName(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	deviceName := r.URL.Query().Get("deviceName")
	if deviceName == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing deviceName")
		return
	}
	var id string
	err := dbpkg.Pool.QueryRow(
		"SELECT id FROM device WHERE tenant_id = $1 AND name = $2",
		tenantId, deviceName,
	).Scan(&id)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}
	dev, err := queryDevice(id, tenantId)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dev)
}

// HandleDevicesByIds processes GET /api/devices?deviceIds=id1,id2,...
func HandleDevicesByIds(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	idsParam := r.URL.Query().Get("deviceIds")
	if idsParam == "" {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
		return
	}

	ids := strings.Split(idsParam, ",")
	devices := []map[string]interface{}{}

	for _, id := range ids {
		id = strings.TrimSpace(id)
		dev, err := queryDevice(id, tenantId)
		if err == nil {
			devices = append(devices, dev)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(devices)
}

// ─── Internal helpers ───────────────────────────────────────────────────────

func queryDevice(deviceId, tenantId string) (map[string]interface{}, error) {
	var id, name, devType string
	var createdTime int64
	var label, customerIdStr, deviceProfileId, additionalInfo *string
	var version *int64

	err := dbpkg.Pool.QueryRow(`
		SELECT id, created_time, name, type, label, customer_id, device_profile_id, additional_info, version
		FROM device WHERE id = $1 AND tenant_id = $2`, deviceId, tenantId).Scan(
		&id, &createdTime, &name, &devType, &label, &customerIdStr,
		&deviceProfileId, &additionalInfo, &version)
	if err != nil {
		return nil, err
	}

	return buildDeviceResponse(id, createdTime, name, devType, label, customerIdStr,
		deviceProfileId, additionalInfo, tenantId, version, nil, false), nil
}

func queryDeviceInfo(deviceId, tenantId string) (map[string]interface{}, error) {
	var id, name, devType string
	var createdTime int64
	var label, customerIdStr, deviceProfileId, additionalInfo, profileName *string
	var version *int64

	err := dbpkg.Pool.QueryRow(`
		SELECT d.id, d.created_time, d.name, d.type, d.label, d.customer_id, 
		       d.device_profile_id, d.additional_info, d.version, dp.name
		FROM device d
		LEFT JOIN device_profile dp ON dp.id = d.device_profile_id
		WHERE d.id = $1 AND d.tenant_id = $2`, deviceId, tenantId).Scan(
		&id, &createdTime, &name, &devType, &label, &customerIdStr,
		&deviceProfileId, &additionalInfo, &version, &profileName)
	if err != nil {
		return nil, err
	}

	return buildDeviceResponse(id, createdTime, name, devType, label, customerIdStr,
		deviceProfileId, additionalInfo, tenantId, version, profileName, true), nil
}

func buildDeviceResponse(id string, createdTime int64, name, devType string,
	label, customerIdStr, deviceProfileId, additionalInfo *string,
	tenantId string, version *int64, profileName *string, isInfo bool) map[string]interface{} {

	nilUUID := "13814000-1dd2-11b2-8080-808080808080"
	custId := nilUUID
	if customerIdStr != nil && *customerIdStr != "" {
		custId = *customerIdStr
	}

	item := map[string]interface{}{
		"id": map[string]interface{}{
			"entityType": "DEVICE",
			"id":         id,
		},
		"createdTime": createdTime,
		"tenantId": map[string]interface{}{
			"entityType": "TENANT",
			"id":         tenantId,
		},
		"customerId": map[string]interface{}{
			"entityType": "CUSTOMER",
			"id":         custId,
		},
		"name": name,
		"type": devType,
	}

	if version != nil {
		item["version"] = *version
	} else {
		item["version"] = 1
	}
	if label != nil {
		item["label"] = *label
	} else {
		item["label"] = ""
	}
	if deviceProfileId != nil {
		item["deviceProfileId"] = map[string]interface{}{
			"entityType": "DEVICE_PROFILE",
			"id":         *deviceProfileId,
		}
	}

	if additionalInfo != nil && *additionalInfo != "" {
		var info interface{}
		json.Unmarshal([]byte(*additionalInfo), &info)
		item["additionalInfo"] = info
	} else {
		item["additionalInfo"] = nil
	}

	if isInfo && profileName != nil {
		item["deviceProfileName"] = *profileName
	}

	return item
}
