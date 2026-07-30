package device

import (
	"encoding/json"
	"net/http"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

const (
	DeviceSecurityActive    = "ACTIVE"
	DeviceSecuritySuspended = "SUSPENDED"
)

func normalizeDeviceSecurityStatus(status string) (string, bool) {
	switch status {
	case "", DeviceSecurityActive:
		return DeviceSecurityActive, true
	case DeviceSecuritySuspended:
		return DeviceSecuritySuspended, true
	default:
		return "", false
	}
}

// HandleDeviceSecurity processes GET/POST /api/device/{id}/security.
// This is a flow-core extension over TB classic: the device and its
// credentials remain visible/manageable, but transports deny auth while
// the device is SUSPENDED.
func HandleDeviceSecurity(w http.ResponseWriter, r *http.Request, deviceID string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	var deviceTenant, deviceName, currentStatus string
	if err := dbpkg.Pool.QueryRow(`
		SELECT tenant_id::text, name, COALESCE(security_status, 'ACTIVE')
		FROM device
		WHERE id = $1`, deviceID,
	).Scan(&deviceTenant, &deviceName, &currentStatus); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}
	if deviceTenant != tenantID {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}

	if r.Method == "GET" {
		writeDeviceSecurity(w, deviceID, currentStatus)
		return
	}
	if r.Method != "POST" && r.Method != "PUT" {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	rawStatus, _ := body["securityStatus"].(string)
	if rawStatus == "" {
		rawStatus, _ = body["status"].(string)
	}
	nextStatus, valid := normalizeDeviceSecurityStatus(rawStatus)
	if !valid {
		httputil.WriteError(w, http.StatusBadRequest, "Unsupported securityStatus")
		return
	}

	if _, err := dbpkg.Pool.Exec(`
		UPDATE device
		SET security_status = $1, version = COALESCE(version, 1) + 1
		WHERE id = $2 AND tenant_id = $3`, nextStatus, deviceID, tenantID,
	); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to update device security status")
		return
	}

	actionData, _ := json.Marshal(map[string]interface{}{
		"oldSecurityStatus": currentStatus,
		"newSecurityStatus": nextStatus,
	})
	userID, _ := claims["userId"].(string)
	userName, _ := claims["sub"].(string)
	audit.Write(audit.Event{
		TenantID:   tenantID,
		UserID:     userID,
		UserName:   userName,
		EntityID:   deviceID,
		EntityType: "DEVICE",
		EntityName: deviceName,
		ActionType: "SECURITY_STATUS_UPDATED",
		ActionData: string(actionData),
		Status:     "SUCCESS",
	})

	writeDeviceSecurity(w, deviceID, nextStatus)
}

func writeDeviceSecurity(w http.ResponseWriter, deviceID, status string) {
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"deviceId": map[string]interface{}{
			"entityType": "DEVICE",
			"id":         deviceID,
		},
		"securityStatus": status,
	})
}
