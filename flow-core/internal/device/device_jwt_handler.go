package device

import (
	"encoding/json"
	"net/http"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/devicejwt"
	"flow-core/internal/httputil"
)

// HandleDeviceJwt issues a short-lived device JWT for an existing device.
// It is a control-plane operation used by simulators, provisioning tooling,
// and custom UIs; telemetry flows device -> RMQTT/HTTP ingest -> NATS.
func HandleDeviceJwt(w http.ResponseWriter, r *http.Request, deviceID string) {
	if r.Method != http.MethodPost {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Tenant missing")
		return
	}
	issueDeviceJwtForDevice(w, deviceID, tenantID)
}

func issueDeviceJwtForDevice(w http.ResponseWriter, deviceID, tenantID string) {
	var name, securityStatus string
	err := dbpkg.Pool.QueryRow(
		`SELECT name, COALESCE(security_status, 'ACTIVE')
		   FROM device
		  WHERE id = $1 AND tenant_id = $2`,
		deviceID, tenantID,
	).Scan(&name, &securityStatus)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}
	if securityStatus != DeviceSecurityActive {
		httputil.WriteError(w, http.StatusForbidden, "Device is suspended")
		return
	}

	token, err := devicejwt.Issue(deviceID, tenantID, name)
	if err != nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(token)
}

// HandleDeviceJwtRefresh lets a native device exchange its still-valid
// short-lived Device JWT for a fresh one. It avoids storing tenant/user
// credentials on field gateways during normal operation.
func HandleDeviceJwtRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	raw := strings.TrimSpace(r.Header.Get("Authorization"))
	if raw == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Device JWT is required")
		return
	}
	claims, err := devicejwt.Validate(raw)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid or expired device JWT")
		return
	}
	deviceID, _ := claims["deviceId"].(string)
	if deviceID == "" {
		deviceID, _ = claims["sub"].(string)
	}
	tenantID, _ := claims["tenantId"].(string)
	if deviceID == "" || tenantID == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Device JWT claims missing")
		return
	}
	issueDeviceJwtForDevice(w, deviceID, tenantID)
}
