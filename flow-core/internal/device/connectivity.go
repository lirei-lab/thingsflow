package device

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/devicejwt"
	"flow-core/internal/httputil"
)

// HandleDeviceConnectivity — GET /api/device-connectivity/{id}.
//
// Backs the ThingsBoard UI "Check connectivity" dialog. It returns the
// PublishTelemetryCommand shape the UI renders (http/mqtt/coap groups of shell
// snippets).
//
// Why this is not a cosmetic endpoint here: the UI shows a device's ACCESS_TOKEN as
// "the credential", but this platform authenticates ingest with a short-lived ES256
// DEVICE JWT — the access token does not work for MQTT at all. Without this endpoint an
// operator has no in-UI way to learn the real connection recipe. So the snippets embed a
// freshly minted device JWT, the pinned MQTT Client ID (the broker rejects a mismatch and
// the ACL binds the topic to it), and a `ts` field (the data plane drops ts-less messages).
//
// Endpoints come from env so the chart stays the single source of truth:
//
//	DEVICE_CONNECTIVITY_HTTP_BASE_URL — public base for HTTP ingest (default ALLOWED_ORIGIN)
//	DEVICE_CONNECTIVITY_MQTT_HOST/_PORT — public MQTT-over-TLS endpoint (mqtt group omitted if host unset)
func HandleDeviceConnectivity(w http.ResponseWriter, r *http.Request, deviceID string) {
	if r.Method != http.MethodGet {
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

	var name, securityStatus string
	if err := dbpkg.Pool.QueryRow(
		`SELECT name, COALESCE(security_status, 'ACTIVE')
		   FROM device
		  WHERE id = $1 AND tenant_id = $2`,
		deviceID, tenantID,
	).Scan(&name, &securityStatus); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}

	// The dialog is a copy-paste aid, so mint a usable credential. A suspended device gets
	// the recipe without a token rather than a hard error, so the operator still sees how
	// the transport works and why it will be refused.
	var token devicejwt.TokenResponse
	jwtErr := error(nil)
	if securityStatus == DeviceSecurityActive {
		token, jwtErr = devicejwt.Issue(deviceID, tenantID, name)
	}
	jwtValue := "<DEVICE_JWT>"
	mqttID := topicSafeConnectivityIdentity(deviceID)
	if jwtErr == nil && token.Token != "" {
		jwtValue = token.Token
		if token.MQTTIdentity != "" {
			mqttID = token.MQTTIdentity
		}
	}

	result := map[string]interface{}{}

	if base := connectivityHTTPBaseURL(); base != "" {
		// Envoy verifies this JWT and stamps trusted tenant/device headers before Bento.
		result["http"] = map[string]interface{}{
			"https": strings.Join([]string{
				"export DEVICE_JWT='" + jwtValue + "'   # short-lived; refresh: POST /api/v1/devices/me/jwt/refresh",
				"curl -X POST '" + base + "/api/v1/telemetry' \\",
				"  -H \"Authorization: Bearer $DEVICE_JWT\" -H 'Content-Type: application/json' \\",
				"  -d \"{\\\"ts\\\":$(date +%s000),\\\"temperature\\\":25}\"",
			}, "\n"),
		}
	}

	if host, port := connectivityMQTTHost(), connectivityMQTTPort(); host != "" {
		topic := "thingsflow/devices/" + mqttID + "/telemetry"
		result["mqtt"] = map[string]interface{}{
			"mqtts": []string{
				"export DEVICE_JWT='" + jwtValue + "'   # short-lived; refresh: POST /api/v1/devices/me/jwt/refresh",
				"mosquitto_pub -h " + host + " -p " + port + " --capath /etc/ssl/certs \\",
				"  -i '" + mqttID + "' -u \"$DEVICE_JWT\" \\",
				"  -t '" + topic + "' \\",
				"  -m \"{\\\"ts\\\":$(date +%s000),\\\"temperature\\\":25}\"",
			},
		}
	} else {
		result["mqtt"] = map[string]interface{}{}
	}

	// No CoAP transport in this platform; return the empty group the UI expects.
	result["coap"] = map[string]interface{}{}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func connectivityHTTPBaseURL() string {
	if v := strings.TrimSpace(os.Getenv("DEVICE_CONNECTIVITY_HTTP_BASE_URL")); v != "" {
		return strings.TrimRight(v, "/")
	}
	return strings.TrimRight(strings.TrimSpace(os.Getenv("ALLOWED_ORIGIN")), "/")
}

func connectivityMQTTHost() string {
	return strings.TrimSpace(os.Getenv("DEVICE_CONNECTIVITY_MQTT_HOST"))
}

func connectivityMQTTPort() string {
	if v := strings.TrimSpace(os.Getenv("DEVICE_CONNECTIVITY_MQTT_PORT")); v != "" {
		return v
	}
	return "8883"
}

// topicSafeConnectivityIdentity mirrors the device-JWT mqttId derivation (hyphen-stripped
// device id) so the snippet is still correct when no JWT could be minted.
func topicSafeConnectivityIdentity(deviceID string) string {
	return strings.ReplaceAll(deviceID, "-", "")
}
