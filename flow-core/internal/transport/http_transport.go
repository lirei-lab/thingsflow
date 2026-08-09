package transport

// HTTP device transport — TB classic compatible surface for device
// provisioning and read-only attribute fetches.
//
// All routes share /api/v1/{deviceToken}/ prefix and authenticate by
// looking up the access token in device_credentials.
//
//   POST /api/v1/{token}/telemetry          — rejected; use http-ingest
//   POST /api/v1/{token}/attributes         — rejected; use http-ingest
//   GET  /api/v1/{token}/attributes         — query: ?clientKeys=&sharedKeys=
//
// Device-origin writes are not accepted by flow-core. The production data plane
// is Envoy/Bento -> NATS -> materializers.
// GET attributes is synchronous and reads from postgres directly — TB
// classic also bypasses the queue here.

import (
	"net/http"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// resolveDeviceByToken returns (deviceId, tenantId, profileId) for an
// ACCESS_TOKEN device, or "","" if the token doesn't match.
func resolveDeviceByToken(token string) (deviceID, tenantID, profileID string, ok bool) {
	if token == "" || dbpkg.Pool == nil {
		return "", "", "", false
	}
	err := dbpkg.Pool.QueryRow(`
		SELECT c.device_id::text, d.tenant_id::text, d.device_profile_id::text
		  FROM device_credentials c
		 JOIN device d ON d.id = c.device_id
		 WHERE c.credentials_type = 'ACCESS_TOKEN'
		   AND c.credentials_id = $1
		   AND COALESCE(d.security_status, 'ACTIVE') = 'ACTIVE'`, token,
	).Scan(&deviceID, &tenantID, &profileID)
	if err != nil {
		return "", "", "", false
	}
	return deviceID, tenantID, profileID, true
}

// Handle — dispatches /api/v1/{token}/{action} based on
// path suffix and method. Wired in api.go via /api/v1/ prefix.
func Handle(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		http.NotFound(w, r)
		return
	}
	token := parts[0]
	action := parts[1]

	deviceID, _, _, ok := resolveDeviceByToken(token)
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("device not found"))
		return
	}

	if (action == "telemetry" && r.Method == "POST") ||
		(action == "attributes" && r.Method == "POST") {
		http.Error(w, "device ingest is handled by the HTTP ingest gateway", http.StatusServiceUnavailable)
		return
	}

	switch {
	case action == "attributes" && r.Method == "GET":
		handleHttpAttributesGet(w, r, deviceID)
	default:
		http.Error(w, "not implemented", http.StatusNotFound)
	}
}

// handleHttpAttributesGet reads CLIENT/SHARED/DESIRED scope attributes
// synchronously (not via queue) — same approach TB classic takes for HTTP GETs.
// The `desired` group (R5) serves the desired state persisted as SERVER_SCOPE
// feature.<name>.desired.<property> keys (05-02) to non-MQTT devices that poll
// instead of subscribing to the MQTT desired topic; it is additive and never
// changes the existing {client, shared} shape.
func handleHttpAttributesGet(w http.ResponseWriter, r *http.Request, deviceID string) {
	q := r.URL.Query()
	clientKeys := splitCSV(q.Get("clientKeys"))
	sharedKeys := splitCSV(q.Get("sharedKeys"))
	desiredKeys := splitCSV(q.Get("desiredKeys"))

	resp := map[string]interface{}{
		"client":  map[string]interface{}{},
		"shared":  map[string]interface{}{},
		"desired": map[string]interface{}{},
	}
	if len(clientKeys) > 0 {
		FetchAttributes(deviceID, 0, clientKeys, resp["client"].(map[string]interface{}))
	}
	if len(sharedKeys) > 0 {
		// attribute_type 1 = SHARED_SCOPE per the canonical mapping
		// (CLIENT=0, SHARED=1, SERVER=2 — tenant.normalizeAttributeScope).
		// This read 2 (SERVER_SCOPE) for years, so shared attributes
		// written via the UI were invisible to devices.
		FetchAttributes(deviceID, 1, sharedKeys, resp["shared"].(map[string]interface{}))
	}
	if len(desiredKeys) > 0 {
		// attribute_type 2 = SERVER_SCOPE. Desired state persists as
		// feature.<name>.desired.<property> SERVER_SCOPE keys (05-02); a
		// non-MQTT device polls them here. The F1 fix that corrected the
		// shared read (attr_type 1) is the same mapping this uses.
		FetchAttributes(deviceID, 2, desiredKeys, resp["desired"].(map[string]interface{}))
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
