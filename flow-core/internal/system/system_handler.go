package system

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// HandleSystemParams processes GET /api/system/params
// Returns system configuration that the TB UI needs to bootstrap.
func HandleSystemParams(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}

	userId, _ := claims["userId"].(string)

	// Load user-specific settings (opened menu sections, etc.)
	userSettings := loadUserSettings(userId)

	params := map[string]interface{}{
		"userTokenAccessEnabled":                           true,
		"allowedDashboardIds":                              []string{},
		"edgesSupportEnabled":                              false,
		"hasRepository":                                    false,
		"tbelEnabled":                                      true,
		"persistDeviceStateToTelemetry":                    false,
		"userSettings":                                     userSettings,
		"maxDatapointsLimit":                               50000,
		"maxResourceSize":                                  0,
		"mobileQrEnabled":                                  false,
		"maxDebugModeDurationMinutes":                      15,
		"ruleChainDebugPerTenantLimitsConfiguration":       "50000:3600",
		"calculatedFieldDebugPerTenantLimitsConfiguration": "50000:3600",
		"maxArgumentsPerCF":                                10,
		"maxDataPointsPerRollingArg":                       1000,
		"minAllowedScheduledUpdateIntervalInSecForCF":      10,
		"maxRelationLevelPerCfArgument":                    2,
		"maxRelatedEntitiesToReturnPerCfArgument":          100,
		"minAllowedDeduplicationIntervalInSecForCF":        10,
		"minAllowedAggregationIntervalInSecForCF":          60,
		"intermediateAggregationIntervalInSecForCF":        300,
		"trendzSettings": map[string]interface{}{
			"enabled": false,
			"baseUrl": nil,
			"apiKey":  nil,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(params)
}

// HandleServerTime processes GET /api/dashboard/serverTime.
// TB returns an epoch-millisecond Long, NOT an ISO string. Returning a string
// (or worse, an unquoted ISO) breaks the UI's RxJS chain on the home dashboard.
func HandleServerTime(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%d", time.Now().UnixMilli())
}

// HandleAdminSettings processes GET /api/admin/settings/{key} where key
// is one of general, mail, sms, jwt, securitySettings, mailTemplate,
// twoFaSettings, etc.
//
// UI v3.7+ sends the key in the path; older versions / SDKs use ?key=X.
// Both shapes are accepted. Missing rows return {} (empty config) so
// settings pages render with default values instead of erroring.
func HandleAdminSettings(w http.ResponseWriter, r *http.Request) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		// Path form: /api/admin/settings/{key}
		key = strings.TrimPrefix(r.URL.Path, "/api/admin/settings/")
		if key == "" || strings.Contains(key, "/") {
			httputil.WriteError(w, http.StatusBadRequest, "Missing settings key")
			return
		}
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "Database not available")
		return
	}

	var jsonValue string
	if err := dbpkg.Pool.QueryRow("SELECT json_value FROM admin_settings WHERE key = $1", key).Scan(&jsonValue); err != nil {
		// Missing row = unconfigured. Return an empty object so the UI
		// renders the form with defaults instead of throwing.
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(jsonValue))
}

// HandleFeaturesInfo processes GET /api/admin/featuresInfo
func HandleFeaturesInfo(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"emailEnabled":        false,
		"smsEnabled":          false,
		"slackEnabled":        false,
		"oauthEnabled":        false,
		"twoFaEnabled":        false,
		"notificationEnabled": true,
	})
}

// HandleUserSettings processes GET/POST /api/user/settings
func HandleUserSettings(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}

	userId, _ := claims["userId"].(string)

	if r.Method == "GET" {
		settings := loadUserSettings(userId)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(settings)
	} else if r.Method == "PUT" || r.Method == "POST" {
		var settings interface{}
		if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		settingsJSON, _ := json.Marshal(settings)
		_, err := dbpkg.Pool.Exec(`
			INSERT INTO user_settings (user_id, type, settings) 
			VALUES ($1, 'GENERAL', $2::jsonb) 
			ON CONFLICT (user_id, type) DO UPDATE SET settings = $2::jsonb`,
			userId, string(settingsJSON))
		if err != nil {
			log.Printf("ERROR saving user settings: %v", err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to save settings")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(settings)
	}
}

// loadUserSettings fetches user settings from the user_settings table.
func loadUserSettings(userId string) map[string]interface{} {
	defaults := map[string]interface{}{
		"openedMenuSections": []string{"/entities"},
	}

	if dbpkg.Pool == nil || userId == "" {
		return defaults
	}

	var settingsJSON string
	err := dbpkg.Pool.QueryRow("SELECT settings::text FROM user_settings WHERE user_id = $1 AND type = 'GENERAL'", userId).Scan(&settingsJSON)
	if err != nil {
		return defaults
	}

	var settings map[string]interface{}
	if err := json.Unmarshal([]byte(settingsJSON), &settings); err != nil {
		return defaults
	}
	return settings
}
