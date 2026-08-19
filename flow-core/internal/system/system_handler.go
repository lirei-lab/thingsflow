package system

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
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
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		// Path form: /api/admin/settings/{key}
		key = strings.TrimPrefix(r.URL.Path, "/api/admin/settings/")
		if key == "" || strings.Contains(key, "/") {
			// Auth before the 400 so an unauthenticated probe still sees 401.
			if _, err := httputil.ExtractToken(r); err != nil {
				httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
				return
			}
			httputil.WriteError(w, http.StatusBadRequest, "Missing settings key")
			return
		}
	}

	// System-scope settings (jwt, mail, sms, security) are SYS_ADMIN-only:
	// they hold the signing key, SMTP creds and security policy. Non-secret
	// keys (general, connectivity) stay readable by any authenticated user so
	// the tenant UI can bootstrap (baseUrl, connectivity params).
	if isSystemScopedSetting(key) {
		if _, ok := httputil.RequireSysAdmin(w, r); !ok {
			return
		}
	} else if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
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
	// Never return secret material over the wire, even to a SYS_ADMIN: the
	// live JWT signing key and SMTP password must not leave the DB. Redaction
	// is defensive-in-depth on top of the scope gate above.
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(redactSettingsSecrets(jsonValue)))
}

// isSystemScopedSetting reports whether an admin_settings key holds
// system-scope configuration that only a SYS_ADMIN may read/write.
func isSystemScopedSetting(key string) bool {
	switch key {
	case "jwt", "mail", "sms", "security", "securitySettings":
		return true
	default:
		return false
	}
}

// redactSettingsSecrets walks the JSON value of an admin_settings row and
// blanks out any secret-bearing field (tokenSigningKey, and any key whose
// name contains "password" or "secret", case-insensitively). Returns the
// input unchanged when it is not a JSON object we can parse.
func redactSettingsSecrets(jsonValue string) string {
	var v interface{}
	if err := json.Unmarshal([]byte(jsonValue), &v); err != nil {
		// Not parseable as JSON — safest to return an empty object rather
		// than risk emitting raw secret text.
		return "{}"
	}
	redactSecretsInPlace(v)
	out, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(out)
}

func redactSecretsInPlace(v interface{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			if isSecretKey(k) {
				t[k] = ""
				continue
			}
			redactSecretsInPlace(val)
		}
	case []interface{}:
		for _, item := range t {
			redactSecretsInPlace(item)
		}
	}
}

func isSecretKey(k string) bool {
	lk := strings.ToLower(k)
	return lk == "tokensigningkey" ||
		strings.Contains(lk, "password") ||
		strings.Contains(lk, "secret")
}

// HandleFeaturesInfo processes GET /api/admin/featuresInfo
func HandleFeaturesInfo(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	// oauthEnabled: real OIDC config exists (internal/oidc.ProvidersFromEnv
	// reads the same two env vars to decide single- vs multi-provider mode)
	// — checked directly here rather than by calling ProvidersFromEnv
	// itself, since that function performs live discovery HTTP calls and
	// this is a cheap status check, not a place to add request-time network
	// I/O. The other flags stay hardcoded: no email/SMS/Slack transport or
	// 2FA provider exists anywhere in this codebase to check against.
	oauthEnabled := strings.TrimSpace(os.Getenv("OIDC_PROVIDERS_JSON")) != "" ||
		strings.EqualFold(os.Getenv("OIDC_ENABLED"), "true") || os.Getenv("OIDC_ENABLED") == "1"

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"emailEnabled":        false,
		"smsEnabled":          false,
		"slackEnabled":        false,
		"oauthEnabled":        oauthEnabled,
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
	// The TB UI's menu builder (menu.service.ts updateOpenedMenuSections)
	// reads userSettings.openedMenuSections and crashes with
	// "Cannot read properties of undefined (reading 'includes')" when it is
	// absent. A stored GENERAL setting written by an older flow-core, or a
	// partial PUT like {}, may lack the key, so always merge the default in.
	if _, ok := settings["openedMenuSections"]; !ok {
		settings["openedMenuSections"] = defaults["openedMenuSections"]
	}
	return settings
}
