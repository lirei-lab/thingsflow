package device

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/provisioning"
	"flow-core/internal/quotas"
	"flow-core/internal/x509cred"
)

// TwinRegistrySync / TwinRegistryDelete are injected at boot (main.go) with
// internal/twin registry functions — sibling domains never import each other,
// so the twin registry stays convergent with device CRUD through these hooks
// (same pattern as transport.FetchAttributes). Nil until wired; call sites
// nil-check so tests and partial deployments stay safe.
var (
	TwinRegistrySync   func(tenantID, deviceID string)
	TwinRegistryDelete func(tenantID, deviceID string)
)

// ─── Device CRUD ──────────────────────────────────────────────────────────────

// HandleDeviceCreateOrUpdate processes POST /api/device (create or update).
// TB convention: if the request body has an "id", update; otherwise create.
func HandleDeviceCreateOrUpdate(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	name, _ := body["name"].(string)
	devType, _ := body["type"].(string)
	label, _ := body["label"].(string)
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing device name")
		return
	}
	if devType == "" {
		devType = "default"
	}

	customerId := httputil.ExtractEntityID(body, "customerId")
	deviceProfileId := httputil.ExtractEntityID(body, "deviceProfileId")

	// Resolve default device profile if not provided
	if deviceProfileId == "" {
		if err := dbpkg.Pool.QueryRow(
			"SELECT id FROM device_profile WHERE tenant_id = $1 AND is_default = true LIMIT 1",
			tenantId,
		).Scan(&deviceProfileId); err != nil {
			log.Printf("ERROR: No default device profile for tenant %s: %v", tenantId, err)
			httputil.WriteError(w, http.StatusInternalServerError, "No default device profile available")
			return
		}
	}

	additionalInfoJSON := dbutil.JSONOrNil(body["additionalInfo"])

	// Update if id present, else create
	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		// Update — check ownership first
		var existingTenant string
		if err := dbpkg.Pool.QueryRow("SELECT tenant_id FROM device WHERE id = $1", id).Scan(&existingTenant); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "Device not found")
			return
		}
		if existingTenant != tenantId {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
			return
		}

		_, err := dbpkg.Pool.Exec(`
			UPDATE device
			SET name = $1, type = $2, label = $3,
			    customer_id = $4, device_profile_id = $5,
			    additional_info = $6, version = COALESCE(version, 1) + 1
			WHERE id = $7`,
			name, devType, dbutil.NullStr(label), dbutil.NullUUID(customerId), deviceProfileId, additionalInfoJSON, id)
		if err != nil {
			log.Printf("ERROR updating device %s: %v", id, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update device")
			return
		}

		dev, _ := queryDevice(id, tenantId)
		audit.EntityChange(claims, "DEVICE", id, name, "UPDATED")
		httputil.WriteJSON(w, http.StatusOK, dev)
		return
	}

	// Create
	if !quotas.Enforce(w, tenantId, "device") {
		return
	}
	id = uuid.New().String()
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO device (id, created_time, name, type, label,
		                    customer_id, device_profile_id, additional_info,
		                    tenant_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1)`,
		id, now, name, devType, dbutil.NullStr(label),
		dbutil.NullUUID(customerId), deviceProfileId, additionalInfoJSON, tenantId)
	if err != nil {
		log.Printf("ERROR creating device: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create device")
		return
	}

	if TwinRegistrySync != nil {
		TwinRegistrySync(tenantId, id)
	}

	// Auto-create ACCESS_TOKEN credentials
	credId := uuid.New().String()
	credentialsId := generateAccessToken20()
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO device_credentials (id, created_time, device_id, credentials_type, credentials_id, version)
		VALUES ($1, $2, $3, 'ACCESS_TOKEN', $4, 1)`,
		credId, now, id, credentialsId)
	if err != nil {
		log.Printf("WARN: Failed to create default credentials for device %s: %v", id, err)
	}

	dev, _ := queryDevice(id, tenantId)
	audit.EntityChange(claims, "DEVICE", id, name, "ADDED")
	httputil.WriteJSON(w, http.StatusOK, dev)
}

// HandleDeviceDelete processes DELETE /api/device/{id}
func HandleDeviceDelete(w http.ResponseWriter, r *http.Request, deviceId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var existingTenant, existingName string
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, name FROM device WHERE id = $1", deviceId).Scan(&existingTenant, &existingName); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}
	if existingTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant delete denied")
		return
	}

	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to begin transaction")
		return
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM device_credentials WHERE device_id = $1", deviceId); err != nil {
		log.Printf("ERROR deleting credentials for device %s: %v", deviceId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete credentials")
		return
	}
	// entity_alarm is the lookup index; the alarm rows themselves live in `alarm`
	// and were being left behind. An alarm whose originator no longer exists shows
	// up in listings with nothing to click through to, and anything that re-derives
	// alarms from telemetry keeps extending it — telemetry outlives the device row.
	// Order matters: the index references the alarms, so it goes first.
	if _, err := tx.Exec("DELETE FROM entity_alarm WHERE entity_id = $1", deviceId); err != nil {
		log.Printf("WARN deleting entity_alarm for %s: %v", deviceId, err)
	}
	if _, err := tx.Exec("DELETE FROM alarm WHERE originator_id = $1", deviceId); err != nil {
		log.Printf("WARN deleting alarms for %s: %v", deviceId, err)
	}
	if _, err := tx.Exec("DELETE FROM attribute_kv WHERE entity_id = $1", deviceId); err != nil {
		log.Printf("WARN deleting attributes for %s: %v", deviceId, err)
	}
	if _, err := tx.Exec("DELETE FROM device WHERE id = $1", deviceId); err != nil {
		log.Printf("ERROR deleting device %s: %v", deviceId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete device")
		return
	}
	if err := tx.Commit(); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to commit deletion")
		return
	}
	// twin_registry has no FK/cascade — reclaim the row explicitly, after the
	// commit so a rolled-back delete never loses its registry entry.
	if TwinRegistryDelete != nil {
		TwinRegistryDelete(tenantId, deviceId)
	}

	audit.EntityChange(claims, "DEVICE", deviceId, existingName, "DELETED")
	w.WriteHeader(http.StatusOK)
}

// ─── Device Credentials CRUD ──────────────────────────────────────────────────

// HandleDeviceCredentialsUpdate processes POST /api/device/credentials
func HandleDeviceCredentialsUpdate(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	deviceId := httputil.ExtractEntityID(body, "deviceId")
	if deviceId == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing deviceId")
		return
	}

	var existingTenant, deviceName string
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, name FROM device WHERE id = $1", deviceId).Scan(&existingTenant, &deviceName); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}
	if existingTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
		return
	}

	normalized, status, err := normalizeCredentialInput(body)
	if err != nil {
		httputil.WriteError(w, status, err.Error())
		return
	}
	credentialsType, autoGenerated := normalized.Type, normalized.AutoGenerated

	if err := upsertDeviceCredentials(deviceId, normalized); err != nil {
		log.Printf("ERROR upserting credentials for device %s: %v", deviceId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save credentials")
		return
	}

	actionData, _ := json.Marshal(map[string]interface{}{
		"credentialsType": credentialsType,
		"autoGenerated":   autoGenerated,
	})
	userID, _ := claims["userId"].(string)
	userName, _ := claims["sub"].(string)
	audit.Write(audit.Event{
		TenantID:   tenantId,
		UserID:     userID,
		UserName:   userName,
		EntityID:   deviceId,
		EntityType: "DEVICE",
		EntityName: deviceName,
		ActionType: "CREDENTIALS_UPDATED",
		ActionData: string(actionData),
		Status:     "SUCCESS",
	})

	// Return updated credentials
	HandleDeviceCredentials(w, r, deviceId)
}

// deviceCredentialInput is a validated, ready-to-persist credential. Kept separate from
// the HTTP layer so callers (the credentials endpoint and the device-with-credentials
// wizard endpoint) can VALIDATE before mutating anything — the wizard must not leave a
// half-created device behind when the supplied credential is malformed.
type deviceCredentialInput struct {
	Type          string
	ID            string
	Value         string
	AutoGenerated bool
}

// normalizeCredentialInput validates + normalizes the TB credential payload fields.
// Returns the HTTP status to use when err != nil.
func normalizeCredentialInput(body map[string]interface{}) (deviceCredentialInput, int, error) {
	out := deviceCredentialInput{
		Type:  bodyString(body, "credentialsType"),
		ID:    bodyString(body, "credentialsId"),
		Value: bodyString(body, "credentialsValue"),
	}
	if out.Type == "" {
		out.Type = "ACCESS_TOKEN"
	}
	switch out.Type {
	case "ACCESS_TOKEN":
		normalizedID, err := normalizeAccessTokenCredential(out.ID)
		if err != nil {
			return out, http.StatusBadRequest, err
		}
		out.ID = normalizedID
		if out.ID == "" {
			out.ID = generateAccessToken20()
			out.AutoGenerated = true
		}
	case "X509_CERTIFICATE":
		fp, err := x509cred.ResolveCredentialsID(out.ID, out.Value)
		if err != nil {
			return out, http.StatusBadRequest, err
		}
		out.ID = fp
	case "MQTT_BASIC", "LWM2M_CREDENTIALS":
		if out.ID == "" {
			return out, http.StatusBadRequest, errors.New("credentialsId is required for " + out.Type)
		}
	default:
		return out, http.StatusBadRequest, errors.New("Unsupported credentialsType: " + out.Type)
	}
	return out, http.StatusOK, nil
}

// upsertDeviceCredentials persists the credential; device_id is unique so this replaces
// any existing credential for the device (rotation).
func upsertDeviceCredentials(deviceID string, cred deviceCredentialInput) error {
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO device_credentials (id, created_time, device_id, credentials_type, credentials_id, credentials_value, version)
		VALUES ($1, $2, $3, $4, $5, $6, 1)
		ON CONFLICT (device_id) DO UPDATE SET
		    credentials_type = EXCLUDED.credentials_type,
		    credentials_id = EXCLUDED.credentials_id,
		    credentials_value = EXCLUDED.credentials_value,
		    version = COALESCE(device_credentials.version, 1) + 1`,
		uuid.New().String(), time.Now().UnixMilli(), deviceID, cred.Type, cred.ID, dbutil.NullStr(cred.Value))
	return err
}

// ─── Device Profile CRUD ──────────────────────────────────────────────────────

// HandleDeviceProfileCreateOrUpdate processes POST /api/deviceProfile
func HandleDeviceProfileCreateOrUpdate(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	name, _ := body["name"].(string)
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing profile name")
		return
	}

	profileType, _ := body["type"].(string)
	if profileType == "" {
		profileType = "DEFAULT"
	}
	transportType, _ := body["transportType"].(string)
	if transportType == "" {
		transportType = "DEFAULT"
	}
	// Provisioning config: the ThingsBoard UI writes it into
	// profileData.provisionConfiguration ({type, provisionDeviceKey, provisionDeviceSecret}),
	// and only MIRRORS type/key at the top level — the SECRET is nested ONLY. Reading it
	// top-level meant a UI-configured secret was never persisted, so provision_device_secret_hash
	// stayed NULL and every /api/v1/provision attempt failed the secret check. Prefer the nested
	// values (the UI form is authoritative) and fall back to top-level for API/script callers.
	provisionCfg := nestedProvisionConfig(body)
	provisionType := firstNonEmpty(provisionCfg["type"], body["provisionType"])
	if provisionType == "" {
		provisionType = "DISABLED"
	}
	provisionDeviceKey := firstNonEmpty(provisionCfg["provisionDeviceKey"], body["provisionDeviceKey"])
	provisionSecretHash, provisionSecretProvided, err := provisioning.NormalizeProvisionSecretForProfile(
		firstNonEmpty(provisionCfg["provisionDeviceSecret"], body["provisionDeviceSecret"]))
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	description, _ := body["description"].(string)
	isDefault, _ := body["default"].(bool)

	profileData := dbutil.JSONOrNil(body["profileData"])
	if profileData == nil {
		emptyJSON := "{}"
		profileData = emptyJSON
	}

	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		var existingTenant string
		if err := dbpkg.Pool.QueryRow("SELECT tenant_id FROM device_profile WHERE id = $1", id).Scan(&existingTenant); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "Device profile not found")
			return
		}
		if existingTenant != tenantId {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
			return
		}

		secretHashArg := interface{}(nil)
		if provisionSecretProvided {
			secretHashArg = provisionSecretHash
		}
		_, err = dbpkg.Pool.Exec(`
			UPDATE device_profile
			SET name = $1, type = $2, transport_type = $3, provision_type = $4,
			    description = $5, is_default = $6, profile_data = $7::jsonb,
			    provision_device_key = $8,
			    provision_device_secret_hash = COALESCE($9::text, provision_device_secret_hash),
			    version = COALESCE(version, 1) + 1
			WHERE id = $10`,
			name, profileType, transportType, provisionType,
			dbutil.NullStr(description), isDefault, profileData, dbutil.NullStr(provisionDeviceKey), secretHashArg, id)
		if err != nil {
			log.Printf("ERROR updating device_profile %s: %v", id, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update profile")
			return
		}
		audit.EntityChange(claims, "DEVICE_PROFILE", id, name, "UPDATED")
		w.WriteHeader(http.StatusOK)
		return
	}

	id = uuid.New().String()
	var secretHashArg interface{}
	if provisionSecretProvided {
		secretHashArg = provisionSecretHash
	}
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO device_profile (id, created_time, name, type, transport_type, provision_type,
		                            description, is_default, tenant_id, profile_data,
		                            provision_device_key, provision_device_secret_hash, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, 1)`,
		id, now, name, profileType, transportType, provisionType,
		dbutil.NullStr(description), isDefault, tenantId, profileData,
		dbutil.NullStr(provisionDeviceKey), secretHashArg)
	if err != nil {
		log.Printf("ERROR creating device_profile: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create profile")
		return
	}

	audit.EntityChange(claims, "DEVICE_PROFILE", id, name, "ADDED")
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":                 map[string]interface{}{"entityType": "DEVICE_PROFILE", "id": id},
		"createdTime":        now,
		"name":               name,
		"type":               profileType,
		"default":            isDefault,
		"provisionType":      provisionType,
		"provisionDeviceKey": provisionDeviceKey,
	})
}

// HandleDeviceProfileDelete processes DELETE /api/deviceProfile/{id}
func HandleDeviceProfileDelete(w http.ResponseWriter, r *http.Request, profileId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var existingTenant, existingName string
	var isDefault bool
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, name, COALESCE(is_default, false) FROM device_profile WHERE id = $1", profileId).Scan(&existingTenant, &existingName, &isDefault); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device profile not found")
		return
	}
	if existingTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant delete denied")
		return
	}
	if isDefault {
		httputil.WriteError(w, http.StatusBadRequest, "Cannot delete default device profile")
		return
	}

	var inUse int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM device WHERE device_profile_id = $1", profileId).Scan(&inUse)
	if inUse > 0 {
		httputil.WriteError(w, http.StatusBadRequest, fmt.Sprintf("Profile is referenced by %d device(s)", inUse))
		return
	}

	if _, err := dbpkg.Pool.Exec("DELETE FROM device_profile WHERE id = $1", profileId); err != nil {
		log.Printf("ERROR deleting device_profile %s: %v", profileId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete profile")
		return
	}
	audit.EntityChange(claims, "DEVICE_PROFILE", profileId, existingName, "DELETED")
	w.WriteHeader(http.StatusOK)
}

// generateAccessToken20 produces a 20-char alphanumeric token (TB default format).
func generateAccessToken20() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := uuid.New()
	out := strings.Builder{}
	for i := 0; i < 20; i++ {
		out.WriteByte(alphabet[int(b[i%16])%len(alphabet)])
	}
	return out.String()
}

func bodyString(body map[string]interface{}, key string) string {
	v, _ := body[key].(string)
	return v
}

// nestedProvisionConfig returns body.profileData.provisionConfiguration, the object the
// ThingsBoard UI actually writes the provisioning form into (see DeviceProfileData in the
// UI's device.models.ts). Returns an empty map when absent so callers can index freely.
func nestedProvisionConfig(body map[string]interface{}) map[string]interface{} {
	profileData, ok := body["profileData"].(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	cfg, ok := profileData["provisionConfiguration"].(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	return cfg
}

// firstNonEmpty returns the first value that is a non-blank string, trimmed. Used to prefer
// the UI's nested provisioning fields over the top-level mirrors without losing API callers.
func firstNonEmpty(values ...interface{}) string {
	for _, v := range values {
		if s, ok := v.(string); ok {
			if s = strings.TrimSpace(s); s != "" {
				return s
			}
		}
	}
	return ""
}
