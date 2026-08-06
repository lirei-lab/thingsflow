package provisioning

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/devicejwt"
	"flow-core/internal/httputil"
	"flow-core/internal/quotas"
)

const (
	provisionAllowCreateNew = "ALLOW_CREATE_NEW_DEVICES"
	provisionCheckExisting  = "CHECK_PRE_PROVISIONED_DEVICES"
)

// TwinRegistrySync is injected at boot (main.go) with internal/twin's registry
// upsert — provisioning is the real production onboarding path, so a device
// created here must get its twin registry row exactly like a UI-created one.
// Sibling domains never import each other; nil until wired.
var TwinRegistrySync func(tenantID, deviceID string)

type provisionRequest struct {
	DeviceName            string `json:"deviceName"`
	DeviceType            string `json:"deviceType"`
	ProvisionDeviceKey    string `json:"provisionDeviceKey"`
	ProvisionDeviceSecret string `json:"provisionDeviceSecret"`
}

type normalizedProvisionRequest struct {
	DeviceName            string
	DeviceType            string
	ProvisionDeviceKey    string
	ProvisionDeviceSecret string
}

type provisionResponse struct {
	Status           string                   `json:"status"`
	CredentialsType  string                   `json:"credentialsType,omitempty"`
	CredentialsValue string                   `json:"credentialsValue,omitempty"`
	DeviceID         string                   `json:"deviceId,omitempty"`
	TenantID         string                   `json:"tenantId,omitempty"`
	DeviceJWT        *devicejwt.TokenResponse `json:"deviceJwt,omitempty"`
	ErrorMsg         string                   `json:"errorMsg,omitempty"`
}

func normalizeProvisionRequest(req provisionRequest) (normalizedProvisionRequest, error) {
	out := normalizedProvisionRequest{
		DeviceName:            strings.TrimSpace(req.DeviceName),
		DeviceType:            strings.TrimSpace(req.DeviceType),
		ProvisionDeviceKey:    strings.TrimSpace(req.ProvisionDeviceKey),
		ProvisionDeviceSecret: strings.TrimSpace(req.ProvisionDeviceSecret),
	}
	if out.DeviceName == "" {
		return out, errors.New("deviceName is required")
	}
	if out.DeviceType == "" {
		out.DeviceType = "default"
	}
	if out.ProvisionDeviceKey == "" {
		return out, errors.New("provisionDeviceKey is required")
	}
	if out.ProvisionDeviceSecret == "" {
		return out, errors.New("provisionDeviceSecret is required")
	}
	return out, nil
}

func HashProvisionSecret(secret string) (string, error) {
	return hashProvisionSecret(secret)
}

func hashProvisionSecret(secret string) (string, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return "", errors.New("provisionDeviceSecret is required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func verifyProvisionSecret(hash, secret string) bool {
	hash = strings.TrimSpace(hash)
	secret = strings.TrimSpace(secret)
	if hash == "" || secret == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(secret)) == nil
}

func successResponse(credentialsType, credentialsValue, deviceID, tenantID string, jwtToken *devicejwt.TokenResponse) provisionResponse {
	return provisionResponse{
		Status:           "SUCCESS",
		CredentialsType:  credentialsType,
		CredentialsValue: credentialsValue,
		DeviceID:         deviceID,
		TenantID:         tenantID,
		DeviceJWT:        jwtToken,
	}
}

func failureResponse(msg string) provisionResponse {
	return provisionResponse{Status: "FAILURE", ErrorMsg: msg}
}

// HandleProvision implements the TB-style public device provisioning endpoint:
// POST /api/v1/provision.
func HandleProvision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteJSON(w, http.StatusServiceUnavailable, failureResponse("service unavailable"))
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil || len(body) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}
	var raw provisionRequest
	if err := json.Unmarshal(body, &raw); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}
	req, err := normalizeProvisionRequest(raw)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	credentials, deviceID, tenantID, created, ok := provisionDevice(req)
	if !ok {
		httputil.WriteJSON(w, http.StatusOK, failureResponse("provisioning failed"))
		return
	}
	if created {
		audit.Write(audit.Event{
			TenantID:   tenantID,
			UserName:   "device-provisioning",
			EntityID:   deviceID,
			EntityType: "DEVICE",
			EntityName: req.DeviceName,
			ActionType: "PROVISIONED",
			Status:     "SUCCESS",
		})
	}
	var issued *devicejwt.TokenResponse
	if devicejwt.Enabled() {
		token, err := devicejwt.Issue(deviceID, tenantID, req.DeviceName)
		if err != nil {
			log.Printf("WARN provisioning device JWT issue failed: %v", err)
			httputil.WriteJSON(w, http.StatusServiceUnavailable, failureResponse("device credentials unavailable"))
			return
		}
		issued = &token
	}
	httputil.WriteJSON(w, http.StatusOK, successResponse("ACCESS_TOKEN", credentials, deviceID, tenantID, issued))
}

func provisionDevice(req normalizedProvisionRequest) (credentialsID, deviceID, tenantID string, created bool, ok bool) {
	var profileID, provisionType, secretHash string
	err := dbpkg.Pool.QueryRow(`
		SELECT id::text, tenant_id::text, COALESCE(provision_type, ''),
		       COALESCE(provision_device_secret_hash, '')
		  FROM device_profile
		 WHERE provision_device_key = $1
		 LIMIT 1`, req.ProvisionDeviceKey,
	).Scan(&profileID, &tenantID, &provisionType, &secretHash)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("WARN provisioning profile lookup failed: %v", err)
		}
		return "", "", "", false, false
	}
	if tenantID == "" || !verifyProvisionSecret(secretHash, req.ProvisionDeviceSecret) {
		return "", "", "", false, false
	}
	if provisionType != provisionAllowCreateNew && provisionType != provisionCheckExisting {
		return "", "", "", false, false
	}

	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		log.Printf("WARN provisioning begin failed: %v", err)
		return "", "", "", false, false
	}
	defer tx.Rollback()

	err = tx.QueryRow(`
		SELECT id::text
		  FROM device
		 WHERE tenant_id = $1 AND name = $2 AND device_profile_id = $3`,
		tenantID, req.DeviceName, profileID,
	).Scan(&deviceID)
	switch {
	case err == nil:
	case err == sql.ErrNoRows && provisionType == provisionAllowCreateNew:
		if !allowDeviceCreate(tenantID) {
			return "", "", "", false, false
		}
		deviceID = uuid.NewString()
		now := time.Now().UnixMilli()
		_, err = tx.Exec(`
			INSERT INTO device (id, created_time, name, type, device_profile_id, tenant_id, version)
			VALUES ($1, $2, $3, $4, $5, $6, 1)`,
			deviceID, now, req.DeviceName, req.DeviceType, profileID, tenantID)
		if err != nil {
			log.Printf("WARN provisioning device insert failed: %v", err)
			return "", "", "", false, false
		}
		created = true
	case err == sql.ErrNoRows:
		return "", "", "", false, false
	default:
		log.Printf("WARN provisioning device lookup failed: %v", err)
		return "", "", "", false, false
	}

	err = tx.QueryRow(`
		SELECT credentials_id
		  FROM device_credentials
		 WHERE device_id = $1 AND credentials_type = 'ACCESS_TOKEN'`,
		deviceID,
	).Scan(&credentialsID)
	if err == sql.ErrNoRows {
		credentialsID = generateAccessToken20()
		_, err = tx.Exec(`
			INSERT INTO device_credentials (id, created_time, device_id, credentials_type, credentials_id, version)
			VALUES ($1, $2, $3, 'ACCESS_TOKEN', $4, 1)
			ON CONFLICT (device_id) DO UPDATE SET
			    credentials_type = 'ACCESS_TOKEN',
			    credentials_id = EXCLUDED.credentials_id,
			    credentials_value = NULL,
			    version = COALESCE(device_credentials.version, 1) + 1`,
			uuid.NewString(), time.Now().UnixMilli(), deviceID, credentialsID)
	}
	if err != nil {
		log.Printf("WARN provisioning credentials failed: %v", err)
		return "", "", "", false, false
	}

	if err := tx.Commit(); err != nil {
		log.Printf("WARN provisioning commit failed: %v", err)
		return "", "", "", false, false
	}
	// After the commit so a rolled-back provision never leaves a registry row.
	if created && TwinRegistrySync != nil {
		TwinRegistrySync(tenantID, deviceID)
	}
	return credentialsID, deviceID, tenantID, created, true
}

func generateAccessToken20() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := uuid.New()
	var out strings.Builder
	for i := 0; i < 20; i++ {
		out.WriteByte(alphabet[int(b[i%16])%len(alphabet)])
	}
	return out.String()
}

func allowDeviceCreate(tenantID string) bool {
	limit := quotas.LimitsFor(tenantID).MaxDevices
	if dbpkg.Pool == nil || tenantID == "" || limit <= 0 {
		return true
	}
	var count int64
	if err := dbpkg.Pool.QueryRow(`SELECT count(*) FROM device WHERE tenant_id = $1`, tenantID).Scan(&count); err != nil {
		log.Printf("WARN provisioning quota count(%s): %v; allowing through", tenantID, err)
		return true
	}
	if count >= limit {
		log.Printf("Quota DENY: tenant=%s kind=device count=%d limit=%d", tenantID, count, limit)
		return false
	}
	return true
}

func NormalizeProvisionSecretForProfile(secret string) (string, bool, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return "", false, nil
	}
	hash, err := hashProvisionSecret(secret)
	if err != nil {
		return "", false, fmt.Errorf("hash provisionDeviceSecret: %w", err)
	}
	return hash, true, nil
}
