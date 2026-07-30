// Package asset implements /api/asset and /api/assetProfile CRUD.
package asset

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/quotas"
)

// GetByID serves GET /api/asset/{id} and GET /api/asset/info/{id}.
// The two endpoints share the same row but the UI calls each from a
// different page (asset detail editor vs asset list "info" projection).
// We return the full Asset shape — extra fields are harmless and the
// detail editor specifically reads `name`, `label`, `type`,
// `customerId`, `assetProfileId`, `additionalInfo`. Without this
// handler the catch-all returned an empty PageData and the detail
// pane rendered blank.
func GetByID(w http.ResponseWriter, r *http.Request, assetId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	if !httputil.LooksLikeUUID(assetId) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid asset id")
		return
	}

	var (
		id, name                                             string
		createdTime                                          int64
		additionalInfo, customerId, label, atype, externalId *string
		assetProfileId                                       string
		rowTenantId                                          string
	)
	err := dbpkg.Pool.QueryRow(`
		SELECT id::text, created_time, additional_info, customer_id::text,
		       asset_profile_id::text, name, label, type, tenant_id::text, external_id::text
		  FROM asset WHERE id = $1`, assetId,
	).Scan(&id, &createdTime, &additionalInfo, &customerId, &assetProfileId,
		&name, &label, &atype, &rowTenantId, &externalId)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Asset not found")
		return
	}
	// Cross-tenant guard: tenant_admin only sees their own; sysadmin
	// (tenantId empty in claims) sees everything.
	if tenantId != "" && rowTenantId != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
		return
	}

	resp := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "ASSET", "id": id},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": rowTenantId},
		"name":        name,
		"version":     1,
	}
	if label != nil {
		resp["label"] = *label
	} else {
		resp["label"] = ""
	}
	if atype != nil {
		resp["type"] = *atype
	}
	if customerId != nil && *customerId != "" {
		resp["customerId"] = map[string]interface{}{"entityType": "CUSTOMER", "id": *customerId}
	} else {
		resp["customerId"] = nil
	}
	if assetProfileId != "" {
		resp["assetProfileId"] = map[string]interface{}{"entityType": "ASSET_PROFILE", "id": assetProfileId}
	}
	if additionalInfo != nil && *additionalInfo != "" {
		var ai interface{}
		if err := json.Unmarshal([]byte(*additionalInfo), &ai); err == nil {
			resp["additionalInfo"] = ai
		}
	} else {
		resp["additionalInfo"] = nil
	}
	if externalId != nil && *externalId != "" {
		resp["externalId"] = map[string]interface{}{"entityType": "ASSET", "id": *externalId}
	} else {
		resp["externalId"] = nil
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// Save — POST /api/asset (create or update).
func Save(w http.ResponseWriter, r *http.Request) {
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
		httputil.WriteError(w, http.StatusBadRequest, "Missing asset name")
		return
	}
	assetType, _ := body["type"].(string)
	if assetType == "" {
		assetType = "default"
	}
	label, _ := body["label"].(string)
	additionalInfoJSON := dbutil.JSONOrNil(body["additionalInfo"])
	customerId := httputil.ExtractEntityID(body, "customerId")
	assetProfileId := httputil.ExtractEntityID(body, "assetProfileId")
	if assetProfileId == "" {
		if err := dbpkg.Pool.QueryRow(
			"SELECT id FROM asset_profile WHERE tenant_id = $1 AND is_default = true LIMIT 1",
			tenantId,
		).Scan(&assetProfileId); err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "No default asset profile")
			return
		}
	}

	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		var existingTenant string
		if err := dbpkg.Pool.QueryRow("SELECT tenant_id FROM asset WHERE id = $1", id).Scan(&existingTenant); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "Asset not found")
			return
		}
		if existingTenant != tenantId {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
			return
		}
		_, err := dbpkg.Pool.Exec(`
			UPDATE asset SET name = $1, type = $2, label = $3,
			    customer_id = $4, asset_profile_id = $5, additional_info = $6,
			    version = COALESCE(version, 1) + 1
			WHERE id = $7`,
			name, assetType, dbutil.NullStr(label), dbutil.NullUUID(customerId), assetProfileId,
			additionalInfoJSON, id)
		if err != nil {
			log.Printf("ERROR updating asset %s: %v", id, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update asset")
			return
		}
		audit.EntityChange(claims, "ASSET", id, name, "UPDATED")
		w.WriteHeader(http.StatusOK)
		return
	}
	if !quotas.Enforce(w, tenantId, "asset") {
		return
	}
	id = uuid.New().String()
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO asset (id, created_time, name, type, label,
		                  customer_id, asset_profile_id, additional_info,
		                  tenant_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1)`,
		id, now, name, assetType, dbutil.NullStr(label),
		dbutil.NullUUID(customerId), assetProfileId, additionalInfoJSON, tenantId)
	if err != nil {
		log.Printf("ERROR creating asset: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create asset")
		return
	}
	audit.EntityChange(claims, "ASSET", id, name, "ADDED")
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "ASSET", "id": id},
		"createdTime": now,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"name":        name,
		"type":        assetType,
	})
}

// Delete — DELETE /api/asset/{id}.
func Delete(w http.ResponseWriter, r *http.Request, assetId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var existingTenant, existingName string
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, name FROM asset WHERE id = $1", assetId).Scan(&existingTenant, &existingName); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Asset not found")
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
	tx.Exec("DELETE FROM attribute_kv WHERE entity_id = $1", assetId)
	if _, err := tx.Exec("DELETE FROM asset WHERE id = $1", assetId); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete asset")
		return
	}
	tx.Commit()
	audit.EntityChange(claims, "ASSET", assetId, existingName, "DELETED")
	w.WriteHeader(http.StatusOK)
}

// SaveProfile — POST /api/assetProfile (create or update).
func SaveProfile(w http.ResponseWriter, r *http.Request) {
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
	description, _ := body["description"].(string)
	isDefault, _ := body["default"].(bool)

	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		var existingTenant string
		if err := dbpkg.Pool.QueryRow("SELECT tenant_id FROM asset_profile WHERE id = $1", id).Scan(&existingTenant); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "Asset profile not found")
			return
		}
		if existingTenant != tenantId {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
			return
		}
		_, err := dbpkg.Pool.Exec(`
			UPDATE asset_profile SET name = $1, description = $2, is_default = $3,
			    version = COALESCE(version, 1) + 1 WHERE id = $4`,
			name, dbutil.NullStr(description), isDefault, id)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update profile")
			return
		}
		audit.EntityChange(claims, "ASSET_PROFILE", id, name, "UPDATED")
		w.WriteHeader(http.StatusOK)
		return
	}
	id = uuid.New().String()
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO asset_profile (id, created_time, name, description, is_default, tenant_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, 1)`,
		id, now, name, dbutil.NullStr(description), isDefault, tenantId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create profile")
		return
	}
	audit.EntityChange(claims, "ASSET_PROFILE", id, name, "ADDED")
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "ASSET_PROFILE", "id": id},
		"createdTime": now,
		"name":        name,
		"default":     isDefault,
	})
}

// DeleteProfile — DELETE /api/assetProfile/{id}.
// Refuses to delete the tenant's default profile.
func DeleteProfile(w http.ResponseWriter, r *http.Request, profileId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var existingTenant, existingName string
	var isDefault bool
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, name, COALESCE(is_default, false) FROM asset_profile WHERE id = $1", profileId).Scan(&existingTenant, &existingName, &isDefault); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Asset profile not found")
		return
	}
	if existingTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant delete denied")
		return
	}
	if isDefault {
		httputil.WriteError(w, http.StatusBadRequest, "Cannot delete default asset profile")
		return
	}
	if _, err := dbpkg.Pool.Exec("DELETE FROM asset_profile WHERE id = $1", profileId); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete profile")
		return
	}
	audit.EntityChange(claims, "ASSET_PROFILE", profileId, existingName, "DELETED")
	w.WriteHeader(http.StatusOK)
}
