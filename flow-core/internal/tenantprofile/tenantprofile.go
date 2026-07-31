package tenantprofile

// SYS_ADMIN-scoped CRUD over the tenant_profile table.
//
// Surface mirrored from TB classic's TenantProfileController:
//   GET    /api/tenantProfiles?pageSize=&page=&textSearch=
//   GET    /api/tenantProfileInfos?pageSize=&page=&textSearch=
//   GET    /api/tenantProfile/{id}
//   GET    /api/tenantProfileInfo/{id}
//   POST   /api/tenantProfile           (create or update)
//   DELETE /api/tenantProfile/{id}
//   POST   /api/tenantProfile/{id}/default
//
// All endpoints require a JWT with scope SYS_ADMIN.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/quotas"
)

func tenantProfileRow(id, name string, createdTime int64, profileData *string, description *string, isDefault, isolatedTbCore, isolatedRE bool, includeData bool) map[string]interface{} {
	row := map[string]interface{}{
		"id":                   map[string]string{"entityType": "TENANT_PROFILE", "id": id},
		"createdTime":          createdTime,
		"name":                 name,
		"default":              isDefault,
		"isolatedTbCore":       isolatedTbCore,
		"isolatedTbRuleEngine": isolatedRE,
	}
	if description != nil && *description != "" {
		row["description"] = *description
	} else {
		row["description"] = ""
	}
	if includeData {
		if profileData != nil && *profileData != "" {
			row["profileData"] = json.RawMessage(*profileData)
		} else {
			row["profileData"] = map[string]interface{}{}
		}
	}
	return row
}

// List — GET /api/tenantProfiles or /api/tenantProfileInfos
func List(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	q := r.URL.Query()
	pageSize := httputil.PageSize(r, 10)
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 0 {
		page = 0
	}
	textSearch := strings.TrimSpace(q.Get("textSearch"))
	includeInfo := strings.Contains(r.URL.Path, "tenantProfileInfos")

	whereClause := ""
	args := []interface{}{}
	if textSearch != "" {
		whereClause = " WHERE LOWER(name) LIKE LOWER($1) "
		args = append(args, "%"+textSearch+"%")
	}

	var total int
	if err := dbpkg.Pool.QueryRow("SELECT count(*) FROM tenant_profile"+whereClause, args...).Scan(&total); err != nil {
		log.Printf("ERROR tenantProfile count: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}

	args = append(args, pageSize, page*pageSize)
	limitSQL := fmt.Sprintf(" ORDER BY created_time DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))

	rows, err := dbpkg.Pool.Query(`
		SELECT id::text, created_time, name, profile_data::text,
		       COALESCE(description,''), COALESCE(is_default,false),
		       COALESCE(isolated_tb_core,false), COALESCE(isolated_tb_rule_engine,false)
		  FROM tenant_profile`+whereClause+limitSQL, args...)
	if err != nil {
		log.Printf("ERROR tenantProfile list: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, name, profileData, description string
		var createdTime int64
		var isDefault, isoCore, isoRE bool
		if err := rows.Scan(&id, &createdTime, &name, &profileData, &description, &isDefault, &isoCore, &isoRE); err != nil {
			log.Printf("WARN tenantProfile scan: %v", err)
			continue
		}
		strPtr := func(s string) *string {
			if s == "" {
				return nil
			}
			v := s
			return &v
		}
		// /tenantProfileInfos returns a slim projection (no profileData)
		row := tenantProfileRow(id, name, createdTime, strPtr(profileData), strPtr(description), isDefault, isoCore, isoRE, !includeInfo)
		if includeInfo {
			delete(row, "createdTime")
			delete(row, "description")
			delete(row, "isolatedTbCore")
			delete(row, "isolatedTbRuleEngine")
		}
		data = append(data, row)
	}

	totalPages := total / pageSize
	if total%pageSize != 0 {
		totalPages++
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page+1)*pageSize < total,
	})
}

// ByID — GET /api/tenantProfile/{id} (and /tenantProfileInfo/{id})
func ByID(w http.ResponseWriter, r *http.Request, profileId string, includeData bool) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	var id, name, profileData, description string
	var createdTime int64
	var isDefault, isoCore, isoRE bool
	err := dbpkg.Pool.QueryRow(`
		SELECT id::text, created_time, name, COALESCE(profile_data::text,''),
		       COALESCE(description,''), COALESCE(is_default,false),
		       COALESCE(isolated_tb_core,false), COALESCE(isolated_tb_rule_engine,false)
		  FROM tenant_profile WHERE id = $1`, profileId).Scan(
		&id, &createdTime, &name, &profileData, &description, &isDefault, &isoCore, &isoRE)
	if err == sql.ErrNoRows {
		httputil.WriteError(w, http.StatusNotFound, "Tenant profile not found")
		return
	}
	if err != nil {
		log.Printf("ERROR tenantProfile by id: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	strPtr := func(s string) *string {
		if s == "" {
			return nil
		}
		v := s
		return &v
	}
	row := tenantProfileRow(id, name, createdTime, strPtr(profileData), strPtr(description), isDefault, isoCore, isoRE, includeData)
	httputil.WriteJSON(w, http.StatusOK, row)
}

// Save — POST /api/tenantProfile
func Save(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}
	name, _ := body["name"].(string)
	if strings.TrimSpace(name) == "" {
		httputil.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}

	id := httputil.ExtractEntityID(body, "id")
	description, _ := body["description"].(string)
	isolatedCore, _ := body["isolatedTbCore"].(bool)
	isolatedRE, _ := body["isolatedTbRuleEngine"].(bool)
	profileDataJSON := dbutil.JSONOrNil(body["profileData"])
	if profileDataJSON == nil {
		profileDataJSON = "{}"
	}
	now := time.Now().UnixMilli()

	if id == "" {
		id = uuid.New().String()
		_, err := dbpkg.Pool.Exec(`
			INSERT INTO tenant_profile (id, created_time, name, profile_data, description,
			                            is_default, isolated_tb_core, isolated_tb_rule_engine)
			VALUES ($1, $2, $3, $4::jsonb, $5, false, $6, $7)`,
			id, now, name, profileDataJSON, nullIfEmpty(description), isolatedCore, isolatedRE,
		)
		if err != nil {
			log.Printf("ERROR tenantProfile insert: %v", err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to insert tenant profile")
			return
		}
		log.Printf("TenantProfile CREATED: id=%s name=%s", id, name)
	} else {
		// isolation flags are immutable in TB classic — preserve current values
		res, err := dbpkg.Pool.Exec(`
			UPDATE tenant_profile
			   SET name = $1, profile_data = $2::jsonb, description = $3
			 WHERE id = $4`,
			name, profileDataJSON, nullIfEmpty(description), id,
		)
		if err != nil {
			log.Printf("ERROR tenantProfile update: %v", err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update tenant profile")
			return
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			httputil.WriteError(w, http.StatusNotFound, "Tenant profile not found")
			return
		}
		log.Printf("TenantProfile UPDATED: id=%s name=%s", id, name)
	}
	// Drop the entire quota cache so any tenant on this profile picks up
	// the new limits on its next create attempt, not 30s from now.
	quotas.InvalidateCache("")
	ByID(w, r, id, true)
}

// Delete — DELETE /api/tenantProfile/{id}
func Delete(w http.ResponseWriter, r *http.Request, profileId string) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	// TB classic refuses to delete a profile while tenants reference it,
	// and never lets you delete the default. We mirror both checks.
	var isDefault bool
	if err := dbpkg.Pool.QueryRow(`SELECT COALESCE(is_default,false) FROM tenant_profile WHERE id = $1`, profileId).Scan(&isDefault); err == sql.ErrNoRows {
		httputil.WriteError(w, http.StatusNotFound, "Tenant profile not found")
		return
	} else if err != nil {
		log.Printf("ERROR tenantProfile delete preflight: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	if isDefault {
		httputil.WriteError(w, http.StatusBadRequest, "Cannot delete the default tenant profile")
		return
	}
	var refs int
	if err := dbpkg.Pool.QueryRow(`SELECT count(*) FROM tenant WHERE tenant_profile_id = $1`, profileId).Scan(&refs); err != nil {
		log.Printf("ERROR tenantProfile delete refs check: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	if refs > 0 {
		httputil.WriteError(w, http.StatusBadRequest, fmt.Sprintf("Tenant profile is referenced by %d tenant(s)", refs))
		return
	}
	res, err := dbpkg.Pool.Exec(`DELETE FROM tenant_profile WHERE id = $1`, profileId)
	if err != nil {
		log.Printf("ERROR tenantProfile delete: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete tenant profile")
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		httputil.WriteError(w, http.StatusNotFound, "Tenant profile not found")
		return
	}
	log.Printf("TenantProfile DELETED: id=%s", profileId)
	w.WriteHeader(http.StatusOK)
}

// SetDefault — POST /api/tenantProfile/{id}/default
func SetDefault(w http.ResponseWriter, r *http.Request, profileId string) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`UPDATE tenant_profile SET is_default = false WHERE is_default = true`); err != nil {
		log.Printf("ERROR clearing default tenantProfile: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	res, err := tx.Exec(`UPDATE tenant_profile SET is_default = true WHERE id = $1`, profileId)
	if err != nil {
		log.Printf("ERROR setting default tenantProfile: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		httputil.WriteError(w, http.StatusNotFound, "Tenant profile not found")
		return
	}
	if err := tx.Commit(); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Commit failed")
		return
	}
	log.Printf("TenantProfile SET DEFAULT: id=%s", profileId)
	quotas.InvalidateCache("")
	ByID(w, r, profileId, true)
}

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
