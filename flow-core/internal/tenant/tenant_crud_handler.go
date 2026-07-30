package tenant

// SYS_ADMIN-scoped CRUD over the tenant table.
//
// Surface mirrored from TB classic's TenantController:
//   GET    /api/tenants?pageSize=&page=&textSearch=
//   GET    /api/tenantInfos?pageSize=&page=&textSearch=
//   GET    /api/tenantInfo/{id}
//   POST   /api/tenant            (create or update)
//   DELETE /api/tenant/{id}       (cascade across tenant-scoped tables)
//
// All endpoints require a JWT with scope SYS_ADMIN. The bridge already
// holds the scopes in jwt.MapClaims; we enforce here.

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
)

func tenantRowToMap(id, title string, createdTime int64, profileId, region, country, state, city, address, address2, zip, phone, email, additionalInfo *string, version *int64, profileName *string) map[string]interface{} {
	out := map[string]interface{}{
		"id":          map[string]string{"entityType": "TENANT", "id": id},
		"createdTime": createdTime,
		"title":       title,
		"name":        title,
	}
	httputil.SetOptional(out, "region", region)
	httputil.SetOptional(out, "country", country)
	httputil.SetOptional(out, "state", state)
	httputil.SetOptional(out, "city", city)
	httputil.SetOptional(out, "address", address)
	httputil.SetOptional(out, "address2", address2)
	httputil.SetOptional(out, "zip", zip)
	httputil.SetOptional(out, "phone", phone)
	httputil.SetOptional(out, "email", email)
	if profileId != nil {
		out["tenantProfileId"] = map[string]string{"entityType": "TENANT_PROFILE", "id": *profileId}
	}
	if profileName != nil && *profileName != "" {
		out["tenantProfileName"] = *profileName
	}
	if version != nil {
		out["version"] = *version
	} else {
		out["version"] = 1
	}
	if additionalInfo != nil && *additionalInfo != "" {
		var parsed interface{}
		_ = json.Unmarshal([]byte(*additionalInfo), &parsed)
		out["additionalInfo"] = parsed
	} else {
		out["additionalInfo"] = nil
	}
	return out
}

// HandleTenantsList — GET /api/tenants
func HandleTenantsList(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}

	q := r.URL.Query()
	pageSize, _ := strconv.Atoi(q.Get("pageSize"))
	if pageSize <= 0 {
		pageSize = 10
	}
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 0 {
		page = 0
	}
	textSearch := strings.TrimSpace(q.Get("textSearch"))
	includeInfo := strings.HasSuffix(r.URL.Path, "Infos") // /api/tenantInfos

	whereClause := ""
	args := []interface{}{}
	if textSearch != "" {
		whereClause = " WHERE LOWER(t.title) LIKE LOWER($1) "
		args = append(args, "%"+textSearch+"%")
	}

	// total count
	var total int
	countSQL := "SELECT count(*) FROM tenant t" + whereClause
	if err := dbpkg.Pool.QueryRow(countSQL, args...).Scan(&total); err != nil {
		log.Printf("ERROR tenant count: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}

	args = append(args, pageSize, page*pageSize)
	limitSQL := fmt.Sprintf(" ORDER BY t.created_time DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))

	rows, err := dbpkg.Pool.Query(`
		SELECT t.id::text, t.created_time, t.title,
		       t.tenant_profile_id::text, COALESCE(t.region,''), COALESCE(t.country,''),
		       COALESCE(t.state,''), COALESCE(t.city,''),
		       COALESCE(t.address,''), COALESCE(t.address2,''),
		       COALESCE(t.zip,''), COALESCE(t.phone,''),
		       COALESCE(t.email,''), COALESCE(t.additional_info,''),
		       COALESCE(t.version, 1), COALESCE(tp.name,'')
		  FROM tenant t
		  LEFT JOIN tenant_profile tp ON tp.id = t.tenant_profile_id`+
		whereClause+limitSQL, args...)
	if err != nil {
		log.Printf("ERROR tenant list: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, title, profileId, region, country, state, city, address, address2, zip, phone, email, additionalInfo, profileName string
		var createdTime, version int64
		if err := rows.Scan(&id, &createdTime, &title, &profileId, &region, &country, &state, &city, &address, &address2, &zip, &phone, &email, &additionalInfo, &version, &profileName); err != nil {
			log.Printf("WARN tenant scan: %v", err)
			continue
		}
		strPtr := func(s string) *string {
			if s == "" {
				return nil
			}
			v := s
			return &v
		}
		intPtr := func(i int64) *int64 { return &i }
		row := tenantRowToMap(id, title, createdTime, strPtr(profileId), strPtr(region), strPtr(country), strPtr(state), strPtr(city), strPtr(address), strPtr(address2), strPtr(zip), strPtr(phone), strPtr(email), strPtr(additionalInfo), intPtr(version), strPtr(profileName))
		if !includeInfo {
			delete(row, "tenantProfileName")
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

// HandleTenantInfoById — GET /api/tenantInfo/{id}
func HandleTenantInfoById(w http.ResponseWriter, r *http.Request, tenantId string) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}
	var id, title, profileId, region, country, state, city, address, address2, zip, phone, email, additionalInfo, profileName string
	var createdTime, version int64
	err := dbpkg.Pool.QueryRow(`
		SELECT t.id::text, t.created_time, t.title,
		       t.tenant_profile_id::text, COALESCE(t.region,''), COALESCE(t.country,''),
		       COALESCE(t.state,''), COALESCE(t.city,''),
		       COALESCE(t.address,''), COALESCE(t.address2,''),
		       COALESCE(t.zip,''), COALESCE(t.phone,''),
		       COALESCE(t.email,''), COALESCE(t.additional_info,''),
		       COALESCE(t.version, 1), COALESCE(tp.name,'')
		  FROM tenant t
		  LEFT JOIN tenant_profile tp ON tp.id = t.tenant_profile_id
		 WHERE t.id = $1`, tenantId).Scan(
		&id, &createdTime, &title, &profileId, &region, &country, &state, &city,
		&address, &address2, &zip, &phone, &email, &additionalInfo, &version, &profileName)
	if err == sql.ErrNoRows {
		httputil.WriteError(w, http.StatusNotFound, "Tenant not found")
		return
	}
	if err != nil {
		log.Printf("ERROR tenantInfo: %v", err)
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
	intPtr := func(i int64) *int64 { return &i }
	row := tenantRowToMap(id, title, createdTime, strPtr(profileId), strPtr(region), strPtr(country), strPtr(state), strPtr(city), strPtr(address), strPtr(address2), strPtr(zip), strPtr(phone), strPtr(email), strPtr(additionalInfo), intPtr(version), strPtr(profileName))
	httputil.WriteJSON(w, http.StatusOK, row)
}

// HandleTenantSave — POST /api/tenant
func HandleTenantSave(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}
	title, _ := body["title"].(string)
	if strings.TrimSpace(title) == "" {
		httputil.WriteError(w, http.StatusBadRequest, "title is required")
		return
	}

	id := httputil.ExtractEntityID(body, "id")
	profileId := httputil.ExtractEntityID(body, "tenantProfileId")
	if profileId == "" {
		// Resolve default profile so the FK never fails on a fresh tenant.
		if err := dbpkg.Pool.QueryRow(
			`SELECT id::text FROM tenant_profile WHERE is_default = true LIMIT 1`,
		).Scan(&profileId); err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "No default tenant profile available")
			return
		}
	}

	getStr := func(k string) interface{} {
		if v, ok := body[k].(string); ok && v != "" {
			return v
		}
		return nil
	}
	additionalInfoJSON := dbutil.JSONOrNil(body["additionalInfo"])
	now := time.Now().UnixMilli()

	if id == "" {
		id = uuid.New().String()
		_, err := dbpkg.Pool.Exec(`
			INSERT INTO tenant (id, created_time, tenant_profile_id, title,
			                   region, country, state, city, address, address2,
			                   zip, phone, email, additional_info)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			id, now, profileId, title,
			getStr("region"), getStr("country"), getStr("state"), getStr("city"),
			getStr("address"), getStr("address2"), getStr("zip"), getStr("phone"),
			getStr("email"), additionalInfoJSON,
		)
		if err != nil {
			log.Printf("ERROR tenant insert: %v", err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to insert tenant")
			return
		}
		log.Printf("Tenant CREATED: id=%s title=%s", id, title)
	} else {
		res, err := dbpkg.Pool.Exec(`
			UPDATE tenant SET tenant_profile_id = $1, title = $2,
			                  region = $3, country = $4, state = $5, city = $6,
			                  address = $7, address2 = $8, zip = $9, phone = $10,
			                  email = $11, additional_info = $12,
			                  version = COALESCE(version, 1) + 1
			              WHERE id = $13`,
			profileId, title,
			getStr("region"), getStr("country"), getStr("state"), getStr("city"),
			getStr("address"), getStr("address2"), getStr("zip"), getStr("phone"),
			getStr("email"), additionalInfoJSON, id,
		)
		if err != nil {
			log.Printf("ERROR tenant update: %v", err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update tenant")
			return
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			httputil.WriteError(w, http.StatusNotFound, "Tenant not found")
			return
		}
		log.Printf("Tenant UPDATED: id=%s title=%s", id, title)
	}

	HandleTenantById(w, r, id)
}

// HandleTenantDelete — DELETE /api/tenant/{id}
//
// Cascades across the tenant-scoped tables in a single transaction so the
// row removal is all-or-nothing. We trust postgres FK CASCADEs where they
// exist (alarm_types → tenant) and explicitly delete the rest.
func HandleTenantDelete(w http.ResponseWriter, r *http.Request, tenantId string) {
	if _, ok := httputil.RequireSysAdmin(w, r); !ok {
		return
	}

	// Refuse to delete the system tenant — it's the bootstrap row that owns
	// admin_settings, sysadmin user, default tenant profile, etc.
	if tenantId == "13814000-1dd2-11b2-8080-808080808080" {
		httputil.WriteError(w, http.StatusBadRequest, "Cannot delete the system tenant")
		return
	}

	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		log.Printf("ERROR tenant delete tx begin: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer tx.Rollback()

	// Ordered: tables with FK to other tenant-scoped tables first.
	// Note: alarm_comment is NOT listed — it has no tenant_id column. It
	// gets cascade-deleted by the FK on alarm_id when we delete from alarm.
	cascadeTables := []string{
		"alarm", // FK references alarm_types via fk_entity_tenant_id; alarm_types CASCADE on tenant
		"entity_alarm",
		"audit_log",
		"calculated_field",
		"device_credentials",
		"device",
		"device_profile",
		"asset",
		"asset_profile",
		"entity_view",
		"customer",
		"dashboard",
		"rule_chain",
		"widget_type",
		"widgets_bundle",
		"resource",
		"ota_package",
		"queue",
		"queue_stats",
		"oauth2_client",
		"domain",
		"mobile_app",
		"mobile_app_bundle",
		"notification_template",
		"notification_target",
		"notification_rule",
		"notification_request",
		"qr_code_settings",
		"api_key",
		"api_usage_state",
		"ai_model",
		"job",
		"edge",
		"rpc",
		"admin_settings",
		"tb_user", // last among entities — user_credentials cascades via FK
	}
	// Each table gets its own savepoint so a missing column or non-existent
	// table doesn't abort the whole transaction (postgres marks the tx as
	// errored on the FIRST failed statement until ROLLBACK TO SAVEPOINT).
	for _, t := range cascadeTables {
		spName := "sp_" + t
		if _, err := tx.Exec("SAVEPOINT " + spName); err != nil {
			log.Printf("WARN tenant delete savepoint %s: %v", t, err)
			continue
		}
		_, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE tenant_id = $1", t), tenantId)
		if err != nil {
			log.Printf("WARN tenant delete cascade %s: %v (rolling back savepoint)", t, err)
			if _, rerr := tx.Exec("ROLLBACK TO SAVEPOINT " + spName); rerr != nil {
				log.Printf("ERROR rollback savepoint %s: %v", t, rerr)
			}
			continue
		}
		_, _ = tx.Exec("RELEASE SAVEPOINT " + spName)
	}

	// Finally, drop the tenant row itself.
	res, err := tx.Exec("DELETE FROM tenant WHERE id = $1", tenantId)
	if err != nil {
		log.Printf("ERROR tenant row delete: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete tenant row")
		return
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		httputil.WriteError(w, http.StatusNotFound, "Tenant not found")
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("ERROR tenant delete commit: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Commit failed")
		return
	}
	log.Printf("Tenant DELETED (cascade): id=%s", tenantId)
	w.WriteHeader(http.StatusOK)
}
