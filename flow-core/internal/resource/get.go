package resource

import (
	"database/sql"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// GetByID — GET /api/resource/{id} and GET /api/resource/info/{id}.
//
// Both return the resource metadata. Neither returns `data`: a resource row
// holds the whole file (JS modules, LWM2M models, dashboard images), and the UI
// fetches bytes through the download endpoint. Inlining a multi-megabyte blob
// into a metadata response would be paid on every list render.
func GetByID(w http.ResponseWriter, r *http.Request, id string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	var (
		title, resourceType   string
		createdTime           int64
		resourceKey, fileName sql.NullString
		etag, descriptor      sql.NullString
		subType               sql.NullString
		isPublic              sql.NullBool
		publicKey             sql.NullString
		rowTenant             string
	)
	err := dbpkg.Pool.QueryRow(`
		SELECT title, resource_type, created_time, resource_key, file_name,
		       etag, descriptor, resource_sub_type, is_public, public_resource_key,
		       tenant_id::text
		  FROM resource WHERE id = $1`, id).
		Scan(&title, &resourceType, &createdTime, &resourceKey, &fileName,
			&etag, &descriptor, &subType, &isPublic, &publicKey, &rowTenant)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Resource not found")
		return
	}
	// System resources (the widget bundles seeded at install) carry the null
	// tenant and are readable by every tenant; anything else must match.
	if rowTenant != tenantID && !isSystemTenant(rowTenant) {
		httputil.WriteError(w, http.StatusNotFound, "Resource not found")
		return
	}

	httputil.WriteJSON(w, http.StatusOK, resourceJSON(
		id, rowTenant, title, resourceType, createdTime,
		resourceKey, fileName, etag, descriptor, subType, isPublic, publicKey))
}

// ListByTenant — GET /api/resource/tenant. The tenant's own resources, without
// the system-owned ones the other listings mix in.
func ListByTenant(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)
	pageSize := httputil.PageSize(r, 10)
	page := httputil.IntParam(r, "page", 0)
	if pageSize <= 0 {
		pageSize = 10
	}
	textSearch := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("textSearch")))

	where := "tenant_id = $1"
	args := []interface{}{tenantID}
	if textSearch != "" {
		where += " AND LOWER(title) LIKE $2"
		args = append(args, "%"+textSearch+"%")
	}

	var total int
	_ = dbpkg.Pool.QueryRow("SELECT count(*) FROM resource WHERE "+where, args...).Scan(&total)

	rows, err := dbpkg.Pool.Query(`
		SELECT id::text, title, resource_type, created_time, resource_key, file_name,
		       etag, descriptor, resource_sub_type, is_public, public_resource_key
		  FROM resource WHERE `+where+` ORDER BY title ASC LIMIT $`+
		strconv.Itoa(len(args)+1)+` OFFSET $`+strconv.Itoa(len(args)+2),
		append(args, pageSize, page*pageSize)...)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, title, rtype string
		var createdTime int64
		var rkey, fname, etag, desc, sub, pubKey sql.NullString
		var isPublic sql.NullBool
		if err := rows.Scan(&id, &title, &rtype, &createdTime, &rkey, &fname,
			&etag, &desc, &sub, &isPublic, &pubKey); err != nil {
			continue
		}
		data = append(data, resourceJSON(id, tenantID, title, rtype, createdTime,
			rkey, fname, etag, desc, sub, isPublic, pubKey))
	}

	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

func resourceJSON(id, tenantID, title, resourceType string, createdTime int64,
	resourceKey, fileName, etag, descriptor, subType sql.NullString,
	isPublic sql.NullBool, publicKey sql.NullString) map[string]interface{} {
	out := map[string]interface{}{
		"id":           map[string]interface{}{"entityType": "TB_RESOURCE", "id": id},
		"createdTime":  createdTime,
		"tenantId":     map[string]interface{}{"entityType": "TENANT", "id": tenantID},
		"title":        title,
		"resourceType": resourceType,
		"resourceKey":  resourceKey.String,
		"fileName":     fileName.String,
		"etag":         etag.String,
		"public":       isPublic.Bool,
	}
	if subType.Valid && subType.String != "" {
		out["resourceSubType"] = subType.String
	}
	if publicKey.Valid && publicKey.String != "" {
		out["publicResourceKey"] = publicKey.String
	}
	if descriptor.Valid && descriptor.String != "" {
		var v interface{}
		if err := json.Unmarshal([]byte(descriptor.String), &v); err == nil {
			out["descriptor"] = v
		}
	}
	return out
}

// isSystemTenant reports the reserved all-zero/TB system tenant id, whose
// resources every tenant may read.
func isSystemTenant(id string) bool {
	return id == "" || strings.HasPrefix(id, "13814000-1dd2-11b2")
}
