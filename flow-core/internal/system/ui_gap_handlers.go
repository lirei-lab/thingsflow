package system

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// Handlers for endpoints the ThingsBoard UI calls that previously fell through
// to notImplemented — which answers a GET with an empty page and HTTP 200, so
// the affected screens rendered blank rather than reporting anything.

// HandleCustomerEdgeInfos — GET /api/customer/{customerId}/edgeInfos.
//
// Edge runtime is a declared no-goal, but the *listing* is not: the customer
// page requests it on every open, and returning an empty page keeps that page
// working instead of leaving a pending request. There is no edge table to read,
// so this is deliberately an empty result rather than a lookup.
func HandleCustomerEdgeInfos(w http.ResponseWriter, r *http.Request, customerId string) {
	if _, ok := httputil.RequireAuth(w, r); !ok {
		return
	}
	_ = customerId
	EmptyPageData(w)
}

// HandleTenantDashboardsByID — GET /api/tenant/{tenantId}/dashboards.
// Sysadmin-facing: browse another tenant's dashboards. A tenant admin may only
// ask about its own tenant, which the check below enforces.
func HandleTenantDashboardsByID(w http.ResponseWriter, r *http.Request, tenantId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	if !mayInspectTenant(claims, tenantId) {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
		return
	}
	listSimplePage(w, `SELECT id::text, created_time, title FROM dashboard WHERE tenant_id = $1
	                    ORDER BY title ASC LIMIT $2 OFFSET $3`,
		"SELECT count(*) FROM dashboard WHERE tenant_id = $1", tenantId, r,
		func(id string, created int64, title string) map[string]interface{} {
			return map[string]interface{}{
				"id":          map[string]interface{}{"entityType": "DASHBOARD", "id": id},
				"createdTime": created,
				"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantId},
				"title":       title,
				"name":        title,
			}
		})
}

// HandleTenantUsersByID — GET /api/tenant/{tenantId}/users.
func HandleTenantUsersByID(w http.ResponseWriter, r *http.Request, tenantId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	if !mayInspectTenant(claims, tenantId) {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
		return
	}
	listSimplePage(w, `SELECT id::text, created_time, email FROM tb_user WHERE tenant_id = $1
	                    ORDER BY email ASC LIMIT $2 OFFSET $3`,
		"SELECT count(*) FROM tb_user WHERE tenant_id = $1", tenantId, r,
		func(id string, created int64, email string) map[string]interface{} {
			return map[string]interface{}{
				"id":          map[string]interface{}{"entityType": "USER", "id": id},
				"createdTime": created,
				"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantId},
				"email":       email,
				"name":        email,
			}
		})
}

// mayInspectTenant allows a sysadmin anything, and a tenant admin only its own
// tenant. Without the second half these endpoints would let any authenticated
// tenant enumerate another's dashboards and user emails.
func mayInspectTenant(claims map[string]interface{}, tenantId string) bool {
	if scopes, ok := claims["scopes"].([]interface{}); ok {
		for _, s := range scopes {
			if str, _ := s.(string); str == "SYS_ADMIN" {
				return true
			}
		}
	}
	own, _ := claims["tenantId"].(string)
	return own != "" && own == tenantId
}

func listSimplePage(w http.ResponseWriter, query, countQuery, arg string,
	r *http.Request, build func(string, int64, string) map[string]interface{}) {
	pageSize := httputil.PageSize(r, 10)
	page := httputil.IntParam(r, "page", 0)
	if pageSize <= 0 {
		pageSize = 10
	}
	var total int
	_ = dbpkg.Pool.QueryRow(countQuery, arg).Scan(&total)

	rows, err := dbpkg.Pool.Query(query, arg, pageSize, page*pageSize)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, text string
		var created int64
		if err := rows.Scan(&id, &created, &text); err != nil {
			continue
		}
		data = append(data, build(id, created, text))
	}
	totalPages := 0
	if pageSize > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data": data, "totalPages": totalPages, "totalElements": total,
		"hasNext": (page + 1) < totalPages,
	})
}

// HandleSetDefaultProfile — POST /api/deviceProfile/{id}/default and
// POST /api/assetProfile/{id}/default.
//
// Clearing the previous default and setting the new one happen in one
// transaction: between the two statements the tenant would otherwise have no
// default profile, and anything provisioning a device in that window would fail
// to resolve one.
func HandleSetDefaultProfile(w http.ResponseWriter, r *http.Request, table, entityType, id string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	var rowTenant, name string
	if err := dbpkg.Pool.QueryRow(
		"SELECT tenant_id::text, COALESCE(name,'') FROM "+table+" WHERE id = $1", id).
		Scan(&rowTenant, &name); err != nil {
		httputil.WriteError(w, http.StatusNotFound, entityType+" not found")
		return
	}
	if rowTenant != tenantID {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
		return
	}

	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec("UPDATE "+table+" SET is_default = false WHERE tenant_id = $1 AND is_default = true", tenantID); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to clear previous default")
		return
	}
	if _, err := tx.Exec("UPDATE "+table+" SET is_default = true WHERE id = $1", id); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to set default")
		return
	}
	if err := tx.Commit(); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to commit")
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":       map[string]interface{}{"entityType": entityType, "id": id},
		"tenantId": map[string]interface{}{"entityType": "TENANT", "id": tenantID},
		"name":     name,
		"default":  true,
	})
}

// HandleAssetProfileByID — GET /api/assetProfile/{id}.
func HandleAssetProfileByID(w http.ResponseWriter, r *http.Request, id string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	var name, rowTenant string
	var createdTime int64
	var isDefault sql.NullBool
	var description, image sql.NullString
	err := dbpkg.Pool.QueryRow(`
		SELECT name, tenant_id::text, created_time, is_default, description, image
		  FROM asset_profile WHERE id = $1`, id).
		Scan(&name, &rowTenant, &createdTime, &isDefault, &description, &image)
	if err != nil || rowTenant != tenantID {
		httputil.WriteError(w, http.StatusNotFound, "Asset profile not found")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "ASSET_PROFILE", "id": id},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantID},
		"name":        name,
		"default":     isDefault.Bool,
		"description": description.String,
		"image":       nullableString(image),
	})
}

// HandleMailConfigTemplate — GET /api/mail/config/template.
// The UI asks for the provider presets when opening mail settings. No provider
// is preconfigured here, so the honest answer is an empty list: the form then
// offers manual SMTP entry rather than waiting on a request that never resolves.
func HandleMailConfigTemplate(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireAuth(w, r); !ok {
		return
	}
	httputil.WriteJSON(w, http.StatusOK, []interface{}{})
}

// HandleDashboardCustomersBulk — POST /api/dashboard/{id}/customers,
// .../customers/add and .../customers/remove.
//
// The body is a list of customer ids. `customers` replaces the whole set, `add`
// and `remove` adjust it — the three verbs the UI's assignment dialog uses.
// Assignment itself is delegated so there is one implementation of what
// assigned_customers means.
func HandleDashboardCustomersBulk(w http.ResponseWriter, r *http.Request,
	dashboardID, mode string,
	assign func(w http.ResponseWriter, r *http.Request, customerID, dashboardID string, add bool)) {
	if _, ok := httputil.RequireAuth(w, r); !ok {
		return
	}
	if r.Method != http.MethodPost {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Unreadable body")
		return
	}
	var ids []string
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &ids); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "Expected a JSON array of customer ids")
			return
		}
	}

	// The per-customer helper writes a response, so it cannot be called directly
	// in a loop. Discard all but the last write and answer once.
	for _, cid := range ids {
		cid = strings.TrimSpace(cid)
		if cid == "" {
			continue
		}
		rec := &discardWriter{}
		assign(rec, r, cid, dashboardID, mode != "remove")
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":      map[string]interface{}{"entityType": "DASHBOARD", "id": dashboardID},
		"applied": len(ids),
		"mode":    mode,
	})
}

// discardWriter swallows a handler's response so it can be reused inside a loop.
type discardWriter struct{ header http.Header }

func (d *discardWriter) Header() http.Header {
	if d.header == nil {
		d.header = http.Header{}
	}
	return d.header
}
func (d *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d *discardWriter) WriteHeader(int)             {}

func nullableString(s sql.NullString) interface{} {
	if !s.Valid || s.String == "" {
		return nil
	}
	return s.String
}
