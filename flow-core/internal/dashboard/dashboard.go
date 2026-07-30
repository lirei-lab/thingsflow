// Package dashboard implements the HTTP surface for /api/dashboard*,
// /api/tenant/dashboards, /api/user/dashboards, and /api/dashboard/home.
//
// First domain extracted from package main as part of breaking up the
// monolithic handler layer. Pattern to follow for the remaining domains:
// each package owns its routes' handlers + helpers, reaches for shared
// infra through internal/db, internal/audit, internal/quotas, etc.
package dashboard

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/quotas"
)

// ListByTenant — GET /api/tenant/dashboards (paginated).
//
// /api/dashboards and /api/customer/dashboards route here too — they all
// return the same tenant-scoped list, with optional textSearch + sort.
func ListByTenant(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	textSearch := r.URL.Query().Get("textSearch")
	sortProperty := r.URL.Query().Get("sortProperty")
	sortOrder := r.URL.Query().Get("sortOrder")

	// Whitelist sort columns to keep the SQL safe from injection.
	allowedSort := map[string]string{
		"title":       "title",
		"createdTime": "created_time",
	}
	sortCol := allowedSort[sortProperty]
	if sortCol == "" {
		sortCol = "title"
	}
	dir := "ASC"
	if strings.ToUpper(sortOrder) == "DESC" {
		dir = "DESC"
	}

	var totalElements int
	countQuery := "SELECT count(*) FROM dashboard WHERE tenant_id = $1"
	countArgs := []interface{}{tenantId}
	if textSearch != "" {
		countQuery += " AND LOWER(title) LIKE $2"
		countArgs = append(countArgs, "%"+strings.ToLower(textSearch)+"%")
	}
	dbpkg.Pool.QueryRow(countQuery, countArgs...).Scan(&totalElements)

	offset := page * pageSize
	query := "SELECT id, created_time, title, assigned_customers, mobile_hide, mobile_order, image, version FROM dashboard WHERE tenant_id = $1"
	args := []interface{}{tenantId}
	argIdx := 2
	if textSearch != "" {
		query += " AND LOWER(title) LIKE $2"
		args = append(args, "%"+strings.ToLower(textSearch)+"%")
		argIdx++
	}
	query += " ORDER BY " + sortCol + " " + dir + " LIMIT $" + strconv.Itoa(argIdx) + " OFFSET $" + strconv.Itoa(argIdx+1)
	args = append(args, pageSize, offset)

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("ERROR querying dashboards: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, title string
		var createdTime int64
		var assignedCustomers, image *string
		var mobileHide *bool
		var mobileOrder *int
		var version *int64

		if err := rows.Scan(&id, &createdTime, &title, &assignedCustomers, &mobileHide, &mobileOrder, &image, &version); err != nil {
			log.Printf("ERROR scanning dashboard row: %v", err)
			continue
		}
		data = append(data, buildSummary(id, tenantId, title, createdTime, mobileHide, version, image, mobileOrder, assignedCustomers))
	}

	totalPages := int(math.Ceil(float64(totalElements) / float64(pageSize)))
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": totalElements,
		"hasNext":       (page + 1) < totalPages,
	})
}

// ByID — GET /api/dashboard/{id}. Returns the full dashboard JSON,
// including parsed configuration. Cross-tenant access returns 404 (not
// 403) on purpose — TB classic does the same to avoid leaking existence.
func ByID(w http.ResponseWriter, r *http.Request, dashboardId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var id, title string
	var createdTime int64
	var configuration, assignedCustomers, image *string
	var mobileHide *bool
	var mobileOrder *int
	var version *int64

	err := dbpkg.Pool.QueryRow(`
		SELECT id, created_time, title, configuration, assigned_customers, mobile_hide, mobile_order, image, version
		FROM dashboard WHERE id = $1 AND tenant_id = $2`, dashboardId, tenantId).Scan(
		&id, &createdTime, &title, &configuration, &assignedCustomers, &mobileHide, &mobileOrder, &image, &version,
	)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Dashboard not found")
		return
	}

	result := buildSummary(id, tenantId, title, createdTime, mobileHide, version, image, mobileOrder, assignedCustomers)
	if configuration != nil && *configuration != "" {
		var config interface{}
		if err := json.Unmarshal([]byte(*configuration), &config); err == nil {
			result["configuration"] = config
		}
	}
	httputil.WriteJSON(w, http.StatusOK, result)
}

// Save — POST /api/dashboard. Creates if body has no id, updates otherwise.
// Returns the persisted dashboard via ByID.
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

	title, _ := body["title"].(string)
	if title == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing dashboard title")
		return
	}
	mobileHide, _ := body["mobileHide"].(bool)
	configurationJSON := dbutil.JSONOrNil(body["configuration"])
	assignedCustomersJSON := dbutil.JSONOrNil(body["assignedCustomers"])

	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		var existingTenant string
		if err := dbpkg.Pool.QueryRow("SELECT tenant_id FROM dashboard WHERE id = $1", id).Scan(&existingTenant); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "Dashboard not found")
			return
		}
		if existingTenant != tenantId {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
			return
		}
		_, err := dbpkg.Pool.Exec(`
			UPDATE dashboard
			SET title = $1, configuration = $2, assigned_customers = $3,
			    mobile_hide = $4, version = COALESCE(version, 1) + 1
			WHERE id = $5`,
			title, configurationJSON, assignedCustomersJSON, mobileHide, id)
		if err != nil {
			log.Printf("ERROR updating dashboard %s: %v", id, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update dashboard")
			return
		}
		audit.EntityChange(claims, "DASHBOARD", id, title, "UPDATED")
		ByID(w, r, id)
		return
	}

	if !quotas.Enforce(w, tenantId, "dashboard") {
		return
	}
	id = uuid.New().String()
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO dashboard (id, created_time, title, configuration, assigned_customers,
		                      mobile_hide, tenant_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 1)`,
		id, now, title, configurationJSON, assignedCustomersJSON, mobileHide, tenantId)
	if err != nil {
		log.Printf("ERROR creating dashboard: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create dashboard")
		return
	}
	audit.EntityChange(claims, "DASHBOARD", id, title, "ADDED")
	ByID(w, r, id)
}

// Delete — DELETE /api/dashboard/{id}.
func Delete(w http.ResponseWriter, r *http.Request, dashboardId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var existingTenant, existingTitle string
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, title FROM dashboard WHERE id = $1", dashboardId).Scan(&existingTenant, &existingTitle); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Dashboard not found")
		return
	}
	if existingTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant delete denied")
		return
	}
	if _, err := dbpkg.Pool.Exec("DELETE FROM dashboard WHERE id = $1", dashboardId); err != nil {
		log.Printf("ERROR deleting dashboard %s: %v", dashboardId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete dashboard")
		return
	}
	audit.EntityChange(claims, "DASHBOARD", dashboardId, existingTitle, "DELETED")
	w.WriteHeader(http.StatusOK)
}

// UserList — GET /api/user/dashboards.
//
// TB returns {last:[{id,title,starred,lastVisited}], starred:[]} — NOT a
// PageData. We don't track stars yet so starred is always empty; "last"
// is the most-recently-created 10 tenant dashboards as a stand-in.
func UserList(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	rows, err := dbpkg.Pool.Query(
		`SELECT id, title, created_time FROM dashboard
		 WHERE tenant_id = $1 ORDER BY created_time DESC LIMIT 10`, tenantId)
	last := []map[string]interface{}{}
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id, title string
			var createdTime int64
			if err := rows.Scan(&id, &title, &createdTime); err == nil {
				last = append(last, map[string]interface{}{
					"id":          id,
					"title":       title,
					"starred":     false,
					"lastVisited": createdTime,
				})
			}
		}
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"last":    last,
		"starred": []interface{}{},
	})
}

// Visit — GET /api/user/dashboards/{id}/VISIT.
// TB analytics ping. Always 200 — we don't record anything.
func Visit(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// Home — GET /api/dashboard/home.
//
// TB returns an empty body (no JSON) when the user has no home dashboard.
// The literal string "null" — even though it's valid JSON — desyncs the
// UI's RxJS pipeline, so we deliberately write nothing.
func Home(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ─── helpers ────────────────────────────────────────────────────────────────

// buildSummary produces the slim dashboard projection used by ListByTenant
// and the base of ByID. Keeps both paths field-for-field consistent.
func buildSummary(id, tenantId, title string, createdTime int64,
	mobileHide *bool, version *int64, image *string, mobileOrder *int,
	assignedCustomers *string,
) map[string]interface{} {
	out := map[string]interface{}{
		"id": map[string]interface{}{
			"entityType": "DASHBOARD",
			"id":         id,
		},
		"createdTime": createdTime,
		"tenantId": map[string]interface{}{
			"entityType": "TENANT",
			"id":         tenantId,
		},
		"title":      title,
		"name":       title,
		"mobileHide": mobileHide != nil && *mobileHide,
	}
	if version != nil {
		out["version"] = *version
	} else {
		out["version"] = 1
	}
	if image != nil {
		out["image"] = *image
	} else {
		out["image"] = nil
	}
	if mobileOrder != nil {
		out["mobileOrder"] = *mobileOrder
	} else {
		out["mobileOrder"] = nil
	}
	if assignedCustomers != nil && *assignedCustomers != "" {
		var ac interface{}
		_ = json.Unmarshal([]byte(*assignedCustomers), &ac)
		out["assignedCustomers"] = ac
	} else {
		out["assignedCustomers"] = nil
	}
	return out
}
