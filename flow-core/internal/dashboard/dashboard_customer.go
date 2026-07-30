package dashboard

import (
	"encoding/json"
	"log"
	"math"
	"net/http"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// ListByCustomer — GET /api/customer/{customerId}/dashboards.
//
// Dashboard→customer is many-to-many and lives in the `assigned_customers` JSON
// column, so this filters and paginates in Go rather than in SQL: the column is a
// plain varchar and a `::jsonb` cast would hard-fail the whole query on any row
// with legacy/invalid content. Dashboards per tenant number in the tens, so
// scanning them is cheap and cannot blow up on bad data.
func ListByCustomer(w http.ResponseWriter, r *http.Request, customerId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	pageSize := httputil.IntParam(r, "pageSize", 10)
	page := httputil.IntParam(r, "page", 0)
	if pageSize <= 0 {
		pageSize = 10
	}

	rows, err := dbpkg.Pool.Query(
		`SELECT id, created_time, title, assigned_customers, mobile_hide, mobile_order, image, version
		   FROM dashboard WHERE tenant_id = $1 ORDER BY title ASC`, tenantId)
	if err != nil {
		log.Printf("ERROR querying dashboards for customer %s: %v", customerId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	matched := []map[string]interface{}{}
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
		if !assignedTo(assignedCustomers, customerId) {
			continue
		}
		matched = append(matched, buildSummary(id, tenantId, title, createdTime, mobileHide, version, image, mobileOrder, assignedCustomers))
	}

	totalElements := len(matched)
	start := page * pageSize
	if start > totalElements {
		start = totalElements
	}
	end := start + pageSize
	if end > totalElements {
		end = totalElements
	}
	totalPages := int(math.Ceil(float64(totalElements) / float64(pageSize)))

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          matched[start:end],
		"totalPages":    totalPages,
		"totalElements": totalElements,
		"hasNext":       (page + 1) < totalPages,
	})
}

// ListCustomers — GET /api/dashboard/{dashboardId}/customers. The inverse view of
// ListByCustomer: which customers this dashboard is shared with. The UI shows it
// in the dashboard's "Manage assigned customers" dialog, so the titles must be
// read live from the customer table rather than trusted from the denormalised
// copy in assigned_customers (a customer rename would otherwise show stale).
func ListCustomers(w http.ResponseWriter, r *http.Request, dashboardId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var rowTenant string
	var assigned *string
	err := dbpkg.Pool.QueryRow(
		`SELECT tenant_id::text, assigned_customers FROM dashboard WHERE id = $1`,
		dashboardId).Scan(&rowTenant, &assigned)
	if err != nil || rowTenant != tenantId {
		// 404 rather than 403 on cross-tenant, matching ByID — don't leak existence.
		httputil.WriteError(w, http.StatusNotFound, "Dashboard not found")
		return
	}

	data := []map[string]interface{}{}
	for _, cid := range assignedCustomerIDs(assigned) {
		var title string
		var isPublic bool
		if err := dbpkg.Pool.QueryRow(
			`SELECT COALESCE(title,''), COALESCE(is_public,false) FROM customer WHERE id = $1 AND tenant_id = $2`,
			cid, tenantId).Scan(&title, &isPublic); err != nil {
			// Customer deleted without cleaning the dashboard's array — skip it
			// rather than emitting a half-built row the UI would render blank.
			continue
		}
		data = append(data, map[string]interface{}{
			"id":       map[string]interface{}{"entityType": "CUSTOMER", "id": cid},
			"tenantId": map[string]interface{}{"entityType": "TENANT", "id": tenantId},
			"title":    title,
			"name":     title,
			"isPublic": isPublic,
		})
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          data,
		"totalPages":    1,
		"totalElements": len(data),
		"hasNext":       false,
	})
}

// assignedCustomerIDs returns the customer ids named in the assigned_customers JSON.
func assignedCustomerIDs(assignedCustomers *string) []string {
	if assignedCustomers == nil || *assignedCustomers == "" {
		return nil
	}
	var entries []struct {
		CustomerID struct {
			ID string `json:"id"`
		} `json:"customerId"`
	}
	if err := json.Unmarshal([]byte(*assignedCustomers), &entries); err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.CustomerID.ID != "" {
			out = append(out, e.CustomerID.ID)
		}
	}
	return out
}

// assignedTo reports whether the assigned_customers JSON names customerId.
func assignedTo(assignedCustomers *string, customerId string) bool {
	if assignedCustomers == nil || *assignedCustomers == "" || customerId == "" {
		return false
	}
	var entries []struct {
		CustomerID struct {
			ID string `json:"id"`
		} `json:"customerId"`
	}
	if err := json.Unmarshal([]byte(*assignedCustomers), &entries); err != nil {
		return false
	}
	for _, e := range entries {
		if e.CustomerID.ID == customerId {
			return true
		}
	}
	return false
}
