// Package customer implements /api/customer CRUD.
package customer

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/quotas"
)

// Save — POST /api/customer (create or update).
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
		httputil.WriteError(w, http.StatusBadRequest, "Missing customer title")
		return
	}

	email, _ := body["email"].(string)
	phone, _ := body["phone"].(string)
	country, _ := body["country"].(string)
	state, _ := body["state"].(string)
	city, _ := body["city"].(string)
	address, _ := body["address"].(string)
	address2, _ := body["address2"].(string)
	zip, _ := body["zip"].(string)
	additionalInfoJSON := dbutil.JSONOrNil(body["additionalInfo"])

	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		var existingTenant string
		if err := dbpkg.Pool.QueryRow("SELECT tenant_id FROM customer WHERE id = $1", id).Scan(&existingTenant); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "Customer not found")
			return
		}
		if existingTenant != tenantId {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
			return
		}
		_, err := dbpkg.Pool.Exec(`
			UPDATE customer SET title = $1, email = $2, phone = $3,
			    country = $4, state = $5, city = $6, address = $7, address2 = $8, zip = $9,
			    additional_info = $10, version = COALESCE(version, 1) + 1 WHERE id = $11`,
			title, dbutil.NullStr(email), dbutil.NullStr(phone), dbutil.NullStr(country), dbutil.NullStr(state),
			dbutil.NullStr(city), dbutil.NullStr(address), dbutil.NullStr(address2), dbutil.NullStr(zip),
			additionalInfoJSON, id)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update customer")
			return
		}
		audit.EntityChange(claims, "CUSTOMER", id, title, "UPDATED")
		w.WriteHeader(http.StatusOK)
		return
	}
	if !quotas.Enforce(w, tenantId, "customer") {
		return
	}
	id = uuid.New().String()
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO customer (id, created_time, title, email, phone, country, state, city,
		                    address, address2, zip, additional_info, tenant_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 1)`,
		id, now, title, dbutil.NullStr(email), dbutil.NullStr(phone), dbutil.NullStr(country), dbutil.NullStr(state),
		dbutil.NullStr(city), dbutil.NullStr(address), dbutil.NullStr(address2), dbutil.NullStr(zip),
		additionalInfoJSON, tenantId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create customer")
		return
	}
	audit.EntityChange(claims, "CUSTOMER", id, title, "ADDED")
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "CUSTOMER", "id": id},
		"createdTime": now,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"title":       title,
	})
}

// Delete — DELETE /api/customer/{id}.
func Delete(w http.ResponseWriter, r *http.Request, customerId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var existingTenant, existingTitle string
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, title FROM customer WHERE id = $1", customerId).Scan(&existingTenant, &existingTitle); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Customer not found")
		return
	}
	if existingTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant delete denied")
		return
	}
	if _, err := dbpkg.Pool.Exec("DELETE FROM customer WHERE id = $1", customerId); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete customer")
		return
	}
	audit.EntityChange(claims, "CUSTOMER", customerId, existingTitle, "DELETED")
	w.WriteHeader(http.StatusOK)
}
