// Package entityview implements /api/entityView CRUD.
package entityview

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
)

// Save — POST /api/entityView (create or update).
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
		httputil.WriteError(w, http.StatusBadRequest, "Missing entity view name")
		return
	}
	viewType, _ := body["type"].(string)
	if viewType == "" {
		viewType = "default"
	}
	// entity_id is a uuid column, and ExtractEntityID returns "" when the field
	// is absent or malformed. Passing that empty string straight to the driver
	// made a request without an entityId fail with `invalid input syntax for
	// type uuid` and surface as a 500 -- a malformed body answered as a server
	// fault. NullUUID stores NULL instead, matching how customerId below has
	// always been handled.
	entityId := httputil.ExtractEntityID(body, "entityId")
	entityType := ""
	if v, ok := body["entityId"].(map[string]interface{}); ok {
		entityType, _ = v["entityType"].(string)
	}
	keys := dbutil.JSONOrNil(body["keys"])
	customerId := httputil.ExtractEntityID(body, "customerId")

	var startTs, endTs int64
	if v, ok := body["startTs"].(float64); ok {
		startTs = int64(v)
	}
	if v, ok := body["endTs"].(float64); ok {
		endTs = int64(v)
	}

	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		var existingTenant string
		if err := dbpkg.Pool.QueryRow("SELECT tenant_id FROM entity_view WHERE id = $1", id).Scan(&existingTenant); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "Entity view not found")
			return
		}
		if existingTenant != tenantId {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
			return
		}
		_, err := dbpkg.Pool.Exec(`
			UPDATE entity_view SET name = $1, type = $2, entity_id = $3, entity_type = $4,
			    keys = $5, customer_id = $6, start_ts = $7, end_ts = $8,
			    version = COALESCE(version, 1) + 1 WHERE id = $9`,
			name, viewType, dbutil.NullUUID(entityId), entityType, keys, dbutil.NullUUID(customerId), startTs, endTs, id)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update entity view")
			return
		}
		audit.EntityChange(claims, "ENTITY_VIEW", id, name, "UPDATED")
		w.WriteHeader(http.StatusOK)
		return
	}
	id = uuid.New().String()
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO entity_view (id, created_time, name, type, entity_id, entity_type, tenant_id,
		                       customer_id, keys, start_ts, end_ts, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1)`,
		id, now, name, viewType, dbutil.NullUUID(entityId), entityType, tenantId,
		dbutil.NullUUID(customerId), keys, startTs, endTs)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create entity view")
		return
	}
	audit.EntityChange(claims, "ENTITY_VIEW", id, name, "ADDED")
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "ENTITY_VIEW", "id": id},
		"createdTime": now,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"name":        name,
		"type":        viewType,
	})
}

// Delete — DELETE /api/entityView/{id}.
func Delete(w http.ResponseWriter, r *http.Request, viewId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var existingTenant, existingName string
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, name FROM entity_view WHERE id = $1", viewId).Scan(&existingTenant, &existingName); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Entity view not found")
		return
	}
	if existingTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant delete denied")
		return
	}
	if _, err := dbpkg.Pool.Exec("DELETE FROM entity_view WHERE id = $1", viewId); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete entity view")
		return
	}
	audit.EntityChange(claims, "ENTITY_VIEW", viewId, existingName, "DELETED")
	w.WriteHeader(http.StatusOK)
}
