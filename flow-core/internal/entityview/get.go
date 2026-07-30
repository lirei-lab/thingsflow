package entityview

import (
	"database/sql"
	"encoding/json"
	"net/http"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// GetByID — GET /api/entityView/{id} and GET /api/entityView/info/{id}.
//
// The subtree router used to handle only DELETE here, so opening an entity view
// in the UI fetched nothing and rendered an empty page. Nothing reported an
// error: an unimplemented GET returns an empty payload with HTTP 200, so the UI
// had no way to tell "not implemented" from "no data".
//
// withInfo adds the display fields the *Info* variant carries. TB models it as a
// separate DTO; here it is the same row plus the customer's title, because the
// UI's list and detail views read the same object.
func GetByID(w http.ResponseWriter, r *http.Request, id string, withInfo bool) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	var (
		name, entityType string
		createdTime      int64
		entityID         string
		customerID       sql.NullString
		viewType         sql.NullString
		keys, additional sql.NullString
		startTS, endTS   sql.NullInt64
		version          sql.NullInt64
		customerTitle    sql.NullString
		customerIsPublic sql.NullBool
	)
	err := dbpkg.Pool.QueryRow(`
		SELECT ev.name, ev.entity_type, ev.created_time, ev.entity_id::text,
		       ev.customer_id::text, ev.type, ev.keys, ev.additional_info,
		       ev.start_ts, ev.end_ts, ev.version,
		       c.title, COALESCE(c.is_public,false)
		  FROM entity_view ev
		  LEFT JOIN customer c ON c.id = ev.customer_id
		 WHERE ev.id = $1 AND ev.tenant_id = $2`, id, tenantID).
		Scan(&name, &entityType, &createdTime, &entityID, &customerID, &viewType,
			&keys, &additional, &startTS, &endTS, &version, &customerTitle, &customerIsPublic)
	if err != nil {
		// 404 rather than 403 on a cross-tenant id, matching the rest of the API:
		// a different status would confirm the object exists in another tenant.
		httputil.WriteError(w, http.StatusNotFound, "Entity view not found")
		return
	}

	out := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "ENTITY_VIEW", "id": id},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantID},
		"name":        name,
		"type":        viewType.String,
		"entityId":    map[string]interface{}{"entityType": entityType, "id": entityID},
		"startTimeMs": startTS.Int64,
		"endTimeMs":   endTS.Int64,
	}
	if customerID.Valid && customerID.String != "" {
		out["customerId"] = map[string]interface{}{"entityType": "CUSTOMER", "id": customerID.String}
	}
	if version.Valid {
		out["version"] = version.Int64
	}
	// keys and additionalInfo are stored as JSON text; pass them through as
	// objects so the UI's forms bind to them instead of to a string.
	out["keys"] = rawJSONOrNil(keys)
	out["additionalInfo"] = rawJSONOrNil(additional)

	if withInfo {
		out["customerTitle"] = customerTitle.String
		out["customerIsPublic"] = customerIsPublic.Bool
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

func rawJSONOrNil(s sql.NullString) interface{} {
	if !s.Valid || s.String == "" {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal([]byte(s.String), &v); err != nil {
		return nil
	}
	return v
}
