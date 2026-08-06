// Package relations implements /api/relation* endpoints (graph edges
// between entities). Mirrors TB classic's RelationController surface.
package relations

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/topology"
)

// Handle multiplexes /api/relation* — GET filters, POST upserts, DELETE removes.
func Handle(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	switch r.Method {
	case "GET":
		list(w, r, tenantId)
	case "POST":
		save(w, r, tenantId)
	case "DELETE":
		del(w, r, tenantId)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func list(w http.ResponseWriter, r *http.Request, tenantId string) {
	q := r.URL.Query()
	edges, err := topology.ListEdges(dbpkg.Pool, topology.EdgeFilter{
		TenantID:          tenantId,
		From:              topology.EntityRef{Type: q.Get("fromType"), ID: q.Get("fromId")},
		To:                topology.EntityRef{Type: q.Get("toType"), ID: q.Get("toId")},
		RelationType:      q.Get("relationType"),
		RelationTypeGroup: q.Get("relationTypeGroup"),
	})
	if err != nil {
		log.Printf("ERROR querying relations: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}

	out := []map[string]interface{}{}
	for _, edge := range edges {
		rel := map[string]interface{}{
			"from":      map[string]interface{}{"entityType": edge.From.Type, "id": edge.From.ID},
			"to":        map[string]interface{}{"entityType": edge.To.Type, "id": edge.To.ID},
			"type":      edge.RelationType,
			"typeGroup": edge.RelationTypeGroup,
		}
		if len(edge.Metadata) > 0 && string(edge.Metadata) != "{}" {
			var ai interface{}
			_ = json.Unmarshal(edge.Metadata, &ai)
			rel["additionalInfo"] = ai
		}
		out = append(out, rel)
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

func save(w http.ResponseWriter, r *http.Request, tenantId string) {
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	from, _ := body["from"].(map[string]interface{})
	to, _ := body["to"].(map[string]interface{})
	if from == nil || to == nil {
		httputil.WriteError(w, http.StatusBadRequest, "Missing from/to")
		return
	}
	fromId, _ := from["id"].(string)
	fromType, _ := from["entityType"].(string)
	toId, _ := to["id"].(string)
	toType, _ := to["entityType"].(string)
	rtype, _ := body["type"].(string)
	group, _ := body["typeGroup"].(string)
	direction, _ := body["direction"].(string)
	if group == "" {
		group = "COMMON"
	}
	additionalInfoJSON := dbutil.JSONOrNil(body["additionalInfo"])
	if additionalInfoJSON == "" {
		additionalInfoJSON = "{}"
	}
	metadata, _ := additionalInfoJSON.(string)
	if metadata == "" {
		metadata = "{}"
	}

	err := topology.SaveEdge(dbpkg.Pool, topology.Edge{
		TenantID:          tenantId,
		From:              topology.EntityRef{Type: fromType, ID: fromId},
		To:                topology.EntityRef{Type: toType, ID: toId},
		RelationType:      rtype,
		RelationTypeGroup: group,
		Direction:         direction,
		Metadata:          json.RawMessage(metadata),
	})
	if err != nil {
		log.Printf("ERROR saving relation: %v", err)
		if errors.Is(err, topology.ErrCrossTenant) {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant relation denied")
			return
		}
		if errors.Is(err, topology.ErrInvalidRelationType) || errors.Is(err, topology.ErrUnknownEntity) || errors.Is(err, topology.ErrRelationNotAllowedByModel) {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save relation")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func del(w http.ResponseWriter, r *http.Request, tenantId string) {
	q := r.URL.Query()
	fromId := q.Get("fromId")
	fromType := q.Get("fromType")
	toId := q.Get("toId")
	toType := q.Get("toType")
	relationType := q.Get("relationType")
	group := q.Get("relationTypeGroup")
	if group == "" {
		group = "COMMON"
	}
	if fromId == "" || toId == "" || relationType == "" {
		httputil.WriteError(w, http.StatusBadRequest, "fromId, toId, relationType are required")
		return
	}
	if err := topology.DeleteEdge(dbpkg.Pool, topology.EdgeFilter{
		TenantID:          tenantId,
		From:              topology.EntityRef{Type: fromType, ID: fromId},
		To:                topology.EntityRef{Type: toType, ID: toId},
		RelationType:      relationType,
		RelationTypeGroup: group,
	}); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete relation")
		return
	}
	w.WriteHeader(http.StatusOK)
}
