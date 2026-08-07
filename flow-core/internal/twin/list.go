package twin

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// defaultListPageSize is the page size used when ?pageSize= is absent. The
// OpenAPI contract fixes the range at [1, 1000]; httputil.PageSize clamps to
// that ceiling so a listing request can never make Postgres stream an entire
// tenant table (mirrors the bound the WS plane applies to entity-data pages).
const defaultListPageSize = 100

// listCursor is the opaque, stateless, tenant-bound keyset cursor used by
// GET /api/twins. It encodes the last row's (tenant_id, entity_type,
// entity_id) so a page resumes deterministically on the same ordering. The
// tenant binding is what makes a cursor useless outside the tenant that
// issued it: decoding a cursor whose tenant differs from the caller's is a
// 400, never a silent re-scope. No server-side store is involved — the
// cursor is fully self-contained.
type listCursor struct {
	TenantID   string `json:"t"`
	EntityType string `json:"et"`
	EntityID   string `json:"id"`
}

func (c listCursor) encode() string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeListCursor(raw string) (listCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return listCursor{}, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	var c listCursor
	if err := json.Unmarshal(decoded, &c); err != nil {
		return listCursor{}, fmt.Errorf("invalid cursor payload: %w", err)
	}
	if c.TenantID == "" || c.EntityType == "" || c.EntityID == "" {
		return listCursor{}, fmt.Errorf("incomplete cursor")
	}
	return c, nil
}

// listRow is one twin_registry row projected for the listing response. The
// registry is the single source of governed twin identity, so the listing
// reads it directly (never the device/asset tables) and the tenant predicate
// is applied at SQL level.
type listRow struct {
	TenantID   string
	ThingID    string
	EntityType string
	EntityID   string
	PolicyID   string
	Definition string
	Attributes []byte
}

// HandleList serves GET /api/twins — tenant-scoped, cursor-paginated twin
// listing with kind/definition/text/relation filters (R3 success criterion
// 1). Every predicate is carried in the SQL, so a cross-tenant row can never
// appear in a response; there is no post-hoc Go filtering to forget.
func HandleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		return
	}
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "Database not ready")
		return
	}

	callerTenant, _ := claims["tenantId"].(string)
	sysAdmin := callerIsSysAdmin(claims)
	// Fail closed on an empty tenant claim (401) exactly like GetByEntity; a
	// SYS_ADMIN carries no tenant binding and may list across tenants.
	if !sysAdmin && callerTenant == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	pageSize := httputil.PageSize(r, defaultListPageSize)

	kind := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("kind")))
	if kind != "" && kind != "DEVICE" && kind != "ASSET" {
		httputil.WriteError(w, http.StatusBadRequest, "Unsupported twin kind")
		return
	}
	definition := strings.TrimSpace(r.URL.Query().Get("definition"))
	text := strings.TrimSpace(r.URL.Query().Get("text"))
	relationType := strings.TrimSpace(r.URL.Query().Get("relationType"))
	relationDirection := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("relationDirection")))
	if relationDirection == "" {
		relationDirection = "FROM"
	}
	if relationDirection != "FROM" && relationDirection != "TO" {
		httputil.WriteError(w, http.StatusBadRequest, "Unsupported relation direction")
		return
	}

	// Cursor: the last row of the previous page. Its tenant binding must equal
	// the caller's tenant (or, for a SYS_ADMIN, any tenant scope) or the
	// cursor is rejected — a cursor can never silently re-scope a request.
	var after *listCursor
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		c, err := decodeListCursor(raw)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "Invalid cursor")
			return
		}
		if !sysAdmin && c.TenantID != callerTenant {
			httputil.WriteError(w, http.StatusBadRequest, "Cursor is bound to another tenant")
			return
		}
		after = &c
	}

	filters, filterArgs := listFilters(callerTenant, sysAdmin, kind, definition, text, relationType, relationDirection)

	total, err := countListRows(filters, filterArgs...)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin count query failed")
		return
	}

	rows, err := queryListRows(filters, filterArgs, after, pageSize+1)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin list query failed")
		return
	}

	hasNext := len(rows) > pageSize
	if hasNext {
		rows = rows[:pageSize]
	}
	data := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		item, err := buildListItem(row)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "Twin projection failed")
			return
		}
		data = append(data, item)
	}

	var nextPageLink interface{}
	if hasNext && len(rows) > 0 {
		last := rows[len(rows)-1]
		nextPageLink = (listCursor{TenantID: last.TenantID, EntityType: last.EntityType, EntityID: last.EntityID}).encode()
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          data,
		"nextPageLink":  nextPageLink,
		"hasNext":       hasNext,
		"totalElements": total,
	})
}

// listFilters builds the SQL filter predicate over twin_registry (alias tr)
// plus its bound arguments. callerTenant is "" for a SYS_ADMIN, in which case
// no tenant predicate is emitted (all tenants) — every other caller is pinned
// to their tenant at SQL level.
func listFilters(callerTenant string, sysAdmin bool, kind, definition, text, relationType, relationDirection string) (string, []interface{}) {
	conds := []string{}
	args := []interface{}{}
	arg := func() int {
		args = append(args, nil) // placeholder; caller overwrites
		return len(args)
	}

	if !sysAdmin || callerTenant != "" {
		idx := arg()
		args[idx-1] = callerTenant
		conds = append(conds, fmt.Sprintf("tr.tenant_id = $%d", idx))
	}
	if kind != "" {
		idx := arg()
		args[idx-1] = kind
		conds = append(conds, fmt.Sprintf("tr.entity_type = $%d", idx))
	}
	if definition != "" {
		idx := arg()
		args[idx-1] = "%" + definition + "%"
		conds = append(conds, fmt.Sprintf("tr.definition ILIKE $%d", idx))
	}
	if text != "" {
		idx := arg()
		args[idx-1] = "%" + text + "%"
		conds = append(conds, fmt.Sprintf(
			"(tr.attributes->>'name' ILIKE $%d OR tr.attributes->>'type' ILIKE $%d OR tr.attributes->>'label' ILIKE $%d)",
			idx, idx, idx))
	}
	if relationType != "" {
		idx := arg()
		args[idx-1] = relationType
		topo, legacy := relationFilterSQL(relationDirection, idx)
		conds = append(conds, "(EXISTS ("+topo+") OR EXISTS ("+legacy+"))")
	}

	if len(conds) == 0 {
		return "", args
	}
	return strings.Join(conds, " AND "), args
}

// relationFilterSQL builds the two correlated EXISTS bodies behind the
// listing's relationType existence filter. Both are correlated to the outer
// twin_registry alias tr; relIdx is the bound parameter holding the relation
// type. The topology_edge branch is tenant-predicated on its own tenant_id
// column. The legacy relation branch (no tenant column) resolves the far
// endpoint's tenant through the entity tables and requires it to equal
// tr.tenant_id — the same SQL-level discipline as topology.neighborUnionSQL,
// so a cross-tenant legacy relation can never match a listing row.
func relationFilterSQL(direction string, relIdx int) (topology, legacy string) {
	if direction == "TO" {
		return fmt.Sprintf(`
			SELECT 1 FROM topology_edge te
			 WHERE te.tenant_id = tr.tenant_id
			   AND te.relation_type_group = 'COMMON'
			   AND te.relation_type = $%d
			   AND ((te.direction = 'DIRECTED' AND te.to_type = tr.entity_type AND te.to_id = tr.entity_id)
			     OR (te.direction = 'BIDIRECTIONAL' AND ((te.from_type = tr.entity_type AND te.from_id = tr.entity_id)
			                                           OR (te.to_type = tr.entity_type AND te.to_id = tr.entity_id))))`, relIdx),
			fmt.Sprintf(`
			SELECT 1 FROM relation r
			 LEFT JOIN asset fa ON r.from_type = 'ASSET' AND fa.id = r.from_id
			 LEFT JOIN device fd ON r.from_type = 'DEVICE' AND fd.id = r.from_id
			 LEFT JOIN customer fc ON r.from_type = 'CUSTOMER' AND fc.id = r.from_id
			 LEFT JOIN entity_view fev ON r.from_type = 'ENTITY_VIEW' AND fev.id = r.from_id
			 LEFT JOIN dashboard fda ON r.from_type = 'DASHBOARD' AND fda.id = r.from_id
			 LEFT JOIN device_profile fdp ON r.from_type = 'DEVICE_PROFILE' AND fdp.id = r.from_id
			 LEFT JOIN asset_profile fap ON r.from_type = 'ASSET_PROFILE' AND fap.id = r.from_id
			 WHERE r.relation_type = $%d
			   AND r.to_type = tr.entity_type AND r.to_id = tr.entity_id
			   AND COALESCE(CASE WHEN r.from_type = 'TENANT' THEN r.from_id END,
			                fa.tenant_id, fd.tenant_id, fc.tenant_id, fev.tenant_id,
			                fda.tenant_id, fdp.tenant_id, fap.tenant_id) = tr.tenant_id`, relIdx)
	}
	return fmt.Sprintf(`
		SELECT 1 FROM topology_edge te
		 WHERE te.tenant_id = tr.tenant_id
		   AND te.relation_type_group = 'COMMON'
		   AND te.relation_type = $%d
		   AND ((te.direction = 'DIRECTED' AND te.from_type = tr.entity_type AND te.from_id = tr.entity_id)
		     OR (te.direction = 'BIDIRECTIONAL' AND ((te.from_type = tr.entity_type AND te.from_id = tr.entity_id)
		                                           OR (te.to_type = tr.entity_type AND te.to_id = tr.entity_id))))`, relIdx),
		fmt.Sprintf(`
		SELECT 1 FROM relation r
		 LEFT JOIN asset ta ON r.to_type = 'ASSET' AND ta.id = r.to_id
		 LEFT JOIN device td ON r.to_type = 'DEVICE' AND td.id = r.to_id
		 LEFT JOIN customer tc ON r.to_type = 'CUSTOMER' AND tc.id = r.to_id
		 LEFT JOIN entity_view tev ON r.to_type = 'ENTITY_VIEW' AND tev.id = r.to_id
		 LEFT JOIN dashboard tda ON r.to_type = 'DASHBOARD' AND tda.id = r.to_id
		 LEFT JOIN device_profile tdp ON r.to_type = 'DEVICE_PROFILE' AND tdp.id = r.to_id
		 LEFT JOIN asset_profile tap ON r.to_type = 'ASSET_PROFILE' AND tap.id = r.to_id
		 WHERE r.relation_type = $%d
		   AND r.from_type = tr.entity_type AND r.from_id = tr.entity_id
		   AND COALESCE(CASE WHEN r.to_type = 'TENANT' THEN r.to_id END,
		                ta.tenant_id, td.tenant_id, tc.tenant_id, tev.tenant_id,
		                tda.tenant_id, tdp.tenant_id, tap.tenant_id) = tr.tenant_id`, relIdx)
}

// countListRows returns the total rows matching the filter predicate (without
// the keyset) so totalElements reflects the full filtered set, not the page.
func countListRows(filters string, args ...interface{}) (int, error) {
	query := `SELECT count(*) FROM twin_registry tr`
	if filters != "" {
		query += " WHERE " + filters
	}
	var total int
	if err := dbpkg.Pool.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func queryListRows(filters string, filterArgs []interface{}, after *listCursor, limit int) ([]listRow, error) {
	where := filters
	args := append([]interface{}{}, filterArgs...)
	if after != nil {
		base := len(args)
		args = append(args, after.TenantID, after.EntityType, after.EntityID)
		// Keyset: strictly after the previous page's last row on the same
		// ordering. Within a single tenant this reduces to the documented
		// (entity_type, entity_id) order; the leading tenant_id term keeps a
		// SYS_ADMIN all-tenant listing deterministic too.
		keyset := fmt.Sprintf(
			"(tr.tenant_id, tr.entity_type, tr.entity_id) > ($%d::uuid, $%d, $%d::uuid)",
			base+1, base+2, base+3)
		if where == "" {
			where = keyset
		} else {
			where = where + " AND " + keyset
		}
	}
	query := `SELECT tr.tenant_id::text, tr.thing_id, tr.entity_type, tr.entity_id::text,
	                 tr.policy_id, tr.definition, tr.attributes
	          FROM twin_registry tr`
	if where != "" {
		query += " WHERE " + where
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY tr.tenant_id, tr.entity_type, tr.entity_id LIMIT $%d", len(args))

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []listRow{}
	for rows.Next() {
		var row listRow
		if err := rows.Scan(&row.TenantID, &row.ThingID, &row.EntityType, &row.EntityID,
			&row.PolicyID, &row.Definition, &row.Attributes); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// buildListItem projects one registry row into the Twin schema documented in
// OpenAPI (thingId/policyId/definition/entity/attributes/features/relations).
// Attributes come straight from the registry jsonb; features and relations
// reuse the same tenant-scoped readers as the single-twin read so the listing
// and the detail view can never disagree on a twin's shape.
func buildListItem(row listRow) (map[string]interface{}, error) {
	attrs := map[string]interface{}{}
	if len(row.Attributes) > 0 {
		if err := json.Unmarshal(row.Attributes, &attrs); err != nil {
			return nil, fmt.Errorf("decode twin registry attributes: %w", err)
		}
	}
	features, err := loadFeatures(row.TenantID, row.EntityType, row.EntityID)
	if err != nil {
		return nil, err
	}
	relations, err := loadRelations(row.TenantID, row.EntityType, row.EntityID)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"thingId":    row.ThingID,
		"policyId":   row.PolicyID,
		"definition": row.Definition,
		"entity": map[string]interface{}{
			"entityType": row.EntityType,
			"id":         row.EntityID,
		},
		"attributes": attrs,
		"features":   features,
		"relations":  relations,
	}, nil
}
