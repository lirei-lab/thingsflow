package widget

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	"flow-core/internal/bootstrap"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
)

// Type processes GET /api/widgetType with query params:
// - ?fqn=<fully_qualified_name>
// - ?isSystem=true&bundleAlias=<alias>&alias=<alias>
// - ?pageSize=N&page=N (for paginated listing)
func Type(w http.ResponseWriter, r *http.Request) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	fqn := r.URL.Query().Get("fqn")
	bundleAlias := r.URL.Query().Get("bundleAlias")
	alias := r.URL.Query().Get("alias")

	if fqn != "" {
		// Lookup by fully qualified name (e.g., "charts.basic_timeseries")
		handleWidgetTypeByFqn(w, fqn)
		return
	}

	if bundleAlias != "" && alias != "" {
		// Lookup by bundle alias + widget alias
		handleWidgetTypeByAlias(w, bundleAlias, alias)
		return
	}

	// Return error matching TB format
	httputil.WriteError(w, http.StatusBadRequest,
		`Parameter conditions "fqn" OR "isSystem, bundleAlias, alias" not met for actual request parameters`)
}

// Types processes GET /api/widgetTypes. As with /api/widgetsBundles
// the response shape switches on whether the caller asked for a page:
//   - no page params  → flat JSON array of widget types
//   - page or pageSize → paginated PageData<WidgetType> wrapper
//
// TB UI v4.3+ calls /api/widgetTypes?widgetsBundleId={id} to populate
// the widget gallery for the selected bundle and runs a client-side
// .find/.map on the result, so the flat-array shape is the default.
//
// Filters: widgetsBundleId (joins widgets_bundle_widget),
//
//	fullSearch (case-insensitive in name + description).
func Types(w http.ResponseWriter, r *http.Request) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	q := r.URL.Query()
	wantPaginated := q.Has("page") || q.Has("pageSize")
	bundleID := q.Get("widgetsBundleId")
	// TB v4.3 sends fullSearch=true|false as a BOOLEAN toggle for
	// case-insensitive search behaviour, NOT a search term. The actual
	// query string is in textSearch. Treating fullSearch as the term
	// produces WHERE name LIKE '%false%' and a 0-row response that
	// renders the widget library page empty even though 522 widgets
	// are seeded. Read textSearch (preferred) and fall back to
	// fullSearch only when it isn't the true/false sentinel.
	search := strings.TrimSpace(q.Get("textSearch"))
	if search == "" {
		fs := strings.TrimSpace(q.Get("fullSearch"))
		if fs != "" && fs != "true" && fs != "false" {
			search = fs
		}
	}

	pageSize := httputil.PageSize(r, 100)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize

	// Build the query dynamically. The bundles_json subquery stays so
	// each row carries its full bundle membership (needed by the UI's
	// breadcrumb + filter chips).
	args := []interface{}{}
	conds := []string{}
	join := ""
	if bundleID != "" {
		join = "JOIN widgets_bundle_widget bwf ON bwf.widget_type_id = wt.id"
		args = append(args, bundleID)
		conds = append(conds, fmt.Sprintf("bwf.widgets_bundle_id = $%d", len(args)))
	}
	if search != "" {
		args = append(args, "%"+strings.ToLower(search)+"%")
		conds = append(conds, fmt.Sprintf("(LOWER(wt.name) LIKE $%d OR LOWER(wt.description) LIKE $%d)", len(args), len(args)))
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	// Count for paginated response.
	var total int
	countSQL := fmt.Sprintf("SELECT count(*) FROM widget_type wt %s %s", join, where)
	if err := dbpkg.Pool.QueryRow(countSQL, args...).Scan(&total); err != nil {
		log.Printf("WARN widgetTypes count: %v", err)
	}

	listSQL := fmt.Sprintf(`
		SELECT wt.id, wt.created_time, wt.fqn, wt.name, wt.tenant_id, wt.deprecated, wt.scada,
		       wt.description, wt.tags, wt.version, wt.image,
		       COALESCE((
		           SELECT json_agg(json_build_object(
		               'id', json_build_object('entityType', 'WIDGETS_BUNDLE', 'id', wb.id),
		               'name', wb.title))
		           FROM widgets_bundle_widget bwt
		           JOIN widgets_bundle wb ON wb.id = bwt.widgets_bundle_id
		           WHERE bwt.widget_type_id = wt.id
		       )::text, '[]') AS bundles_json,
		       wt.descriptor::text AS descriptor_json
		FROM widget_type wt %s %s
		ORDER BY wt.name`, join, where)

	var rows *sql.Rows
	if wantPaginated {
		listArgs := append([]interface{}{}, args...)
		listArgs = append(listArgs, pageSize, offset)
		listSQL += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(listArgs)-1, len(listArgs))
		rows, err = dbpkg.Pool.Query(listSQL, listArgs...)
	} else {
		rows, err = dbpkg.Pool.Query(listSQL, args...)
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		item := scanWidgetTypeWithImage(rows)
		if item != nil {
			data = append(data, item)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if !wantPaginated {
		json.NewEncoder(w).Encode(data)
		return
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page + 1) < totalPages,
	})
}

// BundleByAlias processes GET /api/widgetsBundleByAlias/{alias}
func BundleByAlias(w http.ResponseWriter, r *http.Request, bundleAlias string) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	var id, title string
	var createdTime int64
	var tenantId, alias *string
	var image, description *string
	var scada bool
	var order *int
	var version *int64

	err = dbpkg.Pool.QueryRow(`
		SELECT id, created_time, alias, tenant_id, title, image, scada, description, widgets_bundle_order, version
		FROM widgets_bundle WHERE alias = $1`, bundleAlias).Scan(
		&id, &createdTime, &alias, &tenantId, &title, &image, &scada, &description, &order, &version)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Widget bundle not found")
		return
	}

	tid := "13814000-1dd2-11b2-8080-808080808080"
	if tenantId != nil {
		tid = *tenantId
	}

	result := map[string]interface{}{
		"id": map[string]interface{}{
			"entityType": "WIDGETS_BUNDLE",
			"id":         id,
		},
		"createdTime": createdTime,
		"tenantId": map[string]interface{}{
			"entityType": "TENANT",
			"id":         tid,
		},
		"title": title,
		"scada": scada,
	}
	if alias != nil {
		result["alias"] = *alias
	}
	if version != nil {
		result["version"] = *version
	} else {
		result["version"] = 1
	}
	if image != nil {
		result["image"] = *image
	} else {
		result["image"] = nil
	}
	if description != nil {
		result["description"] = *description
	}
	if order != nil {
		result["widgetsBundleOrder"] = *order
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// BundlesFlat serves GET /api/widgetsBundles/all — the TB UI v4.x
// widget gallery calls this expecting a flat JSON array (no pagination
// wrapper). It then runs .sort() on the result client-side, so we MUST
// emit `[...]` not `{data: [...], totalPages: 1, ...}` — the gallery
// throws "this.allWidgetsBundles.sort is not a function" otherwise and
// the dropdown stays empty even though Bundles() returns 32 rows.
func BundlesFlat(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	rows, err := dbpkg.Pool.Query(`
		SELECT id, created_time, alias, tenant_id, title, image, scada, description, widgets_bundle_order, version, external_id
		FROM widgets_bundle ORDER BY title`)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, title string
		var createdTime int64
		var tenantId, aliasStr *string
		var imageStr, desc, externalId *string
		var scada bool
		var order *int
		var version *int64

		if err := rows.Scan(&id, &createdTime, &aliasStr, &tenantId, &title, &imageStr, &scada, &desc, &order, &version, &externalId); err != nil {
			continue
		}

		tid := "13814000-1dd2-11b2-8080-808080808080"
		if tenantId != nil {
			tid = *tenantId
		}

		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "WIDGETS_BUNDLE", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"title":       title,
			"name":        title,
			"scada":       scada,
		}
		if aliasStr != nil {
			item["alias"] = *aliasStr
		}
		if version != nil {
			item["version"] = *version
		} else {
			item["version"] = 1
		}
		if imageStr != nil {
			item["image"] = *imageStr
		} else {
			item["image"] = nil
		}
		if desc != nil {
			item["description"] = *desc
		} else {
			item["description"] = nil
		}
		if order != nil {
			item["order"] = *order
		} else {
			item["order"] = 0
		}
		if externalId != nil && *externalId != "" {
			item["externalId"] = map[string]interface{}{"entityType": "WIDGETS_BUNDLE", "id": *externalId}
		} else {
			item["externalId"] = nil
		}
		data = append(data, item)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// Bundles processes GET /api/widgetsBundles. The response shape depends
// on whether the caller passed page/pageSize:
//   - no page params  → flat JSON array (TB UI v4.3+ getAllWidgetsBundles)
//   - page or pageSize → paginated PageData<WidgetsBundle> wrapper
//
// TB UI v4.3.1 unconditionally calls `/api/widgetsBundles` and runs
// .sort() on the response, so the flat-array shape is the default. The
// paginated form stays available for any tooling that explicitly asks
// for a page (and for the legacy upstream UI contract).
func Bundles(w http.ResponseWriter, r *http.Request) {
	_, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	q := r.URL.Query()
	wantPaginated := q.Has("page") || q.Has("pageSize")

	pageSize := httputil.PageSize(r, 100)
	page := httputil.IntParam(r, "page", 0)
	offset := page * pageSize

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM widgets_bundle").Scan(&total)

	queryStr := `SELECT id, created_time, alias, tenant_id, title, image, scada, description, widgets_bundle_order, version, external_id
		FROM widgets_bundle ORDER BY title`
	var rows *sql.Rows
	if wantPaginated {
		rows, err = dbpkg.Pool.Query(queryStr+" LIMIT $1 OFFSET $2", pageSize, offset)
	} else {
		rows, err = dbpkg.Pool.Query(queryStr)
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, title string
		var createdTime int64
		var tenantId, aliasStr *string
		var imageStr, desc, externalId *string
		var scada bool
		var order *int
		var version *int64

		if err := rows.Scan(&id, &createdTime, &aliasStr, &tenantId, &title, &imageStr, &scada, &desc, &order, &version, &externalId); err != nil {
			continue
		}

		tid := "13814000-1dd2-11b2-8080-808080808080"
		if tenantId != nil {
			tid = *tenantId
		}

		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "WIDGETS_BUNDLE", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"title":       title,
			"name":        title,
			"scada":       scada,
		}
		if aliasStr != nil {
			item["alias"] = *aliasStr
		}
		if version != nil {
			item["version"] = *version
		} else {
			item["version"] = 1
		}
		if imageStr != nil {
			item["image"] = *imageStr
		} else {
			item["image"] = nil
		}
		if desc != nil {
			item["description"] = *desc
		} else {
			item["description"] = nil
		}
		if order != nil {
			item["order"] = *order
		} else {
			item["order"] = 0
		}
		if externalId != nil && *externalId != "" {
			item["externalId"] = map[string]interface{}{"entityType": "WIDGETS_BUNDLE", "id": *externalId}
		} else {
			item["externalId"] = nil
		}
		data = append(data, item)
	}

	w.Header().Set("Content-Type", "application/json")
	if !wantPaginated {
		// Flat array for TB UI v4.3+ getAllWidgetsBundles().
		json.NewEncoder(w).Encode(data)
		return
	}
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page + 1) < totalPages,
	})
}

// ─── Internal helpers ───────────────────────────────────────────────────────

func handleWidgetTypeByFqn(w http.ResponseWriter, fqn string) {
	// TB v4 UI stores typeFullFqn with a "system." prefix for system widgets,
	// but the DB stores them without it. Strip the prefix before querying.
	dbFqn := strings.TrimPrefix(fqn, "system.")

	// Also handle the case where it has multiple dots (e.g., "charts.basic_timeseries")
	// but was passed as-is after stripping
	row := dbpkg.Pool.QueryRow(`
		SELECT id, created_time, fqn, name, tenant_id, deprecated, scada, description, tags, version, descriptor
		FROM widget_type WHERE fqn = $1`, dbFqn)

	item := scanWidgetTypeWithDescriptor(row)
	if item == nil {
		httputil.WriteError(w, http.StatusNotFound, "Widget type not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(item)
}

func handleWidgetTypeByAlias(w http.ResponseWriter, bundleAlias, alias string) {
	fqn := bundleAlias + "." + alias

	row := dbpkg.Pool.QueryRow(`
		SELECT id, created_time, fqn, name, tenant_id, deprecated, scada, description, tags, version, descriptor
		FROM widget_type WHERE fqn = $1`, fqn)

	item := scanWidgetTypeWithDescriptor(row)
	if item == nil {
		httputil.WriteError(w, http.StatusNotFound, "Widget type not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(item)
}

// TypeByID serves GET /api/widgetType/{id}. The UI hits this
// path when the user opens a widget from the library for editing or
// preview — without it the click triggers the "404 OK" toast.
func TypeByID(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	if !httputil.LooksLikeUUID(id) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid widget type id")
		return
	}
	row := dbpkg.Pool.QueryRow(`
		SELECT id, created_time, fqn, name, tenant_id, deprecated, scada, description, tags, version, descriptor
		  FROM widget_type WHERE id = $1`, id)
	item := scanWidgetTypeWithDescriptor(row)
	if item == nil {
		httputil.WriteError(w, http.StatusNotFound, "Widget type not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(item)
}

type widgetScanner interface {
	Scan(dest ...interface{}) error
}

func scanWidgetType(rows widgetScanner) map[string]interface{} {
	var id, name string
	var createdTime int64
	var fqn, tenantId, description *string
	var deprecated, scada bool
	var tagsRaw []byte
	var version *int64

	if err := rows.Scan(&id, &createdTime, &fqn, &name, &tenantId, &deprecated, &scada, &description, &tagsRaw, &version); err != nil {
		log.Printf("ERROR scanning widget_type: %v", err)
		return nil
	}

	tid := "13814000-1dd2-11b2-8080-808080808080"
	if tenantId != nil {
		tid = *tenantId
	}

	item := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "WIDGET_TYPE", "id": id},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
		"name":        name,
		"deprecated":  deprecated,
		"scada":       scada,
		"widgetType":  inferWidgetTypeFromFqn(fqn),
		"tags":        dbutil.ParsePgTextArray(tagsRaw),
	}
	if fqn != nil {
		item["fqn"] = *fqn
	}
	if version != nil {
		item["version"] = *version
	} else {
		item["version"] = 1
	}
	if description != nil {
		item["description"] = *description
	}

	return item
}

// scanWidgetTypeWithImage extends scanWidgetType with the image column,
// the bundles aggregate, and the descriptor JSON. The TB UI v4.3
// gallery sort comparator reads `.descriptor.type` to categorise each
// widget (timeseries/latest/rpc/alarm/static); without descriptor it
// crashes with "Cannot read properties of undefined (reading 'type')".
// We also surface the top-level `type` field that older widget code
// paths consult, so the UI works whether it reads `.type` directly or
// digs into the descriptor.
func scanWidgetTypeWithImage(rows widgetScanner) map[string]interface{} {
	var id, name string
	var createdTime int64
	var fqn, tenantId, description, image, bundlesJSON, descriptorJSON *string
	var deprecated, scada bool
	var tagsRaw []byte
	var version *int64

	if err := rows.Scan(&id, &createdTime, &fqn, &name, &tenantId, &deprecated, &scada,
		&description, &tagsRaw, &version, &image, &bundlesJSON, &descriptorJSON); err != nil {
		log.Printf("ERROR scanning widget_type: %v", err)
		return nil
	}

	tid := "13814000-1dd2-11b2-8080-808080808080"
	if tenantId != nil {
		tid = *tenantId
	}

	// Resolve the canonical widget kind. Prefer the descriptor's `type`
	// (the source of truth in TB classic) — fall back to fqn-based
	// inference for the few rows where descriptor JSON is malformed.
	widgetKind := inferWidgetTypeFromFqn(fqn)
	var descriptor interface{}
	if descriptorJSON != nil && *descriptorJSON != "" {
		if err := json.Unmarshal([]byte(*descriptorJSON), &descriptor); err == nil {
			if dm, ok := descriptor.(map[string]interface{}); ok {
				if t, ok := dm["type"].(string); ok && t != "" {
					widgetKind = t
				}
			}
		}
	}

	item := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "WIDGET_TYPE", "id": id},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
		"name":        name,
		"deprecated":  deprecated,
		"scada":       scada,
		"widgetType":  widgetKind,
		"type":        widgetKind, // some UI code paths read item.type directly
		"tags":        dbutil.ParsePgTextArray(tagsRaw),
	}
	if descriptor != nil {
		item["descriptor"] = descriptor
	}
	if fqn != nil {
		item["fqn"] = *fqn
	}
	if version != nil {
		item["version"] = *version
	} else {
		item["version"] = 1
	}
	if description != nil {
		item["description"] = *description
	}
	if image != nil {
		item["image"] = *image
	} else {
		item["image"] = nil
	}
	bundles := []interface{}{}
	if bundlesJSON != nil && *bundlesJSON != "" {
		_ = json.Unmarshal([]byte(*bundlesJSON), &bundles)
	}
	item["bundles"] = bundles
	return item
}

// inferWidgetTypeFromFqn maps a fully-qualified name to the kind the UI expects.
// TB stores the actual kind inside the descriptor JSON, but for listings the UI
// only needs a generic "widgetType" string. Default to "latest" which is broadly
// compatible; a dedicated descriptor parse can refine later.
func inferWidgetTypeFromFqn(fqn *string) string {
	if fqn == nil {
		return "latest"
	}
	f := *fqn
	switch {
	case strings.Contains(f, "timeseries"):
		return "timeseries"
	case strings.Contains(f, "alarm"):
		return "alarm"
	case strings.Contains(f, "static"):
		return "static"
	case strings.Contains(f, "rpc") || strings.Contains(f, "control"):
		return "rpc"
	default:
		return "latest"
	}
}

func scanWidgetTypeWithDescriptor(row widgetScanner) map[string]interface{} {
	var id, name string
	var createdTime int64
	var fqn, tenantId, description, descriptor *string
	var deprecated, scada bool
	var tags []byte
	var version *int64

	if err := row.Scan(&id, &createdTime, &fqn, &name, &tenantId, &deprecated, &scada, &description, &tags, &version, &descriptor); err != nil {
		return nil
	}

	tid := "13814000-1dd2-11b2-8080-808080808080"
	if tenantId != nil {
		tid = *tenantId
	}

	item := map[string]interface{}{
		"id": map[string]interface{}{
			"entityType": "WIDGET_TYPE",
			"id":         id,
		},
		"createdTime": createdTime,
		"tenantId": map[string]interface{}{
			"entityType": "TENANT",
			"id":         tid,
		},
		"name":       name,
		"deprecated": deprecated,
		"scada":      scada,
	}
	if fqn != nil {
		item["fqn"] = *fqn
	}
	if version != nil {
		item["version"] = *version
	} else {
		item["version"] = 1
	}
	if description != nil {
		item["description"] = *description
	}
	if descriptor != nil && *descriptor != "" {
		var desc interface{}
		if err := json.Unmarshal([]byte(*descriptor), &desc); err == nil {
			item["descriptor"] = desc
		}
	}

	return item
}

// ─── Widget gallery flow (UI dashboard editor) ──────────────────────────────
//
// When a user opens "Add widget" in the dashboard editor, the UI walks
// the gallery via these endpoints. None of them pre-existed in flow-core,
// so the picker came up empty. They all read from the same widget_type
// and widgets_bundle tables that LoadSystemBootstrap already seeded.

// TypesInfos serves GET /api/widgetTypesInfos — paginated
// list of WidgetTypeInfo (slim projection without descriptor blob).
// Filters: widgetsBundleId, fullSearch, deprecatedFilter, scadaFirst,
// search (textSearch), tenantOnly. The UI feeds these from the picker's
// search bar and "Actuel/Obsolète" tabs.
func TypesInfos(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	q := r.URL.Query()
	pageSize := httputil.PageSize(r, 100)
	page := httputil.IntParam(r, "page", 0)
	bundleId := q.Get("widgetsBundleId")
	textSearch := strings.TrimSpace(q.Get("textSearch"))
	deprecated := strings.ToUpper(q.Get("deprecatedFilter")) // ALL/ACTUAL/DEPRECATED
	if deprecated == "" {
		deprecated = "ALL"
	}
	tenantOnly, _ := strconv.ParseBool(q.Get("tenantOnly"))

	conds := []string{}
	args := []interface{}{}
	idx := 1
	if bundleId != "" {
		conds = append(conds, fmt.Sprintf("EXISTS (SELECT 1 FROM widgets_bundle_widget bwt WHERE bwt.widget_type_id = wt.id AND bwt.widgets_bundle_id = $%d)", idx))
		args = append(args, bundleId)
		idx++
	}
	if textSearch != "" {
		conds = append(conds, fmt.Sprintf("LOWER(wt.name) LIKE $%d", idx))
		args = append(args, "%"+strings.ToLower(textSearch)+"%")
		idx++
	}
	switch deprecated {
	case "ACTUAL":
		conds = append(conds, "COALESCE(wt.deprecated, false) = false")
	case "DEPRECATED":
		conds = append(conds, "COALESCE(wt.deprecated, false) = true")
	}
	if tenantOnly {
		conds = append(conds, "wt.tenant_id != '13814000-1dd2-11b2-8080-808080808080'")
	}
	whereClause := ""
	if len(conds) > 0 {
		whereClause = " WHERE " + strings.Join(conds, " AND ")
	}

	var total int
	if err := dbpkg.Pool.QueryRow("SELECT count(*) FROM widget_type wt"+whereClause, args...).Scan(&total); err != nil {
		log.Printf("ERROR widgetTypesInfos count: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}

	args = append(args, pageSize, page*pageSize)
	limitSQL := fmt.Sprintf(" ORDER BY wt.name LIMIT $%d OFFSET $%d", len(args)-1, len(args))

	rows, err := dbpkg.Pool.Query(`
		SELECT wt.id, wt.created_time, wt.fqn, wt.name, wt.tenant_id,
		       COALESCE(wt.deprecated, false), COALESCE(wt.scada, false),
		       wt.description, wt.tags, wt.image,
		       COALESCE(wt.descriptor::jsonb->>'type','') AS widget_type,
		       COALESCE((
		           SELECT json_agg(json_build_object(
		               'id', json_build_object('entityType','WIDGETS_BUNDLE','id', wb.id),
		               'name', wb.title))
		             FROM widgets_bundle_widget bwt
		             JOIN widgets_bundle wb ON wb.id = bwt.widgets_bundle_id
		            WHERE bwt.widget_type_id = wt.id
		       )::text, '[]') AS bundles
		  FROM widget_type wt`+whereClause+limitSQL, args...)
	if err != nil {
		log.Printf("ERROR widgetTypesInfos query: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, fqn, name, widgetTypeStr string
		var createdTime int64
		var tenantId, description, tagsJSON, image, bundlesJSON *string
		var deprecated, scada bool
		if err := rows.Scan(&id, &createdTime, &fqn, &name, &tenantId, &deprecated, &scada, &description, &tagsJSON, &image, &widgetTypeStr, &bundlesJSON); err != nil {
			continue
		}
		tid := bootstrap.SystemTenantID
		if tenantId != nil {
			tid = *tenantId
		}
		item := map[string]interface{}{
			"id":          map[string]interface{}{"entityType": "WIDGET_TYPE", "id": id},
			"createdTime": createdTime,
			"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
			"fqn":         fqn,
			"name":        name,
			"deprecated":  deprecated,
			"scada":       scada,
		}
		// `widgetType` (timeseries|latest|rpc|alarm|static) drives the
		// UI's i18n-key suffix `widget.${widgetType}-short`. Without it
		// the UI shows "widget.undefined-short" next to every card.
		if widgetTypeStr != "" {
			item["widgetType"] = widgetTypeStr
		}
		if description != nil {
			item["description"] = *description
		}
		if image != nil {
			item["image"] = *image
		}
		if tagsJSON != nil && *tagsJSON != "" {
			var tags interface{}
			_ = json.Unmarshal([]byte(*tagsJSON), &tags)
			item["tags"] = tags
		}
		if bundlesJSON != nil && *bundlesJSON != "" {
			var bundles interface{}
			_ = json.Unmarshal([]byte(*bundlesJSON), &bundles)
			item["bundles"] = bundles
		}
		data = append(data, item)
	}

	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page + 1) < totalPages,
	})
}

// TypeInfoByID serves GET /api/widgetTypeInfo/{id}.
// TbResource-flavoured slim projection (no descriptor body).
func TypeInfoByID(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	if !httputil.LooksLikeUUID(id) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid widget type id")
		return
	}
	row := dbpkg.Pool.QueryRow(`
		SELECT id, created_time, fqn, name, tenant_id,
		       COALESCE(deprecated,false), COALESCE(scada,false),
		       description, tags, image,
		       COALESCE(descriptor::jsonb->>'type','') AS widget_type
		  FROM widget_type WHERE id = $1`, id)
	var wid, fqn, name, widgetTypeStr string
	var createdTime int64
	var tenantId, description, tagsJSON, image *string
	var deprecated, scada bool
	if err := row.Scan(&wid, &createdTime, &fqn, &name, &tenantId, &deprecated, &scada, &description, &tagsJSON, &image, &widgetTypeStr); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Widget type not found")
		return
	}
	tid := bootstrap.SystemTenantID
	if tenantId != nil {
		tid = *tenantId
	}
	item := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "WIDGET_TYPE", "id": wid},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
		"fqn":         fqn,
		"name":        name,
		"deprecated":  deprecated,
		"scada":       scada,
	}
	if widgetTypeStr != "" {
		item["widgetType"] = widgetTypeStr
	}
	if description != nil {
		item["description"] = *description
	}
	if image != nil {
		item["image"] = *image
	}
	if tagsJSON != nil && *tagsJSON != "" {
		var tags interface{}
		_ = json.Unmarshal([]byte(*tagsJSON), &tags)
		item["tags"] = tags
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(item)
}

// TypeFqns serves GET /api/widgetTypeFqns?widgetsBundleId={}.
// Returns the ordered list of FQNs for a bundle's contained widgets.
func TypeFqns(w http.ResponseWriter, r *http.Request) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	bundleId := r.URL.Query().Get("widgetsBundleId")
	if bundleId == "" {
		httputil.WriteError(w, http.StatusBadRequest, "widgetsBundleId required")
		return
	}
	rows, err := dbpkg.Pool.Query(`
		SELECT wt.fqn FROM widgets_bundle_widget bwt
		  JOIN widget_type wt ON wt.id = bwt.widget_type_id
		 WHERE bwt.widgets_bundle_id = $1
		 ORDER BY bwt.widget_type_order`, bundleId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err == nil {
			out = append(out, f)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// BundleByID serves GET /api/widgetsBundle/{id}.
func BundleByID(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	if !httputil.LooksLikeUUID(id) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid bundle id")
		return
	}
	row := dbpkg.Pool.QueryRow(`
		SELECT id, created_time, alias, tenant_id, title, image,
		       COALESCE(scada,false), description, widgets_bundle_order
		  FROM widgets_bundle WHERE id = $1`, id)
	var bid, title string
	var createdTime int64
	var alias, tenantId, image, description *string
	var scada bool
	var order *int
	if err := row.Scan(&bid, &createdTime, &alias, &tenantId, &title, &image, &scada, &description, &order); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Bundle not found")
		return
	}
	tid := bootstrap.SystemTenantID
	if tenantId != nil {
		tid = *tenantId
	}
	item := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": "WIDGETS_BUNDLE", "id": bid},
		"createdTime": createdTime,
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tid},
		"title":       title,
		"scada":       scada,
	}
	if alias != nil {
		item["alias"] = *alias
	}
	if image != nil {
		item["image"] = *image
	}
	if description != nil {
		item["description"] = *description
	}
	if order != nil {
		item["order"] = *order
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(item)
}

// BundleWidgetTypes serves GET /api/widgetsBundle/{id}/widgetTypes.
func BundleWidgetTypes(w http.ResponseWriter, r *http.Request, bundleId string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	if !httputil.LooksLikeUUID(bundleId) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid bundle id")
		return
	}
	rows, err := dbpkg.Pool.Query(`
		SELECT wt.id, wt.created_time, wt.fqn, wt.name, wt.tenant_id,
		       COALESCE(wt.deprecated,false), COALESCE(wt.scada,false),
		       wt.description, wt.tags, wt.version, wt.descriptor
		  FROM widgets_bundle_widget bwt
		  JOIN widget_type wt ON wt.id = bwt.widget_type_id
		 WHERE bwt.widgets_bundle_id = $1
		 ORDER BY bwt.widget_type_order`, bundleId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()
	out := []map[string]interface{}{}
	for rows.Next() {
		if it := scanWidgetTypeWithDescriptor(rows); it != nil {
			out = append(out, it)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// BundleWidgetTypeFqns serves
// GET /api/widgetsBundle/{id}/widgetTypeFqns.
func BundleWidgetTypeFqns(w http.ResponseWriter, r *http.Request, bundleId string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	if !httputil.LooksLikeUUID(bundleId) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid bundle id")
		return
	}
	rows, err := dbpkg.Pool.Query(`
		SELECT wt.fqn FROM widgets_bundle_widget bwt
		  JOIN widget_type wt ON wt.id = bwt.widget_type_id
		 WHERE bwt.widgets_bundle_id = $1
		 ORDER BY bwt.widget_type_order`, bundleId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err == nil {
			out = append(out, f)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
