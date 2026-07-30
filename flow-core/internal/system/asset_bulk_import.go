package system

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// HandleAssetBulkImport — POST /api/asset/bulk_import.
//
// Same request shape as the device importer: a raw CSV plus a column mapping.
// Assets have no credentials, so this is identity only — NAME, TYPE, LABEL.
//
// `errors` counts rows that failed, not rows that were skipped as duplicates,
// so an import that finds everything already present reports 0 errors rather
// than looking like a total failure.
func HandleAssetBulkImport(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	raw, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Unreadable body")
		return
	}
	var body struct {
		File    string `json:"file"`
		Mapping struct {
			Columns []struct {
				Type string `json:"type"`
			} `json:"columns"`
			Delimiter string `json:"delimiter"`
			Update    bool   `json:"update"`
			Header    bool   `json:"header"`
		} `json:"mapping"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if strings.TrimSpace(body.File) == "" {
		httputil.WriteError(w, http.StatusBadRequest, "file is required")
		return
	}

	reader := csv.NewReader(strings.NewReader(body.File))
	reader.FieldsPerRecord = -1
	reader.Comma = ','
	if d := strings.TrimSpace(body.Mapping.Delimiter); d != "" {
		reader.Comma = rune(d[0])
	}
	records, err := reader.ReadAll()
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Malformed CSV: "+err.Error())
		return
	}
	if len(records) == 0 {
		httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{"created": 0, "updated": 0, "errors": 0})
		return
	}

	// Column order comes from the mapping when present; otherwise the header row.
	var colTypes []string
	for _, c := range body.Mapping.Columns {
		colTypes = append(colTypes, strings.ToUpper(strings.TrimSpace(c.Type)))
	}
	start := 0
	if len(colTypes) == 0 {
		for _, h := range records[0] {
			colTypes = append(colTypes, strings.ToUpper(strings.TrimSpace(h)))
		}
		start = 1
	} else if body.Mapping.Header {
		start = 1
	}

	defaultProfile := defaultAssetProfileID(tenantID)
	created, updated, failed := 0, 0, 0
	var warnings []string
	unsupported := map[string]bool{}

	for _, row := range records[start:] {
		fields := map[string]string{}
		for i, v := range row {
			if i >= len(colTypes) {
				break
			}
			switch colTypes[i] {
			case "NAME", "TYPE", "LABEL", "DESCRIPTION":
				fields[colTypes[i]] = strings.TrimSpace(v)
			default:
				// Attribute/timeseries columns are reported once rather than
				// dropped silently, so an operator is not left believing they
				// were imported.
				unsupported[colTypes[i]] = true
			}
		}
		name := fields["NAME"]
		if name == "" {
			failed++
			continue
		}
		var existing string
		err := dbpkg.Pool.QueryRow(
			"SELECT id::text FROM asset WHERE tenant_id = $1 AND name = $2", tenantID, name).Scan(&existing)
		if err == nil {
			if !body.Mapping.Update {
				continue // already present and update not requested — not an error
			}
			if _, uerr := dbpkg.Pool.Exec(
				"UPDATE asset SET type = COALESCE(NULLIF($1,''), type), label = COALESCE(NULLIF($2,''), label) WHERE id = $3",
				fields["TYPE"], fields["LABEL"], existing); uerr != nil {
				failed++
				continue
			}
			updated++
			continue
		}
		if _, ierr := dbpkg.Pool.Exec(`
			INSERT INTO asset (id, created_time, tenant_id, name, type, label, asset_profile_id)
			VALUES ($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''),$7)`,
			uuid.NewString(), time.Now().UnixMilli(), tenantID, name,
			fields["TYPE"], fields["LABEL"], nullableUUID(defaultProfile)); ierr != nil {
			failed++
			continue
		}
		created++
	}

	if len(unsupported) > 0 {
		cols := make([]string, 0, len(unsupported))
		for c := range unsupported {
			cols = append(cols, c)
		}
		warnings = append(warnings,
			"ignored unsupported columns: "+strings.Join(cols, ", ")+
				" (attribute and timeseries import is not supported)")
	}

	out := map[string]interface{}{"created": created, "updated": updated, "errors": failed}
	if len(warnings) > 0 {
		out["warnings"] = warnings
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

func defaultAssetProfileID(tenantID string) string {
	var id string
	if err := dbpkg.Pool.QueryRow(
		"SELECT id::text FROM asset_profile WHERE tenant_id = $1 AND is_default = true LIMIT 1",
		tenantID).Scan(&id); err != nil {
		return ""
	}
	return id
}

func nullableUUID(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
