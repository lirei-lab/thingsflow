package device

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/quotas"
)

// Supported ThingsBoard CSV column types. Fleet onboarding (a spreadsheet of meters) only
// needs identity + credential columns; attribute/timeseries column types are accepted by
// the UI but are reported as unsupported rather than silently ignored, so an operator is
// never told "imported" about data that was dropped.
const (
	csvColName        = "NAME"
	csvColType        = "TYPE"
	csvColLabel       = "LABEL"
	csvColAccessToken = "ACCESS_TOKEN"
	csvColDescription = "DESCRIPTION"
	csvColIsGateway   = "IS_GATEWAY"
)

// bulkImportMapping mirrors the UI's BulkImportRequest.mapping.
type bulkImportMapping struct {
	Columns []struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	} `json:"columns"`
	Delimiter string `json:"delimiter"`
	Update    bool   `json:"update"`
	Header    bool   `json:"header"`
}

// HandleDeviceBulkImport processes POST /api/device/bulk_import — the UI's CSV import.
//
// Request:  {"file": "<raw csv>", "mapping": {columns, delimiter, update, header}}
// Response: {"created": N, "updated": N, "errors": N, "errorsList": [...]}
//
// Devices created here get an auto-generated ACCESS_TOKEN unless the CSV carries an
// ACCESS_TOKEN column. Note that on this platform the access token is NOT the ingest
// credential (ingest authenticates with a short-lived device JWT), so bulk import is for
// registering the fleet — each device still obtains a JWT via provisioning or
// POST /api/device/{id}/jwt.
func HandleDeviceBulkImport(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var body struct {
		File    string            `json:"file"`
		Mapping bulkImportMapping `json:"mapping"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}
	if strings.TrimSpace(body.File) == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing file content")
		return
	}
	if len(body.Mapping.Columns) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "Missing column mapping")
		return
	}

	reader := csv.NewReader(strings.NewReader(body.File))
	reader.Comma = bulkImportDelimiter(body.Mapping.Delimiter)
	reader.FieldsPerRecord = -1 // tolerate ragged rows; report them per-row instead
	reader.TrimLeadingSpace = true
	records, err := reader.ReadAll()
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Failed to parse CSV: "+err.Error())
		return
	}
	if body.Mapping.Header && len(records) > 0 {
		records = records[1:]
	}

	// Resolve the default profile once — every created device needs one.
	var defaultProfileID string
	if err := dbpkg.Pool.QueryRow(
		"SELECT id FROM device_profile WHERE tenant_id = $1 AND is_default = true LIMIT 1",
		tenantId,
	).Scan(&defaultProfileID); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "No default device profile available")
		return
	}

	// failedRows drives the `errors` count: only rows that did NOT import. Warnings (e.g.
	// unsupported column types on a row that imported fine) go to messages so the operator
	// still sees them, without "created: 3, errors: 3" implying the import half-failed.
	created, updated, failedRows := 0, 0, 0
	messages := []string{}
	unsupportedSeen := map[string]bool{}
	fail := func(format string, args ...interface{}) {
		failedRows++
		messages = append(messages, fmt.Sprintf(format, args...))
	}

	for i, record := range records {
		rowNum := i + 1
		if body.Mapping.Header {
			rowNum++
		}
		if len(record) == 0 || strings.TrimSpace(strings.Join(record, "")) == "" {
			continue // blank line
		}

		fields := map[string]string{}
		unsupported := map[string]bool{}
		for colIdx, col := range body.Mapping.Columns {
			if colIdx >= len(record) {
				continue
			}
			value := strings.TrimSpace(record[colIdx])
			switch strings.ToUpper(col.Type) {
			case csvColName, csvColType, csvColLabel, csvColAccessToken, csvColDescription, csvColIsGateway:
				fields[strings.ToUpper(col.Type)] = value
			default:
				if value != "" {
					unsupported[strings.ToUpper(col.Type)] = true
				}
			}
		}

		name := fields[csvColName]
		if name == "" {
			fail("row %d: missing device name", rowNum)
			continue
		}
		// Collect unsupported column types once for the whole file rather than per row —
		// it is a property of the mapping, not of any individual row.
		for k := range unsupported {
			unsupportedSeen[k] = true
		}

		// Validate any supplied credential before touching the DB for this row.
		var credential deviceCredentialInput
		hasCredential := fields[csvColAccessToken] != ""
		if hasCredential {
			var cerr error
			credential, _, cerr = normalizeCredentialInput(map[string]interface{}{
				"credentialsType": "ACCESS_TOKEN",
				"credentialsId":   fields[csvColAccessToken],
			})
			if cerr != nil {
				fail("row %d (%s): %v", rowNum, name, cerr)
				continue
			}
		}

		var existingID string
		err := dbpkg.Pool.QueryRow(
			"SELECT id FROM device WHERE tenant_id = $1 AND name = $2", tenantId, name).Scan(&existingID)
		switch {
		case err == nil && !body.Mapping.Update:
			fail("row %d (%s): device already exists", rowNum, name)
			continue
		case err == nil:
			if uerr := bulkUpdateDevice(existingID, fields); uerr != nil {
				fail("row %d (%s): %v", rowNum, name, uerr)
				continue
			}
			if hasCredential {
				if cerr := upsertDeviceCredentials(existingID, credential); cerr != nil {
					fail("row %d (%s): credentials failed", rowNum, name)
					continue
				}
			}
			updated++
		default:
			// Reuse the shared quota rule; captureWriter absorbs its error body so the
			// per-row failure is reported in errorsList instead of aborting the response.
			if !quotas.Enforce(newCaptureWriter(), tenantId, "device") {
				fail("row %d (%s): tenant device quota exceeded", rowNum, name)
				continue
			}
			newID, cerr := bulkCreateDevice(tenantId, defaultProfileID, fields)
			if cerr != nil {
				fail("row %d (%s): %v", rowNum, name, cerr)
				continue
			}
			if !hasCredential {
				credential = deviceCredentialInput{Type: "ACCESS_TOKEN", ID: generateAccessToken20(), AutoGenerated: true}
			}
			if cerr := upsertDeviceCredentials(newID, credential); cerr != nil {
				log.Printf("WARN bulk_import: credentials for %s failed: %v", newID, cerr)
			}
			audit.EntityChange(claims, "DEVICE", newID, name, "ADDED")
			created++
		}
	}

	// Surface unsupported column types once, as a warning that does not inflate `errors`.
	if len(unsupportedSeen) > 0 {
		kinds := make([]string, 0, len(unsupportedSeen))
		for k := range unsupportedSeen {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		messages = append(messages, fmt.Sprintf(
			"warning: column type(s) %s are not supported by this platform and were not imported; "+
				"devices themselves imported normally", strings.Join(kinds, ", ")))
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"created":    created,
		"updated":    updated,
		"errors":     failedRows,
		"errorsList": messages,
	})
}

func bulkImportDelimiter(d string) rune {
	switch d {
	case "":
		return ','
	case "\\t", "\t":
		return '\t'
	default:
		return []rune(d)[0]
	}
}

func bulkCreateDevice(tenantID, profileID string, fields map[string]string) (string, error) {
	id := uuid.New().String()
	devType := fields[csvColType]
	if devType == "" {
		devType = "default"
	}
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO device (id, created_time, name, type, label, device_profile_id, additional_info, tenant_id, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1)`,
		id, time.Now().UnixMilli(), fields[csvColName], devType,
		dbutil.NullStr(fields[csvColLabel]), profileID, bulkAdditionalInfo(fields), tenantID)
	if err != nil {
		return "", fmt.Errorf("create failed")
	}
	if TwinRegistrySync != nil {
		TwinRegistrySync(tenantID, id)
	}
	return id, nil
}

func bulkUpdateDevice(id string, fields map[string]string) error {
	devType := fields[csvColType]
	if devType == "" {
		devType = "default"
	}
	_, err := dbpkg.Pool.Exec(`
		UPDATE device SET type = $1, label = $2, additional_info = $3,
		    version = COALESCE(version, 1) + 1
		 WHERE id = $4`,
		devType, dbutil.NullStr(fields[csvColLabel]), bulkAdditionalInfo(fields), id)
	if err != nil {
		return fmt.Errorf("update failed")
	}
	return nil
}

// bulkAdditionalInfo carries the TB-conventional description/gateway flags that live in
// device.additional_info rather than dedicated columns.
func bulkAdditionalInfo(fields map[string]string) interface{} {
	info := map[string]interface{}{}
	if d := fields[csvColDescription]; d != "" {
		info["description"] = d
	}
	if g := strings.ToLower(fields[csvColIsGateway]); g == "true" || g == "yes" || g == "1" {
		info["gateway"] = true
	}
	if len(info) == 0 {
		return nil
	}
	return dbutil.JSONOrNil(info)
}
