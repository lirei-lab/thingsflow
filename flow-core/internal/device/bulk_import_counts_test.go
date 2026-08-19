package device

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Closes the `needs-live-check` item from the ui-contract data-fidelity audit
// (docs/UI_CONTRACT_DATA_FIDELITY.md): POST /api/device/bulk_import does real
// per-row work, but nothing asserted that the created/updated/errors counts it
// reports actually match what landed. A count that silently disagrees with the
// database is the same class of defect as a hardcoded one — the operator has
// no way to tell an import half-failed.
func TestDeviceBulkImport_CountsMatchWhatLanded(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	tok := fakeJWT(t, tenantA)

	post := func(csv string, update bool) map[string]interface{} {
		t.Helper()
		body, _ := json.Marshal(map[string]interface{}{
			"file": csv,
			"mapping": map[string]interface{}{
				"header":    true,
				"delimiter": ",",
				"update":    update,
				"columns": []map[string]interface{}{
					{"type": "NAME"},
					{"type": "TYPE"},
				},
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/api/device/bulk_import", bytes.NewReader(body))
		req.Header.Set("X-Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		HandleDeviceBulkImport(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		var out map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v\nbody=%s", err, rec.Body.String())
		}
		return out
	}

	countDevices := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow("SELECT count(*) FROM device WHERE tenant_id = $1", tenantA).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	t.Run("fresh rows are counted as created", func(t *testing.T) {
		got := post("name,type\npump-1,pump\npump-2,pump\n", false)
		if got["created"] != float64(2) {
			t.Errorf("created: got %v, want 2", got["created"])
		}
		if got["updated"] != float64(0) {
			t.Errorf("updated: got %v, want 0", got["updated"])
		}
		if got["errors"] != float64(0) {
			t.Errorf("errors: got %v, want 0 — messages=%v", got["errors"], got["errorsList"])
		}
		if n := countDevices(); n != 2 {
			t.Errorf("rows in device: got %d, want 2 — the reported count must match what landed", n)
		}
	})

	// Without mapping.update, an existing name is a per-row error rather than
	// an upsert — and, importantly, nothing is duplicated. The counts must say
	// so honestly: this is the case where a wrong "created" would be worst,
	// since the operator would believe rows landed that did not.
	t.Run("a repeat without update reports errors and duplicates nothing", func(t *testing.T) {
		got := post("name,type\npump-1,pump\npump-2,pump\n", false)
		if got["errors"] != float64(2) {
			t.Errorf("errors: got %v, want 2 — messages=%v", got["errors"], got["errorsList"])
		}
		if got["created"] != float64(0) || got["updated"] != float64(0) {
			t.Errorf("created/updated: got %v/%v, want 0/0", got["created"], got["updated"])
		}
		if n := countDevices(); n != 2 {
			t.Errorf("rows in device: got %d, want still 2 (nothing duplicated)", n)
		}
	})

	t.Run("a repeat with update is counted as updated", func(t *testing.T) {
		got := post("name,type\npump-1,valve\npump-2,valve\n", true)
		if got["updated"] != float64(2) {
			t.Errorf("updated: got %v, want 2 — messages=%v", got["updated"], got["errorsList"])
		}
		if got["created"] != float64(0) {
			t.Errorf("created: got %v, want 0", got["created"])
		}
		if n := countDevices(); n != 2 {
			t.Errorf("rows in device: got %d, want still 2 (no duplicates)", n)
		}
		// The update must actually have been applied, not just counted.
		var typ string
		if err := db.QueryRow("SELECT type FROM device WHERE tenant_id = $1 AND name = 'pump-1'", tenantA).Scan(&typ); err != nil {
			t.Fatalf("verify: %v", err)
		}
		if typ != "valve" {
			t.Errorf("device type: got %q, want %q — reported as updated but the row did not change", typ, "valve")
		}
	})

	t.Run("a nameless row is an error and lands nothing", func(t *testing.T) {
		before := countDevices()
		got := post("name,type\n,pump\npump-3,pump\n", false)
		if got["errors"] != float64(1) {
			t.Errorf("errors: got %v, want 1", got["errors"])
		}
		if got["created"] != float64(1) {
			t.Errorf("created: got %v, want 1 (the valid row must still import)", got["created"])
		}
		if n := countDevices(); n != before+1 {
			t.Errorf("rows in device: got %d, want %d — the error row must not have landed", n, before+1)
		}
	})
}
