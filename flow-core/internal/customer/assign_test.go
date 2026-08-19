package customer

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

const (
	custA = "cccccccc-cccc-cccc-cccc-cccccccccc01"
	custB = "cccccccc-cccc-cccc-cccc-cccccccccc02"
	devA  = "dddddddd-dddd-dddd-dddd-dddddddddd01"
	dashA = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeee01"
)

// setupAssignTables adds the entity tables the assignment handlers touch on top
// of the shared customer/audit schema.
func setupAssignTables(t *testing.T, db *sql.DB) {
	t.Helper()
	setupTables(t, db)
	stmts := []string{
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS dashboard CASCADE`,
		`CREATE TABLE device (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text)`,
		`CREATE TABLE dashboard (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			title text, assigned_customers varchar(1000000))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	seed := []struct {
		q    string
		args []interface{}
	}{
		{`INSERT INTO customer (id, created_time, tenant_id, title) VALUES ($1,0,$2,'Sitio Norte')`, []interface{}{custA, tenantA}},
		{`INSERT INTO customer (id, created_time, tenant_id, title) VALUES ($1,0,$2,'Ajeno')`, []interface{}{custB, tenantB}},
		{`INSERT INTO device (id, created_time, tenant_id, name, type) VALUES ($1,0,$2,'medidor-1','default')`, []interface{}{devA, tenantA}},
		{`INSERT INTO dashboard (id, created_time, tenant_id, title) VALUES ($1,0,$2,'Consumo')`, []interface{}{dashA, tenantA}},
	}
	for _, s := range seed {
		if _, err := db.Exec(s.q, s.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func deviceCustomer(t *testing.T, db *sql.DB) string {
	t.Helper()
	var c sql.NullString
	if err := db.QueryRow(`SELECT customer_id::text FROM device WHERE id = $1`, devA).Scan(&c); err != nil {
		t.Fatalf("read device: %v", err)
	}
	return c.String
}

func TestAssignDevice_RoundTrip(t *testing.T) {
	db := newTestDB(t)
	setupAssignTables(t, db)
	tok := fakeJWT(t, tenantA)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/customer/x", nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	HandleAssignToCustomer(rec, req, custA, "device", devA)

	if rec.Code != http.StatusOK {
		t.Fatalf("assign status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if got := deviceCustomer(t, db); got != custA {
		t.Fatalf("device customer_id = %q, want %q", got, custA)
	}

	// The response must carry the new owner so the UI can refresh the row.
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	ref, _ := body["customerId"].(map[string]interface{})
	if ref == nil || ref["id"] != custA {
		t.Fatalf("response customerId = %v, want %s", body["customerId"], custA)
	}

	// Unassign clears the column.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodDelete, "/api/customer/device/"+devA, nil)
	req2.Header.Set("X-Authorization", "Bearer "+tok)
	HandleAssignToCustomer(rec2, req2, "", "device", devA)

	if rec2.Code != http.StatusOK {
		t.Fatalf("unassign status = %d, want 200", rec2.Code)
	}
	if got := deviceCustomer(t, db); got != "" {
		t.Fatalf("device customer_id after unassign = %q, want empty", got)
	}
}

// A tenant must not be able to hand its device to another tenant's customer,
// nor touch another tenant's device.
func TestAssignDevice_CrossTenantDenied(t *testing.T) {
	db := newTestDB(t)
	setupAssignTables(t, db)

	// tenantA's device → tenantB's customer.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/customer/x", nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeJWT(t, tenantA))
	HandleAssignToCustomer(rec, req, custB, "device", devA)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("foreign customer status = %d, want 403", rec.Code)
	}

	// tenantB trying to grab tenantA's device.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/customer/x", nil)
	req2.Header.Set("X-Authorization", "Bearer "+fakeJWT(t, tenantB))
	HandleAssignToCustomer(rec2, req2, custB, "device", devA)
	if rec2.Code != http.StatusForbidden {
		t.Fatalf("foreign device status = %d, want 403", rec2.Code)
	}

	if got := deviceCustomer(t, db); got != "" {
		t.Fatalf("device customer_id = %q after denied assignments, want empty", got)
	}
}

func TestAssignDevice_UnknownEntityKind(t *testing.T) {
	db := newTestDB(t)
	setupAssignTables(t, db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/customer/x", nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeJWT(t, tenantA))
	HandleAssignToCustomer(rec, req, custA, "widget", devA)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	_ = db
}

func dashboardAssigned(t *testing.T, db *sql.DB) string {
	t.Helper()
	var s sql.NullString
	if err := db.QueryRow(`SELECT assigned_customers FROM dashboard WHERE id = $1`, dashA).Scan(&s); err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	return s.String
}

func TestAssignDashboard_RoundTripIsIdempotent(t *testing.T) {
	db := newTestDB(t)
	setupAssignTables(t, db)
	tok := fakeJWT(t, tenantA)

	assign := func(on bool) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/customer/x", nil)
		req.Header.Set("X-Authorization", "Bearer "+tok)
		HandleAssignDashboardToCustomer(rec, req, custA, dashA, on)
		return rec
	}

	// Assigning twice must not duplicate the entry — the UI retries freely.
	assign(true)
	rec := assign(true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	var entries []map[string]interface{}
	if err := json.Unmarshal([]byte(dashboardAssigned(t, db)), &entries); err != nil {
		t.Fatalf("stored assigned_customers is not JSON: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("assigned_customers has %d entries, want 1: %s", len(entries), dashboardAssigned(t, db))
	}
	if got := entries[0]["title"]; got != "Sitio Norte" {
		t.Fatalf("entry title = %v, want the customer title", got)
	}

	// Unassign empties the column back to NULL rather than leaving "[]".
	assign(false)
	if got := dashboardAssigned(t, db); got != "" {
		t.Fatalf("assigned_customers after unassign = %q, want empty", got)
	}
}

func TestAssignDashboard_CrossTenantDenied(t *testing.T) {
	db := newTestDB(t)
	setupAssignTables(t, db)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/customer/x", nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeJWT(t, tenantB))
	HandleAssignDashboardToCustomer(rec, req, custB, dashA, true)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if got := dashboardAssigned(t, db); got != "" {
		t.Fatalf("assigned_customers = %q after denied assign, want empty", got)
	}
}

// Public sharing is not implemented on this platform: no code ever creates
// the per-tenant "Public" customer row TB assigns to, and
// /api/auth/login/public is a documented 501 because there is no
// customer-scoped authorization to bound such a token with. These endpoints
// used to fail as a 403 about the customer belonging to another tenant —
// untrue ("public" names no customer at all) and misleading, since it reads
// as a permissions problem an operator could fix rather than a missing
// capability. See docs/UI_CONTRACT_DATA_FIDELITY.md.
func TestAssignToPublic_IsNotImplemented(t *testing.T) {
	db := newTestDB(t)
	setupAssignTables(t, db)
	tok := fakeJWT(t, tenantA)

	t.Run("device", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/customer/public/device/x", nil)
		req.Header.Set("X-Authorization", "Bearer "+tok)
		HandleAssignToCustomer(rec, req, "public", "device", devA)
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("status = %d, want 501 (was a misleading 403)", rec.Code)
		}
		if got := deviceCustomer(t, db); got != "" {
			t.Fatalf("device customer_id = %q, want unchanged/empty", got)
		}
	})

	t.Run("dashboard", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/customer/public/dashboard/x", nil)
		req.Header.Set("X-Authorization", "Bearer "+tok)
		HandleAssignDashboardToCustomer(rec, req, "public", dashA, true)
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("status = %d, want 501", rec.Code)
		}
		if got := dashboardAssigned(t, db); got != "" {
			t.Fatalf("assigned_customers = %q, want empty", got)
		}
	})

	// The UI contract pins 404 for these paths, because its probes name an
	// entity that does not exist. The 501 must therefore sit AFTER the
	// entity lookup, or the contract check breaks.
	t.Run("a missing entity still 404s, not 501", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/customer/public/device/x", nil)
		req.Header.Set("X-Authorization", "Bearer "+tok)
		HandleAssignToCustomer(rec, req, "public", "device", "00000000-0000-4000-8000-000000000000")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (the UI contract pins this)", rec.Code)
		}
	})

	t.Run("a real customer still assigns normally", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/customer/x", nil)
		req.Header.Set("X-Authorization", "Bearer "+tok)
		HandleAssignToCustomer(rec, req, custA, "device", devA)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := deviceCustomer(t, db); got != custA {
			t.Fatalf("device customer_id = %q, want %q", got, custA)
		}
	})
}
