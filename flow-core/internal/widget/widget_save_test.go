package widget

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): POST /api/widgetType never read the
// request body or checked the method — a save fell through to the GET
// branches and answered a 400 about missing query params, so custom widget
// authoring from the UI was inert even though widget_type is a real table
// the seeder already writes to.
//
// Throwaway schema (CREATE SCHEMA / DROP SCHEMA CASCADE), so it is safe to
// run against a real database.

const (
	wsTenantA = "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa"
	wsTenantB = "bbbbbbbb-2222-2222-2222-bbbbbbbbbbbb"
)

func newWidgetSaveDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	schema := fmt.Sprintf("widget_save_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE widget_type (
		id uuid PRIMARY KEY, created_time bigint, fqn text, descriptor text,
		name text, tenant_id uuid, image text,
		scada boolean NOT NULL DEFAULT false, deprecated boolean NOT NULL DEFAULT false,
		description text, tags text[], external_id uuid, version bigint DEFAULT 1,
		CONSTRAINT uq_widget_type_fqn UNIQUE (tenant_id, fqn))`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	dbpkg.SetPoolForTest(t, db)
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})
	return db
}

func wsJWT(t *testing.T, tenantID string) string {
	t.Helper()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "00000000-0000-0000-0000-000000000001", Email: "x@x.org",
		Authority: "TENANT_ADMIN", TenantID: tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func postWidgetType(t *testing.T, tenantID string, payload map[string]interface{}) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/api/widgetType", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+wsJWT(t, tenantID))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	Type(rec, req)
	return rec
}

func TestWidgetTypeSave_Persists(t *testing.T) {
	db := newWidgetSaveDB(t)

	rec := postWidgetType(t, wsTenantA, map[string]interface{}{
		"name":        "My Chart",
		"fqn":         "custom.my_chart",
		"description": "hand-authored",
		"tags":        []interface{}{"custom", "chart"},
		"descriptor":  map[string]interface{}{"type": "timeseries", "sizeX": 8},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s (a prior version answered 400 about missing query params)", rec.Code, rec.Body.String())
	}

	var count int
	if err := db.QueryRow(
		"SELECT count(*) FROM widget_type WHERE tenant_id = $1 AND fqn = 'custom.my_chart'", wsTenantA,
	).Scan(&count); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if count != 1 {
		t.Fatalf("rows persisted: got %d, want 1 (a prior version discarded the body)", count)
	}

	// The response must be the stored row, descriptor included.
	var got map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["name"] != "My Chart" || got["fqn"] != "custom.my_chart" {
		t.Errorf("response: got %v", got)
	}
	desc, ok := got["descriptor"].(map[string]interface{})
	if !ok || desc["type"] != "timeseries" {
		t.Errorf("descriptor did not round-trip: %v", got["descriptor"])
	}
}

func TestWidgetTypeSave_Validation(t *testing.T) {
	newWidgetSaveDB(t)

	t.Run("missing name", func(t *testing.T) {
		if rec := postWidgetType(t, wsTenantA, map[string]interface{}{"fqn": "x.y"}); rec.Code != http.StatusBadRequest {
			t.Errorf("got %d, want 400", rec.Code)
		}
	})
	t.Run("missing fqn", func(t *testing.T) {
		if rec := postWidgetType(t, wsTenantA, map[string]interface{}{"name": "X"}); rec.Code != http.StatusBadRequest {
			t.Errorf("got %d, want 400", rec.Code)
		}
	})
	t.Run("GET is unaffected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/widgetType", nil)
		req.Header.Set("X-Authorization", "Bearer "+wsJWT(t, wsTenantA))
		rec := httptest.NewRecorder()
		Type(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("got %d, want the existing 400 about query params", rec.Code)
		}
	})
}

func TestWidgetTypeSave_CrossTenantUpdateDenied(t *testing.T) {
	db := newWidgetSaveDB(t)

	rec := postWidgetType(t, wsTenantA, map[string]interface{}{"name": "A's widget", "fqn": "a.widget"})
	if rec.Code != http.StatusOK {
		t.Fatalf("seed save: %d %s", rec.Code, rec.Body.String())
	}
	var id string
	if err := db.QueryRow("SELECT id::text FROM widget_type WHERE tenant_id = $1", wsTenantA).Scan(&id); err != nil {
		t.Fatalf("read back: %v", err)
	}

	// Tenant B must not be able to overwrite tenant A's widget — nor, by the
	// same check, the shared system-tenant catalogue the seeder installs.
	rec = postWidgetType(t, wsTenantB, map[string]interface{}{
		"id":   map[string]interface{}{"entityType": "WIDGET_TYPE", "id": id},
		"name": "hijacked", "fqn": "a.widget",
	})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("got %d, want 403", rec.Code)
	}
	var name string
	if err := db.QueryRow("SELECT name FROM widget_type WHERE id = $1", id).Scan(&name); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if name != "A's widget" {
		t.Errorf("row was modified across tenants: name = %q", name)
	}
}
