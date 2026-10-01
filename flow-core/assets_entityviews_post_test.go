package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/testdb"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): POST /api/assets and
// POST /api/entityViews routed to their GET-only list handlers for every
// method, so a create request silently re-listed instead of creating
// anything. Both now dispatch POST to the real singular-route Save
// functions (asset.Save, entityview.Save) instead.
func TestAssetsAndEntityViewsPost_DispatchToRealCreate(t *testing.T) {
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", testdb.Scoped(t, dsn))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	const tenantID = "77777777-7777-7777-7777-777777777777"
	stmts := []string{
		`DROP TABLE IF EXISTS asset, asset_profile, entity_view, audit_log CASCADE`,
		`CREATE TABLE audit_log (
			id uuid, created_time bigint, tenant_id uuid, customer_id uuid,
			user_id uuid, user_name text, entity_id uuid, entity_type text, entity_name text,
			action_type text, action_status text, action_failure_details text, action_data text)`,
		`CREATE TABLE asset_profile (id uuid PRIMARY KEY, tenant_id uuid, name text, is_default boolean, version bigint, created_time bigint)`,
		`CREATE TABLE asset (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, customer_id uuid,
			name text, type text, label text, asset_profile_id uuid, additional_info text, version bigint)`,
		`CREATE TABLE entity_view (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, customer_id uuid,
			name text, type text, entity_id uuid, entity_type text, keys text,
			start_ts bigint, end_ts bigint, version bigint)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO asset_profile (id, tenant_id, name, is_default, version, created_time)
		 VALUES (gen_random_uuid(), $1, 'Default', true, 1, 1000)`, tenantID,
	); err != nil {
		t.Fatalf("seed asset_profile: %v", err)
	}
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		db.Close()
	})

	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "00000000-0000-0000-0000-000000000001", Email: "x@x.org",
		Authority: "TENANT_ADMIN", TenantID: tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}

	mux := http.NewServeMux()
	registerRoutes(mux, "*")

	t.Run("POST /api/assets creates a real row", func(t *testing.T) {
		body, _ := json.Marshal(map[string]interface{}{"name": "pump-1", "type": "pump"})
		req := httptest.NewRequest(http.MethodPost, "/api/assets", bytes.NewReader(body))
		req.Header.Set("X-Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status: got %d, body=%s", rec.Code, rec.Body.String())
		}
		var count int
		if err := db.QueryRow("SELECT count(*) FROM asset WHERE tenant_id = $1 AND name = 'pump-1'", tenantID).Scan(&count); err != nil {
			t.Fatalf("verify: %v", err)
		}
		if count != 1 {
			t.Fatalf("asset rows named pump-1: got %d, want 1 (a prior version silently re-listed instead of creating)", count)
		}
	})

	t.Run("POST /api/entityViews creates a real row", func(t *testing.T) {
		body, _ := json.Marshal(map[string]interface{}{
			"name": "view-1", "type": "default",
			// The real UI always names a source entity; a view without one is
			// meaningless. Exercise that path rather than the degenerate body.
			"entityId": map[string]interface{}{
				"entityType": "DEVICE",
				"id":         "88888888-8888-8888-8888-888888888888",
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/api/entityViews", bytes.NewReader(body))
		req.Header.Set("X-Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status: got %d, body=%s", rec.Code, rec.Body.String())
		}
		var count int
		if err := db.QueryRow("SELECT count(*) FROM entity_view WHERE tenant_id = $1 AND name = 'view-1'", tenantID).Scan(&count); err != nil {
			t.Fatalf("verify: %v", err)
		}
		if count != 1 {
			t.Fatalf("entity_view rows named view-1: got %d, want 1 (a prior version silently re-listed instead of creating)", count)
		}
		var storedEntity sql.NullString
		if err := db.QueryRow(
			"SELECT entity_id FROM entity_view WHERE tenant_id = $1 AND name = 'view-1'", tenantID,
		).Scan(&storedEntity); err != nil {
			t.Fatalf("read back entity_id: %v", err)
		}
		if !storedEntity.Valid || storedEntity.String != "88888888-8888-8888-8888-888888888888" {
			t.Errorf("entity_id did not round-trip: %+v", storedEntity)
		}
	})

	// entity_id is a uuid column and ExtractEntityID yields "" for an absent
	// field, so a body without entityId used to hand the driver an empty string
	// and fail with `invalid input syntax for type uuid` -- reported as a 500,
	// i.e. a malformed request answered as a server fault. It must store NULL.
	t.Run("POST /api/entityViews without entityId does not 500", func(t *testing.T) {
		body, _ := json.Marshal(map[string]interface{}{"name": "view-no-entity", "type": "default"})
		req := httptest.NewRequest(http.MethodPost, "/api/entityViews", bytes.NewReader(body))
		req.Header.Set("X-Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code == http.StatusInternalServerError {
			t.Fatalf("got 500 for a body with no entityId: %s", rec.Body.String())
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status: got %d, body=%s", rec.Code, rec.Body.String())
		}
		var stored sql.NullString
		if err := db.QueryRow(
			"SELECT entity_id FROM entity_view WHERE tenant_id = $1 AND name = 'view-no-entity'", tenantID,
		).Scan(&stored); err != nil {
			t.Fatalf("verify: %v", err)
		}
		if stored.Valid {
			t.Errorf("entity_id should be NULL when omitted, got %q", stored.String)
		}
	})
}
