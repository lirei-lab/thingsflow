package system

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/testdb"
)

const bulkImportTenant = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

func newAssetBulkImportDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	pool, err := sql.Open("postgres", testdb.Scoped(t, dsn))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := pool.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	dbpkg.SetPoolForTest(t, pool)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func setupAssetBulkImportTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP TABLE IF EXISTS asset_profile CASCADE`,
		`CREATE TABLE asset_profile (
			id uuid PRIMARY KEY, tenant_id uuid, name text, description text,
			is_default boolean, version bigint, created_time bigint)`,
		`CREATE TABLE asset (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, customer_id uuid,
			name text, type text, label text, asset_profile_id uuid,
			additional_info text, version bigint)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	if _, err := db.Exec(`INSERT INTO asset_profile (id, tenant_id, name, is_default, version, created_time)
		VALUES ('99999999-9999-9999-9999-999999999999', $1, 'default', true, 1, $2)`,
		bulkImportTenant, time.Now().UnixMilli()); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
}

func bulkImportJWT(t *testing.T) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000001",
		Email:     "bulk@test.org",
		Authority: "TENANT_ADMIN",
		TenantID:  bulkImportTenant,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

// Bulk-imported assets must fire the twin registry sync hook (injected at
// boot from main.go) once per CREATED row — skipped duplicates and failed
// rows must not.
func TestAssetBulkImportFiresTwinRegistrySyncHookPerCreatedAsset(t *testing.T) {
	db := newAssetBulkImportDB(t)
	setupAssetBulkImportTables(t, db)

	var synced []string
	AssetTwinRegistrySync = func(tenantID, assetID string) { synced = append(synced, tenantID+"/"+assetID) }
	t.Cleanup(func() { AssetTwinRegistrySync = nil })

	// Pre-existing asset: the importer skips it (update=false) → no hook call.
	if _, err := db.Exec(`INSERT INTO asset (id, created_time, tenant_id, name, type)
		VALUES ('11111111-1111-1111-1111-111111111111', $1, $2, 'existing-asset', 'building')`,
		time.Now().UnixMilli(), bulkImportTenant); err != nil {
		t.Fatalf("seed asset: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"file": "NAME,TYPE\nexisting-asset,building\nnew-asset-1,building\nnew-asset-2,room\n",
		"mapping": map[string]interface{}{
			"columns": []map[string]string{{"type": "NAME"}, {"type": "TYPE"}},
			"header":  true,
		},
	})
	req := httptest.NewRequest("POST", "/api/asset/bulk_import", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+bulkImportJWT(t))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	HandleAssetBulkImport(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if got := resp["created"]; got != float64(2) {
		t.Fatalf("created = %v, want 2; body=%s", got, w.Body.String())
	}
	if len(synced) != 2 {
		t.Fatalf("sync hook fired %d times, want 2 (one per created asset): %v", len(synced), synced)
	}
	for i, call := range synced {
		var n int
		id := call[len(bulkImportTenant)+1:]
		if err := db.QueryRow(`SELECT count(*) FROM asset WHERE id = $1 AND tenant_id = $2`,
			id, bulkImportTenant).Scan(&n); err != nil || n != 1 {
			t.Fatalf("hook call %d (%s) does not match a created asset row (n=%d err=%v)", i, call, n, err)
		}
	}
}
