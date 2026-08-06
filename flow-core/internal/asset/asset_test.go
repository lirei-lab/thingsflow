package asset

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
)

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	pool, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := pool.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	dbpkg.SetPoolForTest(t, pool)
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

const tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

func setupTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS audit_log CASCADE`,
		`DROP TABLE IF EXISTS attribute_kv CASCADE`,
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP TABLE IF EXISTS asset_profile CASCADE`,
		`CREATE TABLE audit_log (
			id uuid, created_time bigint, tenant_id uuid, customer_id uuid,
			user_id uuid, user_name text, entity_id uuid, entity_type text, entity_name text,
			action_type text, action_status text, action_failure_details text, action_data text)`,
		`CREATE TABLE asset_profile (
			id uuid PRIMARY KEY, tenant_id uuid, name text, description text,
			is_default boolean, version bigint, created_time bigint)`,
		`CREATE TABLE asset (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, customer_id uuid,
			name text, type text, label text, asset_profile_id uuid,
			additional_info text, version bigint)`,
		`CREATE TABLE attribute_kv (
			entity_id uuid, attribute_type int, attribute_key int,
			str_v text, last_update_ts bigint)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	if _, err := db.Exec(`INSERT INTO asset_profile (id, tenant_id, name, is_default, version, created_time)
		VALUES ('99999999-9999-9999-9999-999999999999', $1, 'default', true, 1, $2)`,
		tenantA, time.Now().UnixMilli()); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
}

func fakeJWT(t *testing.T, tenantID string) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000001",
		Email:     "x@x.org",
		Authority: "TENANT_ADMIN",
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func TestSave_Create(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	tok := fakeJWT(t, tenantA)

	body, _ := json.Marshal(map[string]string{"name": "asset-1", "type": "thermostat"})
	req := httptest.NewRequest("POST", "/api/asset", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	Save(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM asset WHERE tenant_id = $1`, tenantA).Scan(&n)
	if n != 1 {
		t.Errorf("asset rows = %d, want 1", n)
	}
}

func TestSave_MissingName(t *testing.T) {
	newTestDB(t)
	tok := fakeJWT(t, tenantA)
	body, _ := json.Marshal(map[string]string{"type": "default"})
	req := httptest.NewRequest("POST", "/api/asset", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	Save(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestSave_NoAuth(t *testing.T) {
	authpkg.InitConfig()
	body, _ := json.Marshal(map[string]string{"name": "x"})
	req := httptest.NewRequest("POST", "/api/asset", bytes.NewReader(body))
	w := httptest.NewRecorder()
	Save(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestSaveProfile_Create(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	tok := fakeJWT(t, tenantA)

	body, _ := json.Marshal(map[string]interface{}{"name": "MyProfile", "description": "test"})
	req := httptest.NewRequest("POST", "/api/assetProfile", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	SaveProfile(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM asset_profile WHERE name = 'MyProfile'`).Scan(&n)
	if n != 1 {
		t.Errorf("profile rows = %d, want 1", n)
	}
}

func TestDeleteProfile_RefusesDefault(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	tok := fakeJWT(t, tenantA)

	// The seeded default profile id
	const defaultID = "99999999-9999-9999-9999-999999999999"
	req := httptest.NewRequest("DELETE", "/api/assetProfile/"+defaultID, nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	DeleteProfile(w, req, defaultID)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (cannot delete default)", w.Code)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM asset_profile WHERE id = $1`, defaultID).Scan(&n)
	if n != 1 {
		t.Errorf("default profile got deleted! count=%d", n)
	}
}

// The twin registry stays convergent through boot-injected hooks (main.go).
// This pins that asset create and delete fire them with the right identity —
// the hook is a fake, so no twin_registry table is needed here.
func TestAssetCreateAndDeleteFireTwinRegistryHooks(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	tok := fakeJWT(t, tenantA)

	var synced, deleted []string
	TwinRegistrySync = func(tenantID, assetID string) { synced = append(synced, tenantID+"/"+assetID) }
	TwinRegistryDelete = func(tenantID, assetID string) { deleted = append(deleted, tenantID+"/"+assetID) }
	t.Cleanup(func() { TwinRegistrySync = nil; TwinRegistryDelete = nil })

	body, _ := json.Marshal(map[string]string{"name": "twin-hook-asset", "type": "building"})
	req := httptest.NewRequest("POST", "/api/asset", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	Save(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id := resp["id"].(map[string]interface{})["id"].(string)
	if len(synced) != 1 || synced[0] != tenantA+"/"+id {
		t.Fatalf("sync hook calls = %v, want exactly [%s/%s]", synced, tenantA, id)
	}

	req = httptest.NewRequest("DELETE", "/api/asset/"+id, nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w = httptest.NewRecorder()
	Delete(w, req, id)
	if w.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body=%s", w.Code, w.Body.String())
	}
	if len(deleted) != 1 || deleted[0] != tenantA+"/"+id {
		t.Fatalf("delete hook calls = %v, want exactly [%s/%s]", deleted, tenantA, id)
	}
}
