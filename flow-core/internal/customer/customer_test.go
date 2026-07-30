package customer

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

const tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
const tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

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

func setupTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS audit_log CASCADE`,
		`DROP TABLE IF EXISTS customer CASCADE`,
		`CREATE TABLE audit_log (
			id uuid, created_time bigint, tenant_id uuid, customer_id uuid,
			user_id uuid, user_name text, entity_id uuid, entity_type text, entity_name text,
			action_type text, action_status text, action_failure_details text, action_data text)`,
		`CREATE TABLE customer (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			title text, email text, phone text, country text, state text,
			city text, address text, address2 text, zip text,
			additional_info text, version bigint)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
}

func fakeJWT(t *testing.T, tenantID string) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "00000000-0000-0000-0000-000000000001",
		Email:  "x@x.org", Authority: "TENANT_ADMIN", TenantID: tenantID,
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

	body, _ := json.Marshal(map[string]string{"title": "Acme Corp", "email": "ops@acme.com"})
	req := httptest.NewRequest("POST", "/api/customer", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	Save(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM customer WHERE tenant_id = $1`, tenantA).Scan(&n)
	if n != 1 {
		t.Errorf("rows = %d, want 1", n)
	}
}

func TestSave_MissingTitle(t *testing.T) {
	newTestDB(t)
	tok := fakeJWT(t, tenantA)
	body, _ := json.Marshal(map[string]string{"email": "x@y"})
	req := httptest.NewRequest("POST", "/api/customer", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	Save(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestDelete_CrossTenant(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	const targetID = "ccccccc0-0000-0000-0000-000000000001"
	if _, err := db.Exec(`INSERT INTO customer (id, created_time, tenant_id, title, version)
		VALUES ($1, $2, $3, 'theirs', 1)`,
		targetID, time.Now().UnixMilli(), tenantB); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tok := fakeJWT(t, tenantA)
	req := httptest.NewRequest("DELETE", "/api/customer/"+targetID, nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	Delete(w, req, targetID)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 (cross-tenant)", w.Code)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM customer WHERE id = $1`, targetID).Scan(&n)
	if n != 1 {
		t.Errorf("customer deleted across tenants! count=%d", n)
	}
}
