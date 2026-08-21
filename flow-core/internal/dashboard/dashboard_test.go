package dashboard

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

const tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
const tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

func newTestDB(t *testing.T) *sql.DB {
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
		`DROP TABLE IF EXISTS dashboard CASCADE`,
		`CREATE TABLE audit_log (
			id uuid, created_time bigint, tenant_id uuid, customer_id uuid,
			user_id uuid, user_name text, entity_id uuid, entity_type text, entity_name text,
			action_type text, action_status text, action_failure_details text, action_data text)`,
		`CREATE TABLE dashboard (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			title text, configuration text, assigned_customers text,
			mobile_hide boolean, mobile_order int, image text, version bigint)`,
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

	body, _ := json.Marshal(map[string]interface{}{
		"title":         "Test Dashboard",
		"mobileHide":    false,
		"configuration": map[string]interface{}{"widgets": []interface{}{}},
	})
	req := httptest.NewRequest("POST", "/api/dashboard", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	Save(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM dashboard WHERE tenant_id = $1`, tenantA).Scan(&n)
	if n != 1 {
		t.Errorf("dashboards = %d, want 1", n)
	}
}

func TestByID_NotFound(t *testing.T) {
	newTestDB(t)
	tok := fakeJWT(t, tenantA)
	req := httptest.NewRequest("GET", "/api/dashboard/00000000-0000-0000-0000-000000000000", nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	ByID(w, req, "00000000-0000-0000-0000-000000000000")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestDelete_CrossTenant(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	const targetID = "dddddddd-0000-0000-0000-000000000001"
	if _, err := db.Exec(`INSERT INTO dashboard (id, created_time, tenant_id, title, version)
		VALUES ($1, $2, $3, 'theirs', 1)`,
		targetID, time.Now().UnixMilli(), tenantB); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tok := fakeJWT(t, tenantA)
	req := httptest.NewRequest("DELETE", "/api/dashboard/"+targetID, nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	Delete(w, req, targetID)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

func TestListByTenant_Empty(t *testing.T) {
	newTestDB(t)
	setupTables(t, dbpkg.Pool)
	tok := fakeJWT(t, tenantA)

	req := httptest.NewRequest("GET", "/api/tenant/dashboards?pageSize=10&page=0", nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	ListByTenant(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["totalElements"].(float64)) != 0 {
		t.Errorf("totalElements = %v, want 0", resp["totalElements"])
	}
}

func TestUserList_NoAuth(t *testing.T) {
	authpkg.InitConfig()
	req := httptest.NewRequest("GET", "/api/user/dashboards", nil)
	w := httptest.NewRecorder()
	UserList(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestVisit_AlwaysOK(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/user/dashboards/x/VISIT", nil)
	w := httptest.NewRecorder()
	Visit(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestHome_NoAuth(t *testing.T) {
	authpkg.InitConfig()
	req := httptest.NewRequest("GET", "/api/dashboard/home", nil)
	w := httptest.NewRecorder()
	Home(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}
