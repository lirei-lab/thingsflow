package tenant

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/testdb"
)

// Cross-tenant IDOR coverage for the GET-by-id handlers: a TENANT_ADMIN of
// tenant A must not be able to read tenant B's user or customer by UUID, while
// a SYS_ADMIN may read across tenants.

const (
	tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	userAID = "11111111-1111-1111-1111-111111111111"
	userBID = "22222222-2222-2222-2222-222222222222"
	custAID = "33333333-3333-3333-3333-333333333333"
	custBID = "44444444-4444-4444-4444-444444444444"
)

func newAuthzDB(t *testing.T) *sql.DB {
	t.Helper()
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
	// Pin a stable signing key so tokens minted before the handler's Extract
	// validate against the same key. Without this, InitConfig would generate a
	// fresh random key on each call and invalidate earlier tokens.
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()
	stmts := []string{
		`DROP TABLE IF EXISTS user_credentials CASCADE`,
		`DROP TABLE IF EXISTS tb_user CASCADE`,
		`DROP TABLE IF EXISTS customer CASCADE`,
		`CREATE TABLE tb_user (
			id uuid PRIMARY KEY, created_time bigint, email text, authority text,
			tenant_id uuid, customer_id uuid, first_name text, last_name text,
			phone text, additional_info text)`,
		`CREATE TABLE user_credentials (
			user_id uuid PRIMARY KEY, password text, enabled boolean)`,
		`CREATE TABLE customer (
			id uuid PRIMARY KEY, created_time bigint, title text, tenant_id uuid,
			additional_info text, country text, state text, city text, address text,
			address2 text, zip text, phone text, email text, version bigint)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	now := time.Now().UnixMilli()
	seed := []struct {
		q    string
		args []any
	}{
		{`INSERT INTO tb_user (id, created_time, email, authority, tenant_id) VALUES ($1,$2,$3,'TENANT_ADMIN',$4)`, []any{userAID, now, "a@x.org", tenantA}},
		{`INSERT INTO tb_user (id, created_time, email, authority, tenant_id) VALUES ($1,$2,$3,'TENANT_ADMIN',$4)`, []any{userBID, now, "b@x.org", tenantB}},
		{`INSERT INTO user_credentials (user_id, password, enabled) VALUES ($1,'x',true)`, []any{userAID}},
		{`INSERT INTO user_credentials (user_id, password, enabled) VALUES ($1,'x',true)`, []any{userBID}},
		{`INSERT INTO customer (id, created_time, title, tenant_id, version) VALUES ($1,$2,'Cust A',$3,1)`, []any{custAID, now, tenantA}},
		{`INSERT INTO customer (id, created_time, title, tenant_id, version) VALUES ($1,$2,'Cust B',$3,1)`, []any{custBID, now, tenantB}},
	}
	for _, s := range seed {
		if _, err := db.Exec(s.q, s.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		db.Close()
	})
	return db
}

func authzJWT(t *testing.T, tenantID, authority string) string {
	t.Helper()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000009",
		Email:     "caller@x.org",
		Authority: authority,
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func doGet(t *testing.T, h func(http.ResponseWriter, *http.Request, string), id, tok string) int {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/x/"+id, nil)
	if tok != "" {
		req.Header.Set("X-Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	h(w, req, id)
	return w.Code
}

func TestHandleUserById_TenantIsolation(t *testing.T) {
	newAuthzDB(t)
	tenantAdminA := authzJWT(t, tenantA, "TENANT_ADMIN")
	sysAdmin := authzJWT(t, "", "SYS_ADMIN")

	if code := doGet(t, HandleUserById, userAID, tenantAdminA); code != http.StatusOK {
		t.Errorf("own user: status = %d, want 200", code)
	}
	if code := doGet(t, HandleUserById, userBID, tenantAdminA); code != http.StatusForbidden {
		t.Errorf("cross-tenant user: status = %d, want 403", code)
	}
	if code := doGet(t, HandleUserById, userBID, sysAdmin); code != http.StatusOK {
		t.Errorf("sysadmin cross-tenant user: status = %d, want 200", code)
	}
	if code := doGet(t, HandleUserById, userAID, ""); code != http.StatusUnauthorized {
		t.Errorf("no token: status = %d, want 401", code)
	}
}

func TestHandleCustomerById_TenantIsolation(t *testing.T) {
	newAuthzDB(t)
	tenantAdminA := authzJWT(t, tenantA, "TENANT_ADMIN")
	sysAdmin := authzJWT(t, "", "SYS_ADMIN")

	if code := doGet(t, HandleCustomerById, custAID, tenantAdminA); code != http.StatusOK {
		t.Errorf("own customer: status = %d, want 200", code)
	}
	if code := doGet(t, HandleCustomerById, custBID, tenantAdminA); code != http.StatusForbidden {
		t.Errorf("cross-tenant customer: status = %d, want 403", code)
	}
	if code := doGet(t, HandleCustomerById, custBID, sysAdmin); code != http.StatusOK {
		t.Errorf("sysadmin cross-tenant customer: status = %d, want 200", code)
	}
}
