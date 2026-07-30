package user

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

// Privilege-escalation coverage for POST/PUT /api/user
// (HandleUserCreateOrUpdate). A caller must not be able to grant an
// authority above what its own role permits — the core of the fix is that
// `authority` from the body is now checked against the CALLER's role, not
// just a same-tenant check.

const (
	authzTenantA   = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	authzTenantSys = "13814000-1dd2-11b2-8080-808080808080"
	taSelfID       = "11111111-1111-1111-1111-111111111111"
	saSelfID       = "99999999-9999-9999-9999-999999999999"
)

func newUserAuthzDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()
	stmts := []string{
		`DROP TABLE IF EXISTS user_credentials CASCADE`,
		`DROP TABLE IF EXISTS tb_user CASCADE`,
		`CREATE TABLE tb_user (
			id uuid PRIMARY KEY, created_time bigint, email text, authority text,
			tenant_id uuid, customer_id uuid, first_name text, last_name text,
			phone text, additional_info text, version bigint)`,
		`CREATE TABLE user_credentials (
			id uuid PRIMARY KEY, created_time bigint, user_id uuid UNIQUE,
			enabled boolean, activate_token text, activate_token_exp_time bigint,
			reset_token text, reset_token_exp_time bigint, password text DEFAULT '')`,
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
		{`INSERT INTO tb_user (id, created_time, email, authority, tenant_id, version) VALUES ($1,$2,$3,'TENANT_ADMIN',$4,1)`, []any{taSelfID, now, "ta@x.org", authzTenantA}},
		{`INSERT INTO user_credentials (id, created_time, user_id, enabled, password) VALUES ($1,$2,$3,true,'x')`, []any{"cccccccc-1111-1111-1111-111111111111", now, taSelfID}},
		{`INSERT INTO tb_user (id, created_time, email, authority, tenant_id, version) VALUES ($1,$2,$3,'SYS_ADMIN',$4,1)`, []any{saSelfID, now, "sa@x.org", authzTenantSys}},
		{`INSERT INTO user_credentials (id, created_time, user_id, enabled, password) VALUES ($1,$2,$3,true,'x')`, []any{"cccccccc-9999-9999-9999-999999999999", now, saSelfID}},
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

func userAuthzJWT(t *testing.T, userID, tenantID, authority string) string {
	t.Helper()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    userID,
		Email:     "caller@x.org",
		Authority: authority,
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func doUserPost(t *testing.T, body map[string]interface{}, tok string) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/api/user", bytes.NewReader(b))
	if tok != "" {
		req.Header.Set("X-Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	HandleUserCreateOrUpdate(w, req)
	return w.Code
}

func TestUserAuthority_PrivilegeEscalation(t *testing.T) {
	newUserAuthzDB(t)

	tenantAdmin := userAuthzJWT(t, taSelfID, authzTenantA, "TENANT_ADMIN")
	sysAdmin := userAuthzJWT(t, saSelfID, authzTenantSys, "SYS_ADMIN")

	// 1. TENANT_ADMIN elevating ITSELF to SYS_ADMIN → 403.
	if code := doUserPost(t, map[string]interface{}{
		"id":        map[string]interface{}{"entityType": "USER", "id": taSelfID},
		"email":     "ta@x.org",
		"authority": "SYS_ADMIN",
	}, tenantAdmin); code != http.StatusForbidden {
		t.Errorf("tenant admin self-elevation: status = %d, want 403", code)
	}

	// 2. TENANT_ADMIN creating a NEW SYS_ADMIN → 403.
	if code := doUserPost(t, map[string]interface{}{
		"email":     "new-sys@x.org",
		"authority": "SYS_ADMIN",
	}, tenantAdmin); code != http.StatusForbidden {
		t.Errorf("tenant admin creating sys admin: status = %d, want 403", code)
	}

	// 3. SYS_ADMIN creating a SYS_ADMIN → 200.
	if code := doUserPost(t, map[string]interface{}{
		"email":     "another-sys@x.org",
		"authority": "SYS_ADMIN",
	}, sysAdmin); code != http.StatusOK {
		t.Errorf("sys admin creating sys admin: status = %d, want 200", code)
	}

	// 4. TENANT_ADMIN creating a TENANT_ADMIN in its OWN tenant → 200.
	if code := doUserPost(t, map[string]interface{}{
		"email":     "new-tenant-admin@x.org",
		"authority": "TENANT_ADMIN",
	}, tenantAdmin); code != http.StatusOK {
		t.Errorf("tenant admin creating tenant admin (own tenant): status = %d, want 200", code)
	}

	// 5. TENANT_ADMIN editing its own profile without changing authority → 200.
	if code := doUserPost(t, map[string]interface{}{
		"id":        map[string]interface{}{"entityType": "USER", "id": taSelfID},
		"email":     "ta@x.org",
		"authority": "TENANT_ADMIN",
		"firstName": "Updated",
	}, tenantAdmin); code != http.StatusOK {
		t.Errorf("tenant admin self profile edit: status = %d, want 200", code)
	}
}
