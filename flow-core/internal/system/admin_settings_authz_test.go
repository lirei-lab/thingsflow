package system

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/testdb"
)

// Coverage for the admin-settings leak fix: the jwt/mail/security keys are
// SYS_ADMIN-only, and even a SYS_ADMIN never receives the live signing key
// (it is redacted on the way out). Non-secret keys (general) stay readable
// by any authenticated caller so the tenant UI can still bootstrap.

func newAdminSettingsDB(t *testing.T) *sql.DB {
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
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()
	stmts := []string{
		`DROP TABLE IF EXISTS admin_settings CASCADE`,
		`CREATE TABLE admin_settings (key text PRIMARY KEY, json_value text)`,
		`INSERT INTO admin_settings (key, json_value) VALUES
			('jwt', '{"tokenExpirationTime":9000,"tokenIssuer":"thingsboard.io","tokenSigningKey":"c3VwZXItc2VjcmV0LWtleQ=="}')`,
		`INSERT INTO admin_settings (key, json_value) VALUES
			('mail', '{"smtpHost":"localhost","username":"u","password":"smtp-secret"}')`,
		`INSERT INTO admin_settings (key, json_value) VALUES
			('general', '{"baseUrl":"http://localhost:8080"}')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema/seed: %v", err)
		}
	}
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		db.Close()
	})
	return db
}

func settingsJWT(t *testing.T, authority string) string {
	t.Helper()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000009",
		Email:     "caller@x.org",
		Authority: authority,
		TenantID:  "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func getSetting(t *testing.T, key, tok string) (int, string) {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/admin/settings/"+key+"?key="+key, nil)
	if tok != "" {
		req.Header.Set("X-Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	HandleAdminSettings(w, req)
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestAdminSettings_SysAdminOnlyAndRedacted(t *testing.T) {
	newAdminSettingsDB(t)
	tenant := settingsJWT(t, "TENANT_ADMIN")
	sys := settingsJWT(t, "SYS_ADMIN")

	// Non-sysadmin reading a system-scope key → 403.
	if code, _ := getSetting(t, "jwt", tenant); code != http.StatusForbidden {
		t.Errorf("tenant GET jwt: status = %d, want 403", code)
	}
	// Anonymous → 401.
	if code, _ := getSetting(t, "jwt", ""); code != http.StatusUnauthorized {
		t.Errorf("anon GET jwt: status = %d, want 401", code)
	}

	// SYS_ADMIN reading jwt → 200 but tokenSigningKey redacted.
	code, body := getSetting(t, "jwt", sys)
	if code != http.StatusOK {
		t.Fatalf("sysadmin GET jwt: status = %d, want 200", code)
	}
	var jwtCfg map[string]interface{}
	if err := json.Unmarshal([]byte(body), &jwtCfg); err != nil {
		t.Fatalf("jwt body not json: %v (%s)", err, body)
	}
	if sk, _ := jwtCfg["tokenSigningKey"].(string); sk != "" {
		t.Errorf("tokenSigningKey not redacted: got %q", sk)
	}
	if iss, _ := jwtCfg["tokenIssuer"].(string); iss != "thingsboard.io" {
		t.Errorf("non-secret field lost: tokenIssuer = %q", iss)
	}

	// SYS_ADMIN reading mail → password redacted.
	_, mailBody := getSetting(t, "mail", sys)
	var mailCfg map[string]interface{}
	_ = json.Unmarshal([]byte(mailBody), &mailCfg)
	if pw, _ := mailCfg["password"].(string); pw != "" {
		t.Errorf("mail password not redacted: got %q", pw)
	}

	// Non-secret key still readable by a tenant (contract: baseUrl).
	code, genBody := getSetting(t, "general", tenant)
	if code != http.StatusOK {
		t.Errorf("tenant GET general: status = %d, want 200", code)
	}
	var genCfg map[string]interface{}
	_ = json.Unmarshal([]byte(genBody), &genCfg)
	if bu, _ := genCfg["baseUrl"].(string); bu != "http://localhost:8080" {
		t.Errorf("general.baseUrl lost: got %q", bu)
	}
}
