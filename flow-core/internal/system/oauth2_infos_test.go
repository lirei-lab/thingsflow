package system

import (
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

// fakeSystemJWT mints a TENANT_ADMIN token for tenantID, shared across this
// package's ui-contract data-fidelity regression tests (docs/adr/0002).
func fakeSystemJWT(t *testing.T, tenantID string) string {
	t.Helper()
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
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

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): both GET /api/oauth2/client/infos and
// GET /api/oauth2/config/template hardcoded [] unconditionally even though
// real, populated tables (oauth2_client, oauth2_client_registration_template)
// already exist and are read elsewhere in this same package.
func TestOAuth2ClientInfos_ReadsRealTable(t *testing.T) {
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

	stmts := []string{
		`DROP TABLE IF EXISTS oauth2_client CASCADE`,
		`CREATE TABLE oauth2_client (id uuid PRIMARY KEY, created_time bigint, title text, login_button_label text, additional_info text)`,
		`INSERT INTO oauth2_client (id, created_time, title, login_button_label)
		 VALUES (gen_random_uuid(), 1000, 'Google Workspace', 'Sign in with Google')`,
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

	req := httptest.NewRequest(http.MethodGet, "/api/oauth2/client/infos", nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeSystemJWT(t, "11111111-1111-1111-1111-111111111111"))
	rec := httptest.NewRecorder()
	HandleOAuth2ClientInfos(rec, req)

	var got []map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1 (a prior version always returned []) — body=%s", len(got), rec.Body.String())
	}
	if got[0]["title"] != "Google Workspace" {
		t.Errorf("title: got %v, want %q", got[0]["title"], "Google Workspace")
	}
}

func TestOAuth2ConfigTemplate_ReadsRealTable(t *testing.T) {
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

	stmts := []string{
		`DROP TABLE IF EXISTS oauth2_client_registration_template CASCADE`,
		`CREATE TABLE oauth2_client_registration_template (
			id uuid PRIMARY KEY, created_time bigint, provider_id text UNIQUE,
			authorization_uri text, token_uri text, scope text, user_info_uri text,
			user_name_attribute_name text, jwk_set_uri text,
			client_authentication_method text, type text,
			basic_email_attribute_key text, basic_first_name_attribute_key text,
			basic_last_name_attribute_key text, basic_tenant_name_strategy text,
			comment text, login_button_icon text, login_button_label text, help_link text)`,
		`INSERT INTO oauth2_client_registration_template (id, created_time, provider_id, scope, login_button_label)
		 VALUES (gen_random_uuid(), 1000, 'google', 'email,profile', 'Sign in with Google')`,
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

	req := httptest.NewRequest(http.MethodGet, "/api/oauth2/config/template", nil)
	rec := httptest.NewRecorder()
	HandleOAuth2ConfigTemplate(rec, req)

	var got []map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1 (a prior version always returned []) — body=%s", len(got), rec.Body.String())
	}
	if got[0]["providerId"] != "google" {
		t.Errorf("providerId: got %v, want %q", got[0]["providerId"], "google")
	}
	scope, _ := got[0]["scope"].([]interface{})
	if len(scope) != 2 {
		t.Errorf("scope: got %v, want 2 entries", got[0]["scope"])
	}
}
