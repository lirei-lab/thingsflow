package user

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

// newTestDB pulls the postgres pointed at by FLOW_TEST_PG_DSN. Tests
// skip cleanly without it so the suite is harmless on machines without
// docker. Schema is created per-test to keep them independent.
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
	// Audit writes happen asynchronously on a background goroutine.
	// Without this drain, a test that ends right after audit.Write
	// races against the next test's DROP TABLE — the goroutine sees a
	// closed pool and panics. 100ms is well above the worst-case
	// channel + INSERT latency.
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func setupUserTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS audit_log CASCADE`,
		`DROP TABLE IF EXISTS user_credentials CASCADE`,
		`DROP TABLE IF EXISTS tb_user CASCADE`,
		// Audit table minimal — audit.Write fires async on login
		// success/failure and must succeed to keep the tests stable.
		`CREATE TABLE audit_log (
			id              uuid,
			created_time    bigint,
			tenant_id       uuid,
			customer_id     uuid,
			user_id         uuid,
			user_name       text,
			entity_id       uuid,
			entity_type     text,
			entity_name     text,
			action_type     text,
			action_status   text,
			action_failure_details text,
			action_data     text
		)`,
		`CREATE TABLE tb_user (
			id              uuid PRIMARY KEY,
			created_time    bigint,
			tenant_id       uuid,
			customer_id     uuid,
			email           text,
			authority       text,
			first_name      text,
			last_name       text,
			phone           text,
			additional_info text,
			version         bigint
		)`,
		`CREATE TABLE user_credentials (
			id                       uuid PRIMARY KEY,
			created_time             bigint,
			user_id                  uuid,
			enabled                  boolean,
			password                 text,
			activate_token           text,
			activate_token_exp_time  bigint,
			reset_token              text,
			reset_token_exp_time     bigint
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
}

// seedUser inserts a user + bcrypt-hashed password and returns its uuid.
func seedUser(t *testing.T, db *sql.DB, email, password string, enabled bool) string {
	t.Helper()
	const userID = "11111111-1111-1111-1111-111111111111"
	const tenantID = "22222222-2222-2222-2222-222222222222"
	const credID = "33333333-3333-3333-3333-333333333333"
	hash, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	const nilUUID = "13814000-1dd2-11b2-8080-808080808080"
	if _, err := db.Exec(`INSERT INTO tb_user (id, created_time, tenant_id, customer_id, email, authority, version)
		VALUES ($1, $2, $3, $4, $5, 'TENANT_ADMIN', 1)`,
		userID, time.Now().UnixMilli(), tenantID, nilUUID, email); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO user_credentials (id, created_time, user_id, enabled, password)
		VALUES ($1, $2, $3, $4, $5)`,
		credID, time.Now().UnixMilli(), userID, enabled, string(hash)); err != nil {
		t.Fatalf("seed credentials: %v", err)
	}
	return userID
}

func TestHandleLogin_HappyPath(t *testing.T) {
	db := newTestDB(t)
	setupUserTables(t, db)
	authpkg.InitConfig()
	seedUser(t, db, "alice@test.org", "secret123", true)

	body, _ := json.Marshal(map[string]string{
		"username": "alice@test.org",
		"password": "secret123",
	})
	req := httptest.NewRequest("POST", "/api/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	HandleLogin(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["token"] == "" || resp["refreshToken"] == "" {
		t.Errorf("missing tokens in response: %+v", resp)
	}
}

func TestHandleLogin_BadPassword(t *testing.T) {
	db := newTestDB(t)
	setupUserTables(t, db)
	authpkg.InitConfig()
	seedUser(t, db, "bob@test.org", "correct-pw", true)

	body, _ := json.Marshal(map[string]string{
		"username": "bob@test.org",
		"password": "wrong-pw",
	})
	req := httptest.NewRequest("POST", "/api/auth/login", bytes.NewReader(body))
	w := httptest.NewRecorder()
	HandleLogin(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Invalid email or password") {
		t.Errorf("body = %s, want 'Invalid email or password'", w.Body.String())
	}
}

func TestHandleLogin_DisabledUser(t *testing.T) {
	db := newTestDB(t)
	setupUserTables(t, db)
	authpkg.InitConfig()
	seedUser(t, db, "charlie@test.org", "pw", false) // enabled=false

	body, _ := json.Marshal(map[string]string{
		"username": "charlie@test.org",
		"password": "pw",
	})
	req := httptest.NewRequest("POST", "/api/auth/login", bytes.NewReader(body))
	w := httptest.NewRecorder()
	HandleLogin(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	if !strings.Contains(w.Body.String(), "disabled") {
		t.Errorf("body = %s, want 'disabled'", w.Body.String())
	}
}

// TestHandleLogin_DisabledUserRecordsFailure — Phase 5c. The disabled-account
// branch returned 401 without touching the throttle, so it was a free
// username-enumeration oracle that never tripped the counter (and, unlike the
// other 401 paths, could be hammered forever). It must now consume the same
// budget as a bad password.
func TestHandleLogin_DisabledUserRecordsFailure(t *testing.T) {
	db := newTestDB(t)
	setupUserTables(t, db)
	authpkg.InitConfig()
	seedUser(t, db, "dana@test.org", "pw", false) // enabled=false
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "2")

	// Unique source IP: the throttle map is process-global and shared with the
	// other tests in this package.
	const peer = "198.51.100.77:41000"

	attempt := func(username, password string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"username": username, "password": password})
		req := httptest.NewRequest("POST", "/api/auth/login", bytes.NewReader(body))
		req.RemoteAddr = peer
		w := httptest.NewRecorder()
		HandleLogin(w, req)
		return w
	}

	// The throttle counts DISTINCT credentials, so probe with different ones —
	// which is also the shape of the attack this guards against. Hitting the
	// disabled-account branch has to cost budget just like any other failure;
	// otherwise it is a free oracle for telling "account exists but is
	// disabled" apart from "no such account", which does charge for it.
	for i, password := range []string{"pw", "pw-2"} {
		if w := attempt("dana@test.org", password); w.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status = %d, want 401; body=%s", i+1, w.Code, w.Body.String())
		}
	}
	w := attempt("dana@test.org", "pw-3")
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("3rd attempt: status = %d, want 429 (disabled-account failures must count); body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Too many failed login attempts") {
		t.Errorf("body = %s, want the throttle message", w.Body.String())
	}
}

// TestHandleLogin_RepeatedIdenticalCredentialIsNotBruteForce is the other side
// of the rule above. Replaying one wrong credential is a broken client, not an
// attack: it reveals nothing the first attempt did not already reveal. Charging
// it per attempt is what let a stuck edge gateway pin the shared counter and
// lock every other client out of login for ~10 hours on 2026-08-14.
func TestHandleLogin_RepeatedIdenticalCredentialIsNotBruteForce(t *testing.T) {
	db := newTestDB(t)
	setupUserTables(t, db)
	authpkg.InitConfig()
	seedUser(t, db, "stuck@test.org", "correct-pw", true)
	t.Setenv("LOGIN_THROTTLE_MAX_FAILS", "2")

	const peer = "198.51.100.88:41000"
	attempt := func(username, password string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"username": username, "password": password})
		req := httptest.NewRequest("POST", "/api/auth/login", bytes.NewReader(body))
		req.RemoteAddr = peer
		w := httptest.NewRecorder()
		HandleLogin(w, req)
		return w
	}

	for i := 1; i <= 10; i++ {
		if w := attempt("stuck@test.org", "stale-pw"); w.Code != http.StatusUnauthorized {
			t.Fatalf("replay %d: status = %d, want 401 (one credential must not exhaust the budget)", i, w.Code)
		}
	}

	// Budget intact: a different client sharing this address can still be told
	// apart, and a correct password still gets through.
	if w := attempt("stuck@test.org", "correct-pw"); w.Code != http.StatusOK {
		t.Errorf("valid login after 10 replays: status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleLogin_BadRequest(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing fields", `{"username":""}`},
		{"invalid json", `{"username":`},
		{"empty body", ``},
	}
	authpkg.InitConfig()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/auth/login", strings.NewReader(c.body))
			w := httptest.NewRecorder()
			HandleLogin(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", w.Code)
			}
		})
	}
}

func TestHandleLogin_MethodNotAllowed(t *testing.T) {
	authpkg.InitConfig()
	req := httptest.NewRequest("GET", "/api/auth/login", nil)
	w := httptest.NewRecorder()
	HandleLogin(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestHandleAuthUser_NoToken(t *testing.T) {
	authpkg.InitConfig()
	req := httptest.NewRequest("GET", "/api/auth/user", nil)
	w := httptest.NewRecorder()
	HandleAuthUser(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}
