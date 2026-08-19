package system

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	dbpkg "flow-core/internal/db"
)

// Regression coverage for the ui-contract data-fidelity audit
// (docs/UI_CONTRACT_DATA_FIDELITY.md P3): GET /api/noauth/userPasswordPolicy
// returned fixed TB defaults unconditionally, so a policy a SYS_ADMIN had
// actually saved into the securitySettings admin_settings row was never
// shown to the login screen.
func TestUserPasswordPolicy_ReadsStoredSettings(t *testing.T) {
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	schema := fmt.Sprintf("pwpolicy_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE admin_settings (key text PRIMARY KEY, json_value text)`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})

	get := func() map[string]interface{} {
		req := httptest.NewRequest(http.MethodGet, "/api/noauth/userPasswordPolicy", nil)
		rec := httptest.NewRecorder()
		HandleUserPasswordPolicy(rec, req)
		var out map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out
	}

	t.Run("defaults when unconfigured", func(t *testing.T) {
		if got := get(); got["minimumLength"] != float64(6) {
			t.Errorf("minimumLength: got %v, want the default 6", got["minimumLength"])
		}
	})

	// The lockout fields alongside passwordPolicy are SYS_ADMIN-scoped in
	// HandleAdminSettings; this endpoint is public, so they must not leak.
	if _, err := db.Exec(`INSERT INTO admin_settings (key, json_value) VALUES ('securitySettings', $1)`,
		`{"maxFailedLoginAttempts":5,"userLockoutNotificationEmail":"secops@example.org",
		  "passwordPolicy":{"minimumLength":12,"minimumDigits":2,"allowWhitespaces":false}}`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	t.Run("reflects a stored policy", func(t *testing.T) {
		got := get()
		if got["minimumLength"] != float64(12) {
			t.Errorf("minimumLength: got %v, want 12 (a prior version always said 6)", got["minimumLength"])
		}
		if got["minimumDigits"] != float64(2) {
			t.Errorf("minimumDigits: got %v, want 2", got["minimumDigits"])
		}
		if got["allowWhitespaces"] != false {
			t.Errorf("allowWhitespaces: got %v, want false", got["allowWhitespaces"])
		}
	})

	t.Run("keeps defaults for fields the stored policy omits", func(t *testing.T) {
		if got := get(); got["maximumLength"] != float64(72) {
			t.Errorf("maximumLength: got %v, want the default 72", got["maximumLength"])
		}
	})

	t.Run("does not leak the rest of securitySettings", func(t *testing.T) {
		got := get()
		for _, leaked := range []string{"maxFailedLoginAttempts", "userLockoutNotificationEmail"} {
			if _, present := got[leaked]; present {
				t.Errorf("%s leaked from a SYS_ADMIN-scoped setting via this public endpoint", leaked)
			}
		}
	})
}
