package transport

import (
	"database/sql"
	"os"
	"testing"
	"time"

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
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func setupTransportTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS device_credentials CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`CREATE TABLE device (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			name text, type text, device_profile_id uuid, security_status text DEFAULT 'ACTIVE', version bigint)`,
		`CREATE TABLE device_credentials (
			id uuid PRIMARY KEY, created_time bigint, device_id uuid,
			credentials_type text, credentials_id text, credentials_value text, version bigint)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
}

func seedDeviceWithToken(t *testing.T, db *sql.DB, tenantID, deviceID, profileID, accessToken string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, device_profile_id, version)
		VALUES ($1, $2, $3, 'transport-test', 'default', $4, 1)`,
		deviceID, time.Now().UnixMilli(), tenantID, profileID); err != nil {
		t.Fatalf("seed device: %v", err)
	}
	credID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	if _, err := db.Exec(`INSERT INTO device_credentials (id, created_time, device_id, credentials_type, credentials_id, version)
		VALUES ($1, $2, $3, 'ACCESS_TOKEN', $4, 1)`,
		credID, time.Now().UnixMilli(), deviceID, accessToken); err != nil {
		t.Fatalf("seed credentials: %v", err)
	}
}

func TestResolveDeviceByTokenDeniesSuspendedDevice(t *testing.T) {
	db := newTestDB(t)
	setupTransportTables(t, db)
	const tenantID = "11111111-1111-1111-1111-111111111111"
	const deviceID = "22222222-2222-2222-2222-222222222224"
	const profileID = "33333333-3333-3333-3333-333333333333"
	seedDeviceWithToken(t, db, tenantID, deviceID, profileID, "HttpSuspendedToken123")
	if _, err := db.Exec(`UPDATE device SET security_status = 'SUSPENDED' WHERE id = $1`, deviceID); err != nil {
		t.Fatalf("suspend device: %v", err)
	}

	_, _, _, ok := resolveDeviceByToken("HttpSuspendedToken123")
	if ok {
		t.Fatal("resolveDeviceByToken accepted a suspended device")
	}
}

func TestGetEnvDefault(t *testing.T) {
	t.Setenv("FOO_TEST", "bar")
	if got := getEnvDefault("FOO_TEST", "fb"); got != "bar" {
		t.Errorf("got %q, want bar", got)
	}
	if got := getEnvDefault("DEFINITELY_UNSET_KEY", "fb"); got != "fb" {
		t.Errorf("got %q, want fb", got)
	}
}
