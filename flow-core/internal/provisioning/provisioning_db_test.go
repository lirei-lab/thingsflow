package provisioning

import (
	"database/sql"
	"os"
	"testing"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/testdb"
)

const provTestTenant = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
const provTestProfile = "99999999-9999-9999-9999-999999999999"

func newProvisioningTestDB(t *testing.T) *sql.DB {
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
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func setupProvisioningTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS device_credentials CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS device_profile CASCADE`,
		`CREATE TABLE device_profile (
			id uuid PRIMARY KEY, tenant_id uuid, name text, is_default boolean,
			type text, transport_type text, provision_type text, provision_device_key text,
			provision_device_secret_hash text, profile_data jsonb, description text, version bigint)`,
		`CREATE TABLE device (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text, label text,
			device_profile_id uuid, additional_info text, version bigint)`,
		`CREATE TABLE device_credentials (
			id uuid PRIMARY KEY, created_time bigint, device_id uuid,
			credentials_type text, credentials_id text, credentials_value text, version bigint,
			CONSTRAINT provisioning_device_credentials_device_id_unq UNIQUE (device_id))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v\n  stmt: %s", err, s)
		}
	}
}

// Provisioning is the real production onboarding path: a device created here
// must fire the twin registry sync hook exactly like a UI-created one — and
// re-provisioning an EXISTING device (created=false) must not fire it again.
func TestProvisionDeviceFiresTwinRegistrySyncHookOnlyOnCreate(t *testing.T) {
	db := newProvisioningTestDB(t)
	setupProvisioningTables(t, db)

	hash, err := hashProvisionSecret("secret-a")
	if err != nil {
		t.Fatalf("hash secret: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO device_profile
		(id, tenant_id, name, is_default, provision_type, provision_device_key, provision_device_secret_hash, version)
		VALUES ($1, $2, 'default', true, $3, 'fleet-a', $4, 1)`,
		provTestProfile, provTestTenant, provisionAllowCreateNew, hash); err != nil {
		t.Fatalf("seed profile: %v", err)
	}

	var synced []string
	TwinRegistrySync = func(tenantID, deviceID string) { synced = append(synced, tenantID+"/"+deviceID) }
	t.Cleanup(func() { TwinRegistrySync = nil })

	req := normalizedProvisionRequest{
		DeviceName:            "pump-001",
		DeviceType:            "pump",
		ProvisionDeviceKey:    "fleet-a",
		ProvisionDeviceSecret: "secret-a",
	}
	_, deviceID, tenantID, created, ok := provisionDevice(req)
	if !ok || !created {
		t.Fatalf("provision: ok=%v created=%v", ok, created)
	}
	if tenantID != provTestTenant {
		t.Fatalf("tenantID=%s", tenantID)
	}
	if len(synced) != 1 || synced[0] != provTestTenant+"/"+deviceID {
		t.Fatalf("sync hook calls = %v, want exactly [%s/%s]", synced, provTestTenant, deviceID)
	}

	// Same request again resolves the existing device: no second hook call.
	_, deviceID2, _, created2, ok2 := provisionDevice(req)
	if !ok2 || created2 {
		t.Fatalf("re-provision: ok=%v created=%v, want ok and NOT created", ok2, created2)
	}
	if deviceID2 != deviceID {
		t.Fatalf("re-provision deviceID=%s, want %s", deviceID2, deviceID)
	}
	if len(synced) != 1 {
		t.Fatalf("sync hook fired %d times, want 1 (existing device must not re-sync)", len(synced))
	}
}
