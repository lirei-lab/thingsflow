package device

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	// Force synchronous audit writes BEFORE any handler runs: the async audit
	// writer goroutine reads dbpkg.Pool, which this harness swaps per test —
	// a real data race under -race (and a use-after-close in the wild). With
	// sync mode the audit insert happens on the handler's own goroutine, so
	// every Pool access is ordered by the test itself. This must be set in
	// EVERY test of the package: audit's writer starts under a sync.Once, so
	// the first Write decides the mode for the whole test binary.
	t.Setenv("AUDIT_LOG_QUEUE_SIZE", "0")
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

const tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
const tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

func setupDeviceTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS audit_log CASCADE`,
		`DROP TABLE IF EXISTS device_credentials CASCADE`,
		`DROP TABLE IF EXISTS attribute_kv CASCADE`,
		`DROP TABLE IF EXISTS entity_alarm CASCADE`,
		`DROP TABLE IF EXISTS alarm CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS device_profile CASCADE`,
		`CREATE TABLE audit_log (
			id uuid, created_time bigint, tenant_id uuid, customer_id uuid,
			user_id uuid, user_name text, entity_id uuid, entity_type text, entity_name text,
			action_type text, action_status text, action_failure_details text, action_data text)`,
		`CREATE TABLE device_profile (
			id uuid PRIMARY KEY, tenant_id uuid, name text, is_default boolean,
			type text, transport_type text, provision_type text, provision_device_key text,
			provision_device_secret_hash text, profile_data jsonb, description text,
			image text, default_dashboard_id uuid, version bigint)`,
		`CREATE TABLE device (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text, label text,
			device_profile_id uuid, additional_info text, security_status text DEFAULT 'ACTIVE', version bigint)`,
		`CREATE TABLE device_credentials (
			id uuid PRIMARY KEY, created_time bigint, device_id uuid,
			credentials_type text, credentials_id text, credentials_value text, version bigint,
			CONSTRAINT device_credentials_device_id_unq_key UNIQUE (device_id))`,
		`CREATE TABLE attribute_kv (
			entity_id uuid, attribute_type int, attribute_key int,
			str_v text, last_update_ts bigint)`,
		`CREATE TABLE alarm (
			id uuid PRIMARY KEY, tenant_id uuid, originator_id uuid,
			type text, severity text)`,
		`CREATE TABLE entity_alarm (
			tenant_id uuid, entity_id uuid, alarm_id uuid, alarm_type text)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v\n  stmt: %s", err, s)
		}
	}
	// Default device profile for tenantA
	if _, err := db.Exec(`INSERT INTO device_profile (id, tenant_id, name, is_default, version)
		VALUES ('99999999-9999-9999-9999-999999999999', $1, 'default', true, 1)`, tenantA); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
}

// fakeJWT mints a JWT for the given tenant. Bypasses HandleLogin by
// reaching authpkg directly — tests don't need to retest login here.
func fakeJWT(t *testing.T, tenantID string) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000001",
		Email:     "test@x.org",
		Authority: "TENANT_ADMIN",
		TenantID:  tenantID,
	}, "test-session")
	if err != nil {
		t.Fatalf("mint jwt: %v", err)
	}
	return tok
}

func TestHandleDeviceCreateOrUpdate_Create(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	tok := fakeJWT(t, tenantA)

	body, _ := json.Marshal(map[string]interface{}{
		"name": "test-device-1",
		"type": "default",
	})
	req := httptest.NewRequest("POST", "/api/device", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	HandleDeviceCreateOrUpdate(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id := resp["id"].(map[string]interface{})["id"].(string)
	if id == "" {
		t.Fatal("missing id in response")
	}
	// Verify a credentials row was auto-created.
	var credCount int
	_ = db.QueryRow(`SELECT count(*) FROM device_credentials WHERE device_id = $1`, id).Scan(&credCount)
	if credCount != 1 {
		t.Errorf("device_credentials rows = %d, want 1", credCount)
	}
}

func TestHandleDeviceCreateOrUpdate_NoAuth(t *testing.T) {
	authpkg.InitConfig()
	body, _ := json.Marshal(map[string]string{"name": "x"})
	req := httptest.NewRequest("POST", "/api/device", bytes.NewReader(body))
	w := httptest.NewRecorder()
	HandleDeviceCreateOrUpdate(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestHandleDeviceSecurityStatusUpdate_SuspendsAndAuditsDevice(t *testing.T) {
	t.Setenv("AUDIT_LOG_QUEUE_SIZE", "0")
	db := newTestDB(t)
	setupDeviceTables(t, db)
	const deviceID = "aaaaaaa0-0000-0000-0000-000000000113"

	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, version)
		VALUES ($1, $2, $3, 'security-target', 'default', 1)`,
		deviceID, time.Now().UnixMilli(), tenantA); err != nil {
		t.Fatalf("seed device: %v", err)
	}

	tok := fakeJWT(t, tenantA)
	body, _ := json.Marshal(map[string]interface{}{"securityStatus": "SUSPENDED"})
	req := httptest.NewRequest("POST", "/api/device/"+deviceID+"/security", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	HandleDeviceSecurity(w, req, deviceID)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var status string
	if err := db.QueryRow(`SELECT security_status FROM device WHERE id = $1`, deviceID).Scan(&status); err != nil {
		t.Fatalf("read security_status: %v", err)
	}
	if status != "SUSPENDED" {
		t.Fatalf("security_status = %q, want SUSPENDED", status)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		var auditCount int
		_ = db.QueryRow(`SELECT count(*) FROM audit_log
			WHERE tenant_id = $1 AND entity_id = $2 AND action_type = 'SECURITY_STATUS_UPDATED'`,
			tenantA, deviceID).Scan(&auditCount)
		if auditCount == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing SECURITY_STATUS_UPDATED audit row")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandleDeviceSecurityStatusUpdate_RejectsCrossTenant(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	const deviceID = "bbbbbbb0-0000-0000-0000-000000000113"

	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, version)
		VALUES ($1, $2, $3, 'other-tenant-security-target', 'default', 1)`,
		deviceID, time.Now().UnixMilli(), tenantB); err != nil {
		t.Fatalf("seed device: %v", err)
	}

	tok := fakeJWT(t, tenantA)
	body, _ := json.Marshal(map[string]interface{}{"securityStatus": "SUSPENDED"})
	req := httptest.NewRequest("POST", "/api/device/"+deviceID+"/security", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	HandleDeviceSecurity(w, req, deviceID)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleDeviceDelete_CrossTenant(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	const targetID = "ddddddd0-0000-0000-0000-000000000001"

	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, version)
		VALUES ($1, $2, $3, 'theirs', 'default', 1)`,
		targetID, time.Now().UnixMilli(), tenantB); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tok := fakeJWT(t, tenantA) // wrong tenant
	req := httptest.NewRequest("DELETE", fmt.Sprintf("/api/device/%s", targetID), nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	HandleDeviceDelete(w, req, targetID)

	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 (cross-tenant)", w.Code)
	}
	// Device must still exist
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM device WHERE id = $1`, targetID).Scan(&n)
	if n != 1 {
		t.Errorf("device deleted across tenants! count=%d", n)
	}
}

// TestHandleDeviceCredentials_CrossTenantBlocked is the regression
// guard for P-1 in docs/SECURITY_AUDIT.md. Before the fix,
// HandleDeviceCredentials only validated the JWT shape and queried
// device_credentials by device_id alone — a tenant-A admin could
// fetch the access token of any tenant-B device. Now the SQL joins
// device → tenant_id and 404s on mismatch.
func TestHandleDeviceCredentials_CrossTenantBlocked(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	const victimID = "ddddddd0-0000-0000-0000-000000000099"
	const credID = "ddddddd0-0000-0000-0000-000000000098"

	// Seed a device + credentials in tenantB
	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, version)
		VALUES ($1, $2, $3, 'victim', 'default', 1)`,
		victimID, time.Now().UnixMilli(), tenantB); err != nil {
		t.Fatalf("seed device: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO device_credentials (id, created_time, device_id, credentials_type, credentials_id, version)
		VALUES ($1, $2, $3, 'ACCESS_TOKEN', 'STOLEN-TOKEN-VICTIM', 1)`,
		credID, time.Now().UnixMilli(), victimID); err != nil {
		t.Fatalf("seed creds: %v", err)
	}

	// Try to read with tenantA's JWT
	tok := fakeJWT(t, tenantA)
	req := httptest.NewRequest("GET", fmt.Sprintf("/api/device/%s/credentials", victimID), nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	HandleDeviceCredentials(w, req, victimID)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (cross-tenant credentials must not be readable); body=%s",
			w.Code, w.Body.String())
	}
	if bytes.Contains(w.Body.Bytes(), []byte("STOLEN-TOKEN-VICTIM")) {
		t.Error("response leaked the access token across tenants!")
	}
}

// TestHandleDeviceCredentials_HappyPath confirms same-tenant reads
// still work. Pairs with the regression test above.
func TestHandleDeviceCredentials_HappyPath(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	const ownID = "aaaaaaa0-0000-0000-0000-000000000099"
	const credID = "aaaaaaa0-0000-0000-0000-000000000098"

	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, version)
		VALUES ($1, $2, $3, 'mine', 'default', 1)`,
		ownID, time.Now().UnixMilli(), tenantA); err != nil {
		t.Fatalf("seed device: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO device_credentials (id, created_time, device_id, credentials_type, credentials_id, version)
		VALUES ($1, $2, $3, 'ACCESS_TOKEN', 'OWN-TOKEN-12345', 1)`,
		credID, time.Now().UnixMilli(), ownID); err != nil {
		t.Fatalf("seed creds: %v", err)
	}

	tok := fakeJWT(t, tenantA)
	req := httptest.NewRequest("GET", fmt.Sprintf("/api/device/%s/credentials", ownID), nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	HandleDeviceCredentials(w, req, ownID)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte("OWN-TOKEN-12345")) {
		t.Errorf("body missing expected token: %s", w.Body.String())
	}
}

func TestHandleDeviceCredentialsUpdate_RejectsWeakAccessToken(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	const deviceID = "aaaaaaa0-0000-0000-0000-000000000111"

	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, version)
		VALUES ($1, $2, $3, 'weak-token-target', 'default', 1)`,
		deviceID, time.Now().UnixMilli(), tenantA); err != nil {
		t.Fatalf("seed device: %v", err)
	}

	tok := fakeJWT(t, tenantA)
	body, _ := json.Marshal(map[string]interface{}{
		"deviceId":        map[string]interface{}{"entityType": "DEVICE", "id": deviceID},
		"credentialsType": "ACCESS_TOKEN",
		"credentialsId":   "short",
	})
	req := httptest.NewRequest("POST", "/api/device/credentials", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	HandleDeviceCredentialsUpdate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	var count int
	_ = db.QueryRow(`SELECT count(*) FROM device_credentials WHERE device_id = $1`, deviceID).Scan(&count)
	if count != 0 {
		t.Fatalf("weak credentials were persisted, count=%d", count)
	}
}

func TestHandleDeviceCredentialsUpdate_AutoGeneratesAndAuditsAccessToken(t *testing.T) {
	t.Setenv("AUDIT_LOG_QUEUE_SIZE", "0")
	db := newTestDB(t)
	setupDeviceTables(t, db)
	const deviceID = "aaaaaaa0-0000-0000-0000-000000000112"

	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, version)
		VALUES ($1, $2, $3, 'rotation-target', 'default', 1)`,
		deviceID, time.Now().UnixMilli(), tenantA); err != nil {
		t.Fatalf("seed device: %v", err)
	}

	tok := fakeJWT(t, tenantA)
	body, _ := json.Marshal(map[string]interface{}{
		"deviceId":        map[string]interface{}{"entityType": "DEVICE", "id": deviceID},
		"credentialsType": "ACCESS_TOKEN",
	})
	req := httptest.NewRequest("POST", "/api/device/credentials", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	HandleDeviceCredentialsUpdate(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var credentialsID string
	if err := db.QueryRow(`SELECT credentials_id FROM device_credentials WHERE device_id = $1`, deviceID).Scan(&credentialsID); err != nil {
		t.Fatalf("read credentials: %v", err)
	}
	if len(credentialsID) < 20 {
		t.Fatalf("auto-generated token length = %d, want >= 20", len(credentialsID))
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		var count int
		_ = db.QueryRow(`SELECT count(*) FROM audit_log
			WHERE tenant_id = $1 AND entity_id = $2 AND action_type = 'CREDENTIALS_UPDATED'`,
			tenantA, deviceID).Scan(&count)
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing CREDENTIALS_UPDATED audit row")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandleDeviceDelete_NotFound(t *testing.T) {
	newTestDB(t)
	setupDeviceTables(t, newTestDBNoSwap(t))
	tok := fakeJWT(t, tenantA)
	req := httptest.NewRequest("DELETE", "/api/device/00000000-0000-0000-0000-000000000000", nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	HandleDeviceDelete(w, req, "00000000-0000-0000-0000-000000000000")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// newTestDBNoSwap reuses the already-set pool without re-swapping.
func newTestDBNoSwap(t *testing.T) *sql.DB {
	if dbpkg.Pool == nil {
		t.Skip("pool not initialised")
	}
	return dbpkg.Pool
}

// Deleting a device used to leave its alarm rows behind: only the entity_alarm
// index was cleaned. The orphans then showed up in listings pointing at nothing,
// and the silence guard kept extending them, because telemetry outlives the
// device row and keeps naming an originator Postgres no longer has.
func TestHandleDeviceDelete_RemovesItsAlarms(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	devID := "dddddddd-dddd-dddd-dddd-ddddddddddd9"
	if _, err := db.Exec(
		`INSERT INTO device (id, tenant_id, name, type) VALUES ($1,$2,'meter','default')`,
		devID, tenantA); err != nil {
		t.Fatalf("seed device: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO alarm (id, tenant_id, originator_id, type, severity)
		 VALUES ('aaaa1111-1111-1111-1111-111111111111',$1,$2,'DeviceSilent','MAJOR')`,
		tenantA, devID); err != nil {
		t.Fatalf("seed alarm: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/device/"+devID, nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeJWT(t, tenantA))
	rec := httptest.NewRecorder()
	HandleDeviceDelete(rec, req, devID)

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM alarm WHERE originator_id = $1`, devID).Scan(&n); err != nil {
		t.Fatalf("count alarms: %v", err)
	}
	if n != 0 {
		t.Fatalf("%d alarm(s) survived the device they belong to", n)
	}
}

// The twin registry stays convergent through boot-injected hooks (main.go).
// This pins that BOTH device create paths and the delete path fire them with
// the right identity — the hook itself is a fake, so no twin_registry table
// (or internal/twin import) is needed here.
func TestDeviceCreateAndDeleteFireTwinRegistryHooks(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)
	tok := fakeJWT(t, tenantA)

	var synced, deleted []string
	TwinRegistrySync = func(tenantID, deviceID string) { synced = append(synced, tenantID+"/"+deviceID) }
	TwinRegistryDelete = func(tenantID, deviceID string) { deleted = append(deleted, tenantID+"/"+deviceID) }
	t.Cleanup(func() { TwinRegistrySync = nil; TwinRegistryDelete = nil })

	body, _ := json.Marshal(map[string]interface{}{"name": "twin-hook-device", "type": "default"})
	req := httptest.NewRequest("POST", "/api/device", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	HandleDeviceCreateOrUpdate(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d, body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	id := resp["id"].(map[string]interface{})["id"].(string)
	if len(synced) != 1 || synced[0] != tenantA+"/"+id {
		t.Fatalf("sync hook calls = %v, want exactly [%s/%s]", synced, tenantA, id)
	}

	// UPDATE path (rename) must fire the sync hook too — the registry row
	// projects the name, so a rename without a sync leaves it stale until the
	// next boot backfill.
	body, _ = json.Marshal(map[string]interface{}{
		"id":   map[string]interface{}{"entityType": "DEVICE", "id": id},
		"name": "twin-hook-device-renamed", "type": "default",
	})
	req = httptest.NewRequest("POST", "/api/device", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	HandleDeviceCreateOrUpdate(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d, body=%s", w.Code, w.Body.String())
	}
	if len(synced) != 2 || synced[1] != tenantA+"/"+id {
		t.Fatalf("sync hook calls after rename = %v, want a second %s/%s", synced, tenantA, id)
	}

	req = httptest.NewRequest("DELETE", "/api/device/"+id, nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w = httptest.NewRecorder()
	HandleDeviceDelete(w, req, id)
	if w.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body=%s", w.Code, w.Body.String())
	}
	if len(deleted) != 1 || deleted[0] != tenantA+"/"+id {
		t.Fatalf("delete hook calls = %v, want exactly [%s/%s]", deleted, tenantA, id)
	}
}

// Bulk import creates devices through its own INSERT (not the CRUD handler),
// so its hook coverage is pinned separately.
func TestBulkCreateDeviceFiresTwinRegistrySyncHook(t *testing.T) {
	db := newTestDB(t)
	setupDeviceTables(t, db)

	var synced []string
	TwinRegistrySync = func(tenantID, deviceID string) { synced = append(synced, tenantID+"/"+deviceID) }
	t.Cleanup(func() { TwinRegistrySync = nil })

	id, err := bulkCreateDevice(tenantA, "99999999-9999-9999-9999-999999999999",
		map[string]string{csvColName: "bulk-twin-hook", csvColType: "meter"})
	if err != nil {
		t.Fatalf("bulkCreateDevice: %v", err)
	}
	if len(synced) != 1 || synced[0] != tenantA+"/"+id {
		t.Fatalf("sync hook calls = %v, want exactly [%s/%s]", synced, tenantA, id)
	}
}
