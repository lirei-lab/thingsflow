package main

// Production-path attribute round trip (milestone 3 phase 1 review): the UI
// writes a SHARED_SCOPE attribute through tenant.HandleAttributeRest and a
// device reads it back through the transport handler with the REAL
// fetchAttributes (postgres.go) wired via the transport.FetchAttributes seam,
// exactly as main.go wires it at boot. internal/transport's own round-trip
// test uses a local replica of the fetch SQL; this one pins the production
// wiring end to end so a drift between postgres.go and the replica cannot
// hide.

import (
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/tenant"
	"flow-core/internal/testdb"
	"flow-core/internal/transport"
)

const (
	attrTestTenantA = "11111111-1111-1111-1111-111111111111"
	attrTestTenantB = "99999999-9999-9999-9999-999999999999"
	attrTestDeviceA = "22222222-2222-2222-2222-222222222226"
	attrTestToken   = "ROUNDTRIP-TOKEN-000001"
)

// newRootAttrDB is the package-main gated harness for the attribute surface:
// the minimal schema slice HandleAttributeRest (ownership gate + attribute_kv
// writes) and the device transport (token resolve + fetchAttributes) touch,
// seeded with one tenant-A device holding ACCESS_TOKEN credentials.
func newRootAttrDB(t *testing.T) *sql.DB {
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
	// Pin the signing key so tokens minted before the handler's Extract
	// validate against the same key (same rationale as internal/tenant's
	// authz harness).
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()

	stmts := []string{
		`DROP TABLE IF EXISTS device_credentials CASCADE`,
		`DROP TABLE IF EXISTS attribute_kv CASCADE`,
		`DROP TABLE IF EXISTS twin_registry CASCADE`,
		`DROP TABLE IF EXISTS twin_model CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS key_dictionary CASCADE`,
		`CREATE TABLE device (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			name text, type text, label text, security_status text DEFAULT 'ACTIVE',
			device_profile_id uuid, additional_info text, version bigint)`,
		`CREATE TABLE device_credentials (
			id uuid PRIMARY KEY, created_time bigint, device_id uuid,
			credentials_type text, credentials_id text, credentials_value text, version bigint)`,
		`CREATE TABLE key_dictionary (key_id serial PRIMARY KEY, key text UNIQUE)`,
		`CREATE TABLE attribute_kv (
			entity_id uuid, attribute_type int, attribute_key int,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v text,
			last_update_ts bigint,
			CONSTRAINT attribute_kv_pkey PRIMARY KEY (entity_id, attribute_type, attribute_key))`,
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL,
			version varchar(64) NOT NULL, kind varchar(64) NOT NULL,
			definition jsonb NOT NULL DEFAULT '{}', schema jsonb NOT NULL DEFAULT '{}',
			deprecated boolean NOT NULL DEFAULT false,
			created_time bigint NOT NULL, updated_time bigint NOT NULL,
			PRIMARY KEY (tenant_id, model_id, version),
			CONSTRAINT twin_model_version_chk CHECK (
				version ~ '^(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})$'
				AND split_part(version, '.', 1)::numeric <= 2147483647
				AND split_part(version, '.', 2)::numeric <= 2147483647
				AND split_part(version, '.', 3)::numeric <= 2147483647))`,
		`CREATE TABLE twin_registry (
			tenant_id uuid NOT NULL, thing_id varchar(512) NOT NULL,
			entity_type varchar(255) NOT NULL, entity_id uuid NOT NULL,
			policy_id varchar(512) NOT NULL, definition varchar(512) NOT NULL,
			attributes jsonb NOT NULL DEFAULT '{}',
			model_id varchar(255), model_version varchar(64),
			created_time bigint NOT NULL, updated_time bigint NOT NULL,
			version bigint NOT NULL DEFAULT 1,
			UNIQUE (tenant_id, thing_id),
			UNIQUE (tenant_id, entity_type, entity_id),
			CHECK (entity_type IN ('DEVICE', 'ASSET')))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v\n  stmt: %s", err, s)
		}
	}
	now := time.Now().UnixMilli()
	// device_profile_id is non-NULL because resolveDeviceByToken scans it
	// into a plain string (a NULL there fails the scan and reads as 401).
	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, device_profile_id, version)
		VALUES ($1, $2, $3, 'roundtrip-device', 'default', '44444444-4444-4444-4444-444444444441', 1)`,
		attrTestDeviceA, now, attrTestTenantA); err != nil {
		t.Fatalf("seed device: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO device_credentials (id, created_time, device_id, credentials_type, credentials_id, version)
		VALUES ('33333333-3333-3333-3333-333333333331', $1, $2, 'ACCESS_TOKEN', $3, 1)`,
		now, attrTestDeviceA, attrTestToken); err != nil {
		t.Fatalf("seed credentials: %v", err)
	}
	// A migration-0014-shaped registry row with a null pin is a genuine
	// no-model entity. Enforcement must pass this row through; a missing
	// catalog table is instead an infrastructure failure and must return 500.
	if _, err := db.Exec(`INSERT INTO twin_registry
		(tenant_id,thing_id,entity_type,entity_id,policy_id,definition,attributes,created_time,updated_time,version)
		VALUES ($1,'roundtrip:device','DEVICE',$2,'roundtrip-policy','thingsflow:device:default:1.0.0','{}',$3,$3,1)`,
		attrTestTenantA, attrTestDeviceA, now); err != nil {
		t.Fatalf("seed unpinned registry row: %v", err)
	}

	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		db.Close()
	})

	// dbpkg caches key ids process-wide; after recreating key_dictionary the
	// cache may point at ids the fresh table does not hold. Materialise the
	// ids the cache answers so the dictionary and the code path agree (same
	// trick as internal/ws's tenant harness).
	for _, key := range []string{"config"} {
		id := dbpkg.GetOrInsertKeyID(key)
		if id <= 0 {
			t.Fatalf("key_dictionary seed failed for %q (id=%d)", key, id)
		}
		if _, err := db.Exec(
			`INSERT INTO key_dictionary (key_id, key) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
			id, key); err != nil {
			t.Fatalf("seed key_dictionary: %v", err)
		}
	}
	return db
}

func attrTestJWT(t *testing.T, tenantID, authority string) string {
	t.Helper()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-00000000000a",
		Email:     "u@x.org",
		Authority: authority,
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

// TestAttributeRestToDeviceTransportRoundTrip — POST a SHARED_SCOPE attribute
// through the tenant REST handler, then GET it as the device would, through
// transport.Handle with the production fetchAttributes wired on the seam.
func TestAttributeRestToDeviceTransportRoundTrip(t *testing.T) {
	newRootAttrDB(t)

	// Wire the seam exactly as main.go does at boot.
	prev := transport.FetchAttributes
	transport.FetchAttributes = fetchAttributes
	t.Cleanup(func() { transport.FetchAttributes = prev })

	tok := attrTestJWT(t, attrTestTenantA, "TENANT_ADMIN")
	req := httptest.NewRequest("POST",
		"/api/plugins/telemetry/DEVICE/"+attrTestDeviceA+"/SHARED_SCOPE",
		strings.NewReader(`{"config":"hq"}`))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	tenant.HandleAttributeRest(w, req)
	if w.Code != 200 {
		t.Fatalf("attribute POST status = %d body=%s", w.Code, w.Body.String())
	}

	devReq := httptest.NewRequest("GET", "/api/v1/"+attrTestToken+"/attributes?sharedKeys=config", nil)
	devW := httptest.NewRecorder()
	transport.Handle(devW, devReq)
	if devW.Code != 200 {
		t.Fatalf("device GET status = %d body=%s", devW.Code, devW.Body.String())
	}
	var resp struct {
		Shared map[string]interface{} `json:"shared"`
	}
	if err := json.Unmarshal(devW.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, devW.Body.String())
	}
	if resp.Shared["config"] != "hq" {
		t.Fatalf("shared.config = %#v, want \"hq\" — SHARED_SCOPE write did not round-trip through the production fetchAttributes", resp.Shared["config"])
	}
}
