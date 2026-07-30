package relations

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/topology"
)

const (
	tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	assetA  = "11111111-1111-1111-1111-111111111111"
	deviceA = "33333333-3333-3333-3333-333333333333"
	deviceB = "44444444-4444-4444-4444-444444444444"
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
		time.Sleep(100 * time.Millisecond)
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func setupTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS topology_edge CASCADE`,
		`DROP TABLE IF EXISTS topology_relation_type CASCADE`,
		`DROP TABLE IF EXISTS relation CASCADE`,
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`CREATE TABLE asset (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text, label text)`,
		`CREATE TABLE device (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text, label text)`,
		`CREATE TABLE relation (
			from_id uuid, from_type text, to_id uuid, to_type text,
			relation_type_group text, relation_type text,
			additional_info text, version bigint default 0,
			PRIMARY KEY (from_id, from_type, relation_type_group, relation_type, to_id, to_type))`,
		`CREATE TABLE topology_relation_type (
			name text PRIMARY KEY, description text, allowed_from_types text[],
			allowed_to_types text[], is_directed boolean not null default true,
			created_time bigint not null)`,
		`CREATE TABLE topology_edge (
			tenant_id uuid not null, from_id uuid not null, from_type text not null,
			to_id uuid not null, to_type text not null, relation_type text not null,
			relation_type_group text not null default 'COMMON',
			direction text not null default 'DIRECTED',
			metadata jsonb not null default '{}'::jsonb,
			created_time bigint not null, updated_time bigint not null,
			version bigint not null default 1,
			PRIMARY KEY (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	if err := topology.SeedRelationTypes(db); err != nil {
		t.Fatalf("seed relation types: %v", err)
	}
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO asset (id, created_time, tenant_id, name, type)
		VALUES ($1, $2, $3, 'Building A', 'building')`, assetA, now, tenantA); err != nil {
		t.Fatalf("seed asset: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type)
		VALUES ($1, $2, $3, 'Meter A', 'meter'), ($4, $2, $5, 'Meter B', 'meter')`,
		deviceA, now, tenantA, deviceB, tenantB); err != nil {
		t.Fatalf("seed device: %v", err)
	}
}

func fakeJWT(t *testing.T, tenantID string) string {
	t.Helper()
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

func TestHandlePostRelationWritesTopologyAndListReturnsClassicShape(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	tok := fakeJWT(t, tenantA)

	body, _ := json.Marshal(map[string]interface{}{
		"from":           map[string]string{"entityType": "ASSET", "id": assetA},
		"to":             map[string]string{"entityType": "DEVICE", "id": deviceA},
		"type":           "Contains",
		"typeGroup":      "COMMON",
		"additionalInfo": map[string]string{"source": "tb-ui"},
	})
	req := httptest.NewRequest("POST", "/api/relation", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	Handle(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST status=%d body=%s", w.Code, w.Body.String())
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM topology_edge WHERE metadata->>'source' = 'tb-ui'`).Scan(&n)
	if n != 1 {
		t.Fatalf("topology_edge rows=%d, want 1", n)
	}

	req = httptest.NewRequest("GET", "/api/relations?fromId="+assetA+"&fromType=ASSET", nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w = httptest.NewRecorder()
	Handle(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET status=%d body=%s", w.Code, w.Body.String())
	}
	var out []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 || out[0]["type"] != "Contains" {
		t.Fatalf("response=%s", w.Body.String())
	}
}

func TestHandlePostRelationRejectsCrossTenant(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	tok := fakeJWT(t, tenantA)

	body, _ := json.Marshal(map[string]interface{}{
		"from":      map[string]string{"entityType": "ASSET", "id": assetA},
		"to":        map[string]string{"entityType": "DEVICE", "id": deviceB},
		"type":      "Contains",
		"typeGroup": "COMMON",
	})
	req := httptest.NewRequest("POST", "/api/relation", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	Handle(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s, want 403", w.Code, w.Body.String())
	}
}
