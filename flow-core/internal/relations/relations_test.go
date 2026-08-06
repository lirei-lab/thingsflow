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
		`DROP TABLE IF EXISTS twin_registry CASCADE`,
		`DROP TABLE IF EXISTS twin_model CASCADE`,
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
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL,
			version varchar(64) NOT NULL, kind varchar(64) NOT NULL,
			definition jsonb NOT NULL DEFAULT '{}', schema jsonb NOT NULL DEFAULT '{}',
			deprecated boolean NOT NULL DEFAULT false,
			created_time bigint NOT NULL, updated_time bigint NOT NULL,
			PRIMARY KEY (tenant_id, model_id, version))`,
		`CREATE TABLE twin_registry (
			tenant_id uuid NOT NULL, thing_id varchar(512) NOT NULL,
			entity_type varchar(255) NOT NULL, entity_id uuid NOT NULL,
			policy_id varchar(512) NOT NULL, definition varchar(512) NOT NULL,
			attributes jsonb NOT NULL DEFAULT '{}', model_id varchar(255), model_version varchar(64),
			created_time bigint NOT NULL, updated_time bigint NOT NULL, version bigint NOT NULL DEFAULT 1,
			UNIQUE (tenant_id, thing_id), UNIQUE (tenant_id, entity_type, entity_id))`,
		`CREATE TABLE topology_edge (
			tenant_id uuid not null, from_id uuid not null, from_type text not null,
			to_id uuid not null, to_type text not null, relation_type text not null,
			relation_type_group text not null default 'COMMON',
			direction text not null default 'DIRECTED',
			metadata jsonb not null default '{}'::jsonb,
			created_time bigint not null, updated_time bigint not null,
			version bigint not null default 1,
			PRIMARY KEY (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type, direction),
			CHECK (direction IN ('DIRECTED', 'BIDIRECTIONAL')))`,
		`CREATE UNIQUE INDEX topology_edge_bidirectional_unq
			ON topology_edge (tenant_id, relation_type_group, relation_type,
			LEAST(from_type||':'||from_id::text,to_type||':'||to_id::text),
			GREATEST(from_type||':'||from_id::text,to_type||':'||to_id::text))
			WHERE direction='BIDIRECTIONAL'`,
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

func TestHandlePostRelationMapsModelNarrowingRejectToBadRequest(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	t.Setenv("TWIN_MODEL_RELATION_ENFORCE", "reject")
	now := time.Now().UnixMilli()
	for _, model := range []struct {
		id, kind, entityID string
	}{
		{"building", "ASSET", assetA},
		{"meter", "DEVICE", deviceA},
	} {
		schema := `{"modelId":"` + model.id + `","version":"1.0.0","kind":"` + model.kind + `","unknownKeys":"allow","enforcementMode":"warn","attributes":{},"features":{},"relationships":{}}`
		if _, err := db.Exec(`INSERT INTO twin_model
			(tenant_id,model_id,version,kind,definition,schema,created_time,updated_time)
			VALUES ($1,$2,'1.0.0',$3,$4::jsonb,$4::jsonb,$5,$5)`, tenantA, model.id, model.kind, schema, now); err != nil {
			t.Fatalf("seed model %s: %v", model.id, err)
		}
		if _, err := db.Exec(`INSERT INTO twin_registry
			(tenant_id,thing_id,entity_type,entity_id,policy_id,definition,attributes,model_id,model_version,created_time,updated_time)
			VALUES ($1::uuid,$1::uuid::text||':'||lower($2::text)||':'||$3::uuid::text,
			$2::varchar,$3::uuid,'p','d','{}',$4::varchar,'1.0.0',$5,$5)`, tenantA, model.kind, model.entityID, model.id, now); err != nil {
			t.Fatalf("pin model %s: %v", model.id, err)
		}
	}

	body, _ := json.Marshal(map[string]interface{}{
		"from": map[string]string{"entityType": "ASSET", "id": assetA},
		"to":   map[string]string{"entityType": "DEVICE", "id": deviceA},
		"type": "Contains", "typeGroup": "COMMON",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/relation", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+fakeJWT(t, tenantA))
	w := httptest.NewRecorder()
	Handle(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s, want typed narrowing mapped to 400", w.Code, w.Body.String())
	}
}
