package entityquery

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): resolveEntity silently returned nil
// for CUSTOMER/USER/ENTITY_VIEW, and the entityName / entityViewType /
// *SearchQuery / stateEntityOwner filter types fell through to the default
// case — both paths answered an empty 200 with only a server-side WARN, even
// though every table and traversal primitive they need already existed.
//
// Uses the same throwaway-schema harness as entityquery_relations_test.go
// (CREATE SCHEMA ... / DROP SCHEMA CASCADE), so it never touches real tables.

const (
	lfTenantA   = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	lfAssetRoot = "11111111-1111-1111-1111-111111111111"
	lfDevice    = "33333333-3333-3333-3333-333333333333"
	lfCustomer  = "55555555-5555-5555-5555-555555555555"
	lfUser      = "66666666-6666-6666-6666-666666666666"
	lfView      = "77777777-7777-7777-7777-777777777777"
)

func newLegacyFiltersDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	schema := fmt.Sprintf("legacy_filters_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	for _, statement := range []string{
		`CREATE TABLE asset (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, name text, type text, label text, customer_id uuid)`,
		`CREATE TABLE device (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, name text, type text, label text, customer_id uuid)`,
		`CREATE TABLE customer (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, title text)`,
		`CREATE TABLE tb_user (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, email text, first_name text, last_name text)`,
		`CREATE TABLE entity_view (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, name text, type text, customer_id uuid)`,
		`CREATE TABLE dashboard (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, title text)`,
		`CREATE TABLE tenant (id uuid PRIMARY KEY, created_time bigint, title text)`,
		// topology.NeighborsTenant's union covers every entity table it can
		// traverse, so the profile tables must exist even though nothing
		// here seeds them.
		`CREATE TABLE device_profile (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE asset_profile (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE topology_edge (
			tenant_id uuid not null, from_id uuid not null, from_type text not null,
			to_id uuid not null, to_type text not null, relation_type text not null,
			relation_type_group text not null default 'COMMON', direction text not null default 'DIRECTED',
			metadata jsonb not null default '{}'::jsonb, created_time bigint not null,
			updated_time bigint not null, version bigint not null default 1,
			PRIMARY KEY (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type, direction),
			CHECK (direction IN ('DIRECTED', 'BIDIRECTIONAL')))`,
		`CREATE TABLE relation (
			from_id uuid, from_type text, to_id uuid, to_type text,
			relation_type_group text, relation_type text,
			additional_info text, version bigint default 0,
			PRIMARY KEY (from_id, from_type, relation_type_group, relation_type, to_id, to_type))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("schema statement: %v\n%s", err, statement)
		}
	}
	seeds := []struct {
		query string
		args  []interface{}
	}{
		{`INSERT INTO tenant (id,created_time,title) VALUES ($1,10,'Tenant A')`, []interface{}{lfTenantA}},
		{`INSERT INTO customer (id,created_time,tenant_id,title) VALUES ($1,20,$2,'Acme Corp')`, []interface{}{lfCustomer, lfTenantA}},
		{`INSERT INTO tb_user (id,created_time,tenant_id,email,first_name,last_name) VALUES ($1,30,$2,'jane@acme.org','Jane','Roe')`, []interface{}{lfUser, lfTenantA}},
		{`INSERT INTO entity_view (id,created_time,tenant_id,name,type) VALUES ($1,40,$2,'Meter View','energy')`, []interface{}{lfView, lfTenantA}},
		{`INSERT INTO asset (id,created_time,tenant_id,name,type,label) VALUES ($1,50,$2,'Building A','building','HQ')`, []interface{}{lfAssetRoot, lfTenantA}},
		// The device is owned by the customer — stateEntityOwner must resolve to it.
		{`INSERT INTO device (id,created_time,tenant_id,name,type,label,customer_id) VALUES ($1,60,$2,'Meter A','meter','',$3)`, []interface{}{lfDevice, lfTenantA, lfCustomer}},
		{`INSERT INTO topology_edge (tenant_id,from_id,from_type,to_id,to_type,relation_type,relation_type_group,direction,metadata,created_time,updated_time)
		  VALUES ($1,$2,'ASSET',$3,'DEVICE','Contains','COMMON','DIRECTED','{}',1,1)`, []interface{}{lfTenantA, lfAssetRoot, lfDevice}},
	}
	for _, s := range seeds {
		if _, err := db.Exec(s.query, s.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, s.query)
		}
	}

	dbpkg.SetPoolForTest(t, db)
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})
	return db
}

// resolveEntity used to answer nil for these three types, so any
// singleEntity/entityList filter naming one was silently dropped.
func TestResolveEntity_CustomerUserEntityView(t *testing.T) {
	newLegacyFiltersDB(t)

	t.Run("CUSTOMER", func(t *testing.T) {
		got := resolveEntity(lfTenantA, "CUSTOMER", lfCustomer)
		if got == nil {
			t.Fatal("got nil (a prior version dropped CUSTOMER entirely)")
		}
		if got["name"] != "Acme Corp" {
			t.Errorf("name: got %v, want %q", got["name"], "Acme Corp")
		}
	})

	t.Run("USER resolves full name over email", func(t *testing.T) {
		got := resolveEntity(lfTenantA, "USER", lfUser)
		if got == nil {
			t.Fatal("got nil (a prior version dropped USER entirely)")
		}
		if got["name"] != "Jane Roe" {
			t.Errorf("name: got %v, want %q", got["name"], "Jane Roe")
		}
		if got["email"] != "jane@acme.org" {
			t.Errorf("email: got %v, want %q", got["email"], "jane@acme.org")
		}
	})

	t.Run("ENTITY_VIEW", func(t *testing.T) {
		got := resolveEntity(lfTenantA, "ENTITY_VIEW", lfView)
		if got == nil {
			t.Fatal("got nil (a prior version dropped ENTITY_VIEW entirely)")
		}
		if got["name"] != "Meter View" || got["type"] != "energy" {
			t.Errorf("got %v, want name=Meter View type=energy", got)
		}
	})

	t.Run("cross-tenant is still refused", func(t *testing.T) {
		if got := resolveEntity("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "CUSTOMER", lfCustomer); got != nil {
			t.Errorf("a foreign tenant resolved another tenant's customer: %v", got)
		}
	})
}

func TestLegacyFilterTypes(t *testing.T) {
	newLegacyFiltersDB(t)

	t.Run("entityName prefix search", func(t *testing.T) {
		got := handleEntityNameFilter(lfTenantA,
			map[string]interface{}{"entityType": "DEVICE", "entityNameFilter": "Meter"}, nil)
		if len(got) != 1 || got[0]["name"] != "Meter A" {
			t.Fatalf("got %v, want the one device named Meter A (a prior version returned nothing)", got)
		}
	})

	t.Run("entityName refuses an unsupported type rather than guessing", func(t *testing.T) {
		if got := handleEntityNameFilter(lfTenantA,
			map[string]interface{}{"entityType": "NOT_A_TABLE", "entityNameFilter": "x"}, nil); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("entityViewType", func(t *testing.T) {
		got := handleEntityViewTypeFilter(lfTenantA,
			map[string]interface{}{"entityViewType": "energy"}, nil)
		if len(got) != 1 || got[0]["name"] != "Meter View" {
			t.Fatalf("got %v, want the one energy entity view (a prior version returned nothing)", got)
		}
	})

	t.Run("deviceSearchQuery walks relations from the root", func(t *testing.T) {
		got := handleEntitySearchQueryFilter(lfTenantA, "deviceSearchQuery", map[string]interface{}{
			"rootEntity":   map[string]interface{}{"entityType": "ASSET", "id": lfAssetRoot},
			"relationType": "Contains",
		})
		if len(got) != 1 || got[0]["name"] != "Meter A" {
			t.Fatalf("got %v, want the device related to the root asset (a prior version returned nothing)", got)
		}
	})

	t.Run("deviceSearchQuery narrows by subtype", func(t *testing.T) {
		miss := handleEntitySearchQueryFilter(lfTenantA, "deviceSearchQuery", map[string]interface{}{
			"rootEntity":   map[string]interface{}{"entityType": "ASSET", "id": lfAssetRoot},
			"relationType": "Contains",
			"deviceTypes":  []interface{}{"not-a-meter"},
		})
		if len(miss) != 0 {
			t.Errorf("subtype narrowing ignored: got %v", miss)
		}
		hit := handleEntitySearchQueryFilter(lfTenantA, "deviceSearchQuery", map[string]interface{}{
			"rootEntity":   map[string]interface{}{"entityType": "ASSET", "id": lfAssetRoot},
			"relationType": "Contains",
			"deviceTypes":  []interface{}{"meter"},
		})
		if len(hit) != 1 {
			t.Errorf("matching subtype dropped: got %v", hit)
		}
	})

	t.Run("deviceSearchQuery refuses a foreign-tenant root", func(t *testing.T) {
		got := handleEntitySearchQueryFilter("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "deviceSearchQuery",
			map[string]interface{}{
				"rootEntity":   map[string]interface{}{"entityType": "ASSET", "id": lfAssetRoot},
				"relationType": "Contains",
			})
		if len(got) != 0 {
			t.Errorf("traversed another tenant's root: %v", got)
		}
	})

	t.Run("stateEntityOwner resolves the owning customer", func(t *testing.T) {
		got := handleStateEntityOwnerFilter(lfTenantA, map[string]interface{}{
			"singleEntity": map[string]interface{}{"entityType": "DEVICE", "id": lfDevice},
		})
		if len(got) != 1 || got[0]["entityType"] != "CUSTOMER" || got[0]["name"] != "Acme Corp" {
			t.Fatalf("got %v, want the owning customer (a prior version returned nothing)", got)
		}
	})

	t.Run("stateEntityOwner falls back to the tenant when unassigned", func(t *testing.T) {
		got := handleStateEntityOwnerFilter(lfTenantA, map[string]interface{}{
			"singleEntity": map[string]interface{}{"entityType": "ASSET", "id": lfAssetRoot},
		})
		if len(got) != 1 || got[0]["entityType"] != "TENANT" {
			t.Fatalf("got %v, want the tenant as owner", got)
		}
	})
}
