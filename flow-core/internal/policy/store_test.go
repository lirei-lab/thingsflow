package policy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const (
	policyTenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	policyTenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
)

func newPolicyTestDB(t *testing.T) *sql.DB {
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
	schema := fmt.Sprintf("policy_test_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create isolated test schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})
	return db
}

func setupPolicySchema(t *testing.T, db *sql.DB) {
	t.Helper()
	statements := []string{
		`DROP TABLE IF EXISTS policy CASCADE`,
		`CREATE TABLE policy (
			tenant_id uuid NOT NULL, policy_id varchar(255) NOT NULL,
			version varchar(64) NOT NULL, kind varchar(64) NOT NULL,
			definition jsonb NOT NULL DEFAULT '{}', schema jsonb NOT NULL DEFAULT '{}',
			deprecated boolean NOT NULL DEFAULT false,
			created_time bigint NOT NULL, updated_time bigint NOT NULL,
			PRIMARY KEY (tenant_id, policy_id, version))`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("setup policy schema: %v", err)
		}
	}
}

func ownerDoc(tenantID, version string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
		"policyId": "owner",
		"version": %q,
		"subjects": ["tenant:%s"],
		"resources": ["thing:/%s", "thing:/%s/#"],
		"grants": [{"resource": "thing:/%s", "actions": ["READ", "WRITE"]}],
		"revokes": []
	}`, version, tenantID, tenantID, tenantID, tenantID))
}

func TestStoreCreateGetListDeprecateResolve(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	ctx := context.Background()
	store := NewStore(db)

	created, err := store.Create(ctx, policyTenantA, ownerDoc(policyTenantA, "1.0.0"))
	if err != nil {
		t.Fatalf("create v1: %v", err)
	}
	if created.Policy.PolicyID != "owner" || created.Policy.Version != "1.0.0" {
		t.Fatalf("unexpected created policy: %+v", created.Policy)
	}
	if created.Deprecated {
		t.Fatal("new policy must not be deprecated")
	}
	var derived DerivedPolicy
	if err := json.Unmarshal(created.Schema, &derived); err != nil {
		t.Fatalf("unmarshal derived: %v", err)
	}
	if len(derived.Subjects) != 1 || derived.Subjects[0] != "tenant:"+policyTenantA {
		t.Fatalf("unexpected derived subjects: %v", derived.Subjects)
	}
	if len(derived.Grants) != 1 {
		t.Fatalf("unexpected derived grants: %v", derived.Grants)
	}

	// Duplicate version rejected.
	if _, err := store.Create(ctx, policyTenantA, ownerDoc(policyTenantA, "1.0.0")); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	// Second version.
	if _, err := store.Create(ctx, policyTenantA, ownerDoc(policyTenantA, "2.0.0")); err != nil {
		t.Fatalf("create v2: %v", err)
	}

	// Get explicit version.
	got, err := store.Get(ctx, policyTenantA, "owner", "1.0.0")
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if got.Policy.Version != "1.0.0" {
		t.Fatalf("expected v1, got %s", got.Policy.Version)
	}

	// List latest: two versions, one row per policy_id, newest version.
	page, err := store.List(ctx, policyTenantA, ListOptions{Page: 0, PageSize: 10, Latest: true})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if page.TotalElements != 1 || len(page.Data) != 1 {
		t.Fatalf("expected 1 latest policy row, got %d elements / %d rows", page.TotalElements, len(page.Data))
	}
	if page.Data[0].Policy.Version != "2.0.0" {
		t.Fatalf("expected latest 2.0.0, got %s", page.Data[0].Policy.Version)
	}

	// List all versions (latest=false).
	all, err := store.List(ctx, policyTenantA, ListOptions{Page: 0, PageSize: 10, Latest: false})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if all.TotalElements != 2 {
		t.Fatalf("expected 2 versions, got %d", all.TotalElements)
	}

	// Deprecate v1, Resolve must then select v2.
	deprecated, err := store.Deprecate(ctx, policyTenantA, "owner", "1.0.0")
	if err != nil {
		t.Fatalf("deprecate v1: %v", err)
	}
	if !deprecated.Deprecated {
		t.Fatal("expected deprecated flag")
	}
	resolved, err := store.Resolve(ctx, policyTenantA, "tenant:"+policyTenantA+":owner")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.Version != "2.0.0" {
		t.Fatalf("expected resolved v2.0.0, got %s", resolved.Version)
	}

	// Deprecated v1 still explicitly readable.
	if _, err := store.Get(ctx, policyTenantA, "owner", "1.0.0"); err != nil {
		t.Fatalf("get deprecated v1: %v", err)
	}
}

func TestStoreResolveDefaultOwnerFallback(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	ctx := context.Background()
	store := NewStore(db)

	// No explicit rows at all: tenant:<tid>:default resolves to the built-in
	// owner-full-access policy for the owning tenant.
	resolved, err := store.Resolve(ctx, policyTenantA, "tenant:"+policyTenantA+":default")
	if err != nil {
		t.Fatalf("resolve default: %v", err)
	}
	if !resolved.Builtin {
		t.Fatal("expected built-in default policy")
	}
	if resolved.PolicyID != DefaultPolicyID {
		t.Fatalf("expected policyId default, got %s", resolved.PolicyID)
	}
	if len(resolved.Subjects) != 1 || resolved.Subjects[0] != "tenant:"+policyTenantA {
		t.Fatalf("built-in subjects: %v", resolved.Subjects)
	}
	root := "thing:/" + policyTenantA
	if actions, ok := resolved.Grants[root]; !ok || !contains(actions, "READ") || !contains(actions, "WRITE") {
		t.Fatalf("built-in grant on %s missing: %v", root, resolved.Grants)
	}

	// Bare 'default' id behaves the same.
	if _, err := store.Resolve(ctx, policyTenantA, DefaultPolicyID); err != nil {
		t.Fatalf("resolve bare default: %v", err)
	}

	// An explicit default row overrides the built-in fallback.
	explicit := json.RawMessage(fmt.Sprintf(`{
		"policyId": "default", "version": "1.0.0",
		"subjects": ["tenant:%s"], "resources": ["thing:/%s"],
		"grants": [{"resource": "thing:/%s", "actions": ["READ"]}], "revokes": []
	}`, policyTenantA, policyTenantA, policyTenantA))
	if _, err := store.Create(ctx, policyTenantA, explicit); err != nil {
		t.Fatalf("create explicit default: %v", err)
	}
	now, err := store.Resolve(ctx, policyTenantA, "tenant:"+policyTenantA+":default")
	if err != nil {
		t.Fatalf("resolve explicit default: %v", err)
	}
	if now.Builtin {
		t.Fatal("explicit default must not be flagged builtin")
	}
	if contains(now.Grants[root], "WRITE") {
		t.Fatalf("explicit default should only grant READ: %v", now.Grants)
	}
}

func TestStoreResolveFailsClosed(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	ctx := context.Background()
	store := NewStore(db)

	// Unknown policy id fails closed.
	if _, err := store.Resolve(ctx, policyTenantA, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unknown id, got %v", err)
	}
	// Foreign-tenant policyId fails closed (never leaks another tenant's doc).
	if _, err := store.Resolve(ctx, policyTenantA, "tenant:"+policyTenantB+":default"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden for foreign tenant, got %v", err)
	}
	// Malformed policyId (non-canonical) fails closed.
	if _, err := store.Resolve(ctx, policyTenantA, "UPPER"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-canonical id, got %v", err)
	}
}

func TestStoreCrossTenantIsolation(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	ctx := context.Background()
	store := NewStore(db)

	if _, err := store.Create(ctx, policyTenantA, ownerDoc(policyTenantA, "1.0.0")); err != nil {
		t.Fatalf("create tenant A: %v", err)
	}
	// Tenant B cannot read tenant A's policy.
	if _, err := store.Get(ctx, policyTenantB, "owner", "1.0.0"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for foreign-tenant get, got %v", err)
	}
	// Tenant B listing sees nothing.
	page, err := store.List(ctx, policyTenantB, ListOptions{Page: 0, PageSize: 10, Latest: true})
	if err != nil {
		t.Fatalf("list tenant B: %v", err)
	}
	if page.TotalElements != 0 {
		t.Fatalf("tenant B must not see tenant A policies, got %d", page.TotalElements)
	}
	// Tenant B resolve of tenant A's bare id fails closed.
	if _, err := store.Resolve(ctx, policyTenantB, "owner"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound resolving tenant A policy from tenant B, got %v", err)
	}
}

func TestStoreCreateRejectsInvalid(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	ctx := context.Background()
	store := NewStore(db)

	cases := []struct {
		name string
		doc  string
	}{
		{"missing policyId", `{"version": "1.0.0"}`},
		{"bad version", `{"policyId": "owner", "version": "1.0"}`},
		{"bad resource path", `{"policyId": "owner", "version": "1.0.0", "resources": ["http://evil"]}`},
		{"bad subject", `{"policyId": "owner", "version": "1.0.0", "subjects": ["group:admins"]}`},
		{"unknown action", `{"policyId": "owner", "version": "1.0.0", "grants": [{"resource": "thing:/a", "actions": ["EXECUTE"]}]}`},
		{"duplicate grant entry", `{"policyId": "owner", "version": "1.0.0", "grants": [{"resource": "thing:/a", "actions": ["READ"]}, {"resource": "thing:/a", "actions": ["WRITE"]}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.Create(ctx, policyTenantA, json.RawMessage(tc.doc)); !errors.Is(err, ErrInvalidPolicy) {
				t.Fatalf("expected ErrInvalidPolicy, got %v", err)
			}
		})
	}
	// Nothing persisted after the rejects.
	page, err := store.List(ctx, policyTenantA, ListOptions{Page: 0, PageSize: 10, Latest: false, IncludeDeprecated: true})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if page.TotalElements != 0 {
		t.Fatalf("expected no persisted rows after rejects, got %d", page.TotalElements)
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
