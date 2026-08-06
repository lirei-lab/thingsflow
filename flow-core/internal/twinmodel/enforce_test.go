package twinmodel

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const (
	enforceTenantA = "aaaaaaaa-0000-0000-0000-000000000001"
	enforceTenantB = "bbbbbbbb-0000-0000-0000-000000000002"
	enforceDevice  = "dddddddd-0000-0000-0000-000000000001"
	enforceAsset   = "aaaaaaaa-0000-0000-0000-000000000003"
)

func newEnforceTestDB(t *testing.T) *sql.DB {
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
	schema := fmt.Sprintf("twinmodel_enforce_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})
	for _, statement := range []string{
		`CREATE TABLE twin_registry (
			tenant_id uuid NOT NULL, entity_type varchar(255) NOT NULL, entity_id uuid NOT NULL,
			model_id varchar(255), model_version varchar(64),
			UNIQUE (tenant_id, entity_type, entity_id))`,
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL, version varchar(64) NOT NULL,
			kind varchar(64) NOT NULL, schema jsonb NOT NULL,
			PRIMARY KEY (tenant_id, model_id, version))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("schema statement: %v\n%s", err, statement)
		}
	}
	return db
}

func enforceSchema(modelID, version, kind, mode string) string {
	return fmt.Sprintf(`{
		"modelId":%q,"version":%q,"kind":%q,"unknownKeys":"allow","enforcementMode":%q,
		"attributes":{
			"temperature":{"type":"number","maximum":10},
			"label":{"type":"string","minLength":3}
		},"features":{},"relationships":{}}`, modelID, version, kind, mode)
}

func seedEnforcementPin(t *testing.T, db *sql.DB, tenantID, entityType, entityID, modelID, version, schema string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO twin_model (tenant_id,model_id,version,kind,schema)
		VALUES ($1,$2,$3,$4,$5::jsonb)`, tenantID, modelID, version, entityType, schema); err != nil {
		t.Fatalf("seed model: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO twin_registry (tenant_id,entity_type,entity_id,model_id,model_version)
		VALUES ($1,$2,$3,$4,$5)`, tenantID, entityType, entityID, modelID, version); err != nil {
		t.Fatalf("seed pin: %v", err)
	}
}

func TestValidateAttributesEnforceWarnRejectAndExactPointers(t *testing.T) {
	db := newEnforceTestDB(t)
	seedEnforcementPin(t, db, enforceTenantA, "DEVICE", enforceDevice, "meter", "1.0.0",
		enforceSchema("meter", "1.0.0", "DEVICE", "warn"))
	seedEnforcementPin(t, db, enforceTenantA, "ASSET", enforceAsset, "building", "1.0.0",
		enforceSchema("building", "1.0.0", "ASSET", "reject"))

	var logs bytes.Buffer
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(&logs)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	before := attributeViolationCounter.Load()
	err := ValidateAttributes(context.Background(), db, enforceTenantA, "device", enforceDevice,
		map[string]interface{}{"temperature": float64(11), "label": "x"})
	if err != nil {
		t.Fatalf("warn validation returned error: %v", err)
	}
	if delta := attributeViolationCounter.Load() - before; delta != 1 {
		t.Fatalf("warn counter delta=%d, want exactly 1 for a two-violation write", delta)
	}
	line := logs.String()
	for _, field := range []string{
		"tenant_id=" + enforceTenantA, "entity_type=DEVICE", "entity_id=" + enforceDevice,
		"model_id=meter", "model_version=1.0.0", "violation_count=2", "enforcement_mode=warn",
	} {
		if !strings.Contains(line, field) {
			t.Errorf("warn log missing %q: %s", field, line)
		}
	}
	if err := ValidateAttributes(context.Background(), db, enforceTenantA, "ASSET", enforceAsset,
		map[string]interface{}{"temperature": nil, "label": nil}); err != nil {
		t.Fatalf("defensive null write should be accepted: %v", err)
	}

	err = ValidateAttributes(context.Background(), db, enforceTenantA, "ASSET", enforceAsset,
		map[string]interface{}{"temperature": float64(11), "label": "x"})
	var rejected *AttributeValidationError
	if !errors.As(err, &rejected) || !errors.Is(err, ErrAttributesRejected) {
		t.Fatalf("reject error=%T %v, want typed AttributeValidationError", err, err)
	}
	if rejected.ModelID != "building" || rejected.Version != "1.0.0" || len(rejected.Violations) != 2 {
		t.Fatalf("reject details=%+v", rejected)
	}
	if got := []string{rejected.Violations[0].Pointer, rejected.Violations[1].Pointer}; got[0] != "attributes/label" || got[1] != "attributes/temperature" {
		t.Fatalf("reject pointers=%v, want deterministic attribute pointers", got)
	}
	if delta := attributeViolationCounter.Load() - before; delta != 1 {
		t.Fatalf("reject unexpectedly changed warn counter; total delta=%d", delta)
	}
}

func TestValidateAttributesNoModelAndFailureMatrix(t *testing.T) {
	db := newEnforceTestDB(t)
	ctx := context.Background()
	values := map[string]interface{}{"temperature": float64(99)}

	// No registry row and a registry row with a null pin are compatibility
	// pass-throughs, and tenant/category are part of the exact lookup key.
	if err := ValidateAttributes(ctx, db, enforceTenantA, "DEVICE", enforceDevice, values); err != nil {
		t.Fatalf("no registry row: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO twin_registry (tenant_id,entity_type,entity_id) VALUES ($1,'DEVICE',$2)`, enforceTenantA, enforceDevice); err != nil {
		t.Fatalf("seed null pin: %v", err)
	}
	if err := ValidateAttributes(ctx, db, enforceTenantA, "DEVICE", enforceDevice, values); err != nil {
		t.Fatalf("null pin: %v", err)
	}
	if err := ValidateAttributes(ctx, db, enforceTenantB, "DEVICE", enforceDevice, values); err != nil {
		t.Fatalf("cross-tenant lookup should see no registry row: %v", err)
	}

	if _, err := db.Exec(`UPDATE twin_registry SET model_id='missing', model_version='1.0.0'
		WHERE tenant_id=$1 AND entity_type='DEVICE' AND entity_id=$2`, enforceTenantA, enforceDevice); err != nil {
		t.Fatalf("seed dangling pin: %v", err)
	}
	if err := ValidateAttributes(ctx, db, enforceTenantA, "DEVICE", enforceDevice, values); err == nil || !strings.Contains(err.Error(), "dangling") {
		t.Fatalf("dangling pin err=%v, want server-side integrity error", err)
	}

	if _, err := db.Exec(`INSERT INTO twin_model (tenant_id,model_id,version,kind,schema)
		VALUES ($1,'missing','1.0.0','DEVICE','"not-an-object"'::jsonb)`, enforceTenantA); err != nil {
		t.Fatalf("seed corrupt schema: %v", err)
	}
	if err := ValidateAttributes(ctx, db, enforceTenantA, "DEVICE", enforceDevice, values); err == nil || !strings.Contains(err.Error(), "decode schema") {
		t.Fatalf("corrupt schema err=%v", err)
	}

	if _, err := db.Exec(`UPDATE twin_model SET schema=$3::jsonb WHERE tenant_id=$1 AND model_id=$2`,
		enforceTenantA, "missing", enforceSchema("missing", "1.0.0", "DEVICE", "audit")); err != nil {
		t.Fatalf("seed invalid mode: %v", err)
	}
	if err := ValidateAttributes(ctx, db, enforceTenantA, "DEVICE", enforceDevice, values); err == nil || !strings.Contains(err.Error(), "enforcementMode") {
		t.Fatalf("invalid mode err=%v", err)
	}

	if err := ValidateAttributes(ctx, db, enforceTenantA, "GATEWAY", enforceDevice, values); !errors.Is(err, ErrInvalidEntityCategory) {
		t.Fatalf("invalid category err=%v", err)
	}
	if err := ValidateAttributes(ctx, nil, enforceTenantA, "DEVICE", enforceDevice, values); err == nil || !strings.Contains(err.Error(), "database unavailable") {
		t.Fatalf("nil database err=%v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := ValidateAttributes(cancelled, db, enforceTenantA, "DEVICE", enforceDevice, values); err == nil || !strings.Contains(err.Error(), "resolve twin model pin") {
		t.Fatalf("cancelled database query err=%v", err)
	}
}

func TestValidateAttributesMissingModeDefaultsToWarn(t *testing.T) {
	db := newEnforceTestDB(t)
	schema := enforceSchema("meter", "1.0.0", "DEVICE", "")
	seedEnforcementPin(t, db, enforceTenantA, "DEVICE", enforceDevice, "meter", "1.0.0", schema)
	before := attributeViolationCounter.Load()
	if err := ValidateAttributes(context.Background(), db, enforceTenantA, "DEVICE", enforceDevice,
		map[string]interface{}{"temperature": float64(99)}); err != nil {
		t.Fatalf("empty mode did not default to warn: %v", err)
	}
	if got := attributeViolationCounter.Load() - before; got != 1 {
		t.Fatalf("default warn counter delta=%d", got)
	}
}
