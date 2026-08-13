package tenant

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/twinstore"
)

const (
	modelTenantA      = "a0000000-0000-0000-0000-000000000001"
	modelTenantB      = "b0000000-0000-0000-0000-000000000002"
	modelRejectDevice = "d0000000-0000-0000-0000-000000000001"
	modelNoRowDevice  = "d0000000-0000-0000-0000-000000000002"
	modelNullDevice   = "d0000000-0000-0000-0000-000000000003"
	modelDangling     = "d0000000-0000-0000-0000-000000000004"
	modelCorrupt      = "d0000000-0000-0000-0000-000000000005"
	modelOtherTenant  = "d0000000-0000-0000-0000-000000000006"
	modelWarnAsset    = "a0000000-0000-0000-0000-000000000003"
)

// TestMain supplies the catalog tables that the older attribute authorization
// harness predates. Production always has these through migrations; providing
// them here keeps the package-level tests honest: a missing catalog table is a
// database failure (500), not something enforcement may reinterpret as "no
// model". Tables that already exist are preserved byte-for-byte.
func TestMain(m *testing.M) {
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		os.Exit(m.Run())
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tenant test catalog open: %v\n", err)
		os.Exit(2)
	}
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "tenant test catalog ping: %v\n", err)
		db.Close()
		os.Exit(2)
	}
	createdModel, err := ensureTenantTestTable(db, "twin_model", `CREATE TABLE public.twin_model (
		tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL, version varchar(64) NOT NULL,
		kind varchar(64) NOT NULL, schema jsonb NOT NULL,
		PRIMARY KEY (tenant_id, model_id, version))`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tenant test twin_model: %v\n", err)
		db.Close()
		os.Exit(2)
	}
	createdRegistry, err := ensureTenantTestTable(db, "twin_registry", `CREATE TABLE public.twin_registry (
		tenant_id uuid NOT NULL, entity_type varchar(255) NOT NULL, entity_id uuid NOT NULL,
		model_id varchar(255), model_version varchar(64),
		UNIQUE (tenant_id, entity_type, entity_id))`)
	if err != nil {
		if createdModel {
			_, _ = db.Exec(`DROP TABLE public.twin_model`)
		}
		fmt.Fprintf(os.Stderr, "tenant test twin_registry: %v\n", err)
		db.Close()
		os.Exit(2)
	}

	code := m.Run()
	if createdRegistry {
		_, _ = db.Exec(`DROP TABLE public.twin_registry`)
	}
	if createdModel {
		_, _ = db.Exec(`DROP TABLE public.twin_model`)
	}
	db.Close()
	os.Exit(code)
}

func ensureTenantTestTable(db *sql.DB, name, ddl string) (bool, error) {
	var exists bool
	if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, "public."+name).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	if _, err := db.Exec(ddl); err != nil {
		return false, err
	}
	return true, nil
}

type modelMergeCall struct {
	tenantID, entityType, entityID, scope string
	values                                map[string]interface{}
}

type modelBoundarySpy struct {
	twinstore.Store
	calls []modelMergeCall
}

func (s *modelBoundarySpy) MergeAttributes(_ context.Context, tenantID, entityType, entityID, scope string, _ int64, values map[string]interface{}) error {
	s.calls = append(s.calls, modelMergeCall{
		tenantID: tenantID, entityType: entityType, entityID: entityID, scope: scope, values: values,
	})
	return nil
}

func newAttributeModelDB(t *testing.T) (*sql.DB, *modelBoundarySpy) {
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
	schema := fmt.Sprintf("tenant_model_write_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	for _, statement := range []string{
		`CREATE TABLE device (id uuid PRIMARY KEY, tenant_id uuid NOT NULL, type text)`,
		`CREATE TABLE asset (id uuid PRIMARY KEY, tenant_id uuid NOT NULL, type text)`,
		`CREATE TABLE twin_registry (
			tenant_id uuid NOT NULL, entity_type varchar(255) NOT NULL, entity_id uuid NOT NULL,
			model_id varchar(255), model_version varchar(64),
			UNIQUE (tenant_id, entity_type, entity_id))`,
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL, version varchar(64) NOT NULL,
			kind varchar(64) NOT NULL, schema jsonb NOT NULL,
			PRIMARY KEY (tenant_id, model_id, version))`,
		`CREATE TABLE key_dictionary (key_id serial PRIMARY KEY, key text UNIQUE NOT NULL)`,
		`ALTER SEQUENCE key_dictionary_key_id_seq RESTART WITH 100000`,
		`CREATE TABLE attribute_kv (
			entity_id uuid, attribute_type int, attribute_key int,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v text,
			last_update_ts bigint,
			PRIMARY KEY (entity_id, attribute_type, attribute_key))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("schema statement: %v\n%s", err, statement)
		}
	}

	if _, err := db.Exec(`INSERT INTO device (id,tenant_id,type) VALUES
		($1,$7,'meter'),($2,$7,'meter'),($3,$7,'meter'),($4,$7,'meter'),($5,$7,'meter'),($6,$8,'meter')`,
		modelRejectDevice, modelNoRowDevice, modelNullDevice, modelDangling, modelCorrupt, modelOtherTenant,
		modelTenantA, modelTenantB); err != nil {
		t.Fatalf("seed devices: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO asset (id,tenant_id,type) VALUES ($1,$2,'building')`, modelWarnAsset, modelTenantA); err != nil {
		t.Fatalf("seed asset: %v", err)
	}
	rejectSchema := attributeModelSchema("meter", "DEVICE", "reject")
	warnSchema := attributeModelSchema("building", "ASSET", "warn")
	if _, err := db.Exec(`INSERT INTO twin_model (tenant_id,model_id,version,kind,schema) VALUES
		($1,'meter','1.0.0','DEVICE',$2::jsonb),
		($1,'building','1.0.0','ASSET',$3::jsonb),
		($1,'corrupt','1.0.0','DEVICE','"bad-schema"'::jsonb)`, modelTenantA, rejectSchema, warnSchema); err != nil {
		t.Fatalf("seed models: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO twin_registry (tenant_id,entity_type,entity_id,model_id,model_version) VALUES
		($1,'DEVICE',$2,'meter','1.0.0'),
		($1,'DEVICE',$3,NULL,NULL),
		($1,'DEVICE',$4,'missing','1.0.0'),
		($1,'DEVICE',$5,'corrupt','1.0.0'),
		($1,'ASSET',$6,'building','1.0.0')`,
		modelTenantA, modelRejectDevice, modelNullDevice, modelDangling, modelCorrupt, modelWarnAsset); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	dbpkg.SetPoolForTest(t, db)
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()
	spy := &modelBoundarySpy{}
	previousStore := twinstore.Global()
	twinstore.SetGlobal(spy)
	materializeAttributeModelKeys(t, db, "temperature", "label", "freeform")
	t.Cleanup(func() {
		twinstore.SetGlobal(previousStore)
		dbpkg.SetPoolForTest(t, nil)
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})
	return db, spy
}

func attributeModelSchema(modelID, kind, mode string) string {
	return fmt.Sprintf(`{
		"modelId":%q,"version":"1.0.0","kind":%q,"unknownKeys":"allow","enforcementMode":%q,
		"attributes":{"temperature":{"type":"number","maximum":10},"label":{"type":"string","minLength":3}},
		"features":{},"relationships":{}}`, modelID, kind, mode)
}

func materializeAttributeModelKeys(t *testing.T, db *sql.DB, keys ...string) {
	t.Helper()
	for _, key := range keys {
		id := dbpkg.GetOrInsertKeyID(key)
		if id <= 0 {
			t.Fatalf("key id for %q=%d", key, id)
		}
		if _, err := db.Exec(`INSERT INTO key_dictionary (key_id,key) VALUES ($1,$2)
			ON CONFLICT DO NOTHING`, id, key); err != nil {
			t.Fatalf("materialize key %q: %v", key, err)
		}
	}
}

func modelWriteJWT(t *testing.T, tenantID, authority string) string {
	t.Helper()
	token, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "f0000000-0000-0000-0000-000000000001", Email: authority + "@test.local",
		Authority: authority, TenantID: tenantID, Enabled: true,
	}, "attribute-model-test")
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}
	return token
}

func postModelAttributes(t *testing.T, token, entityType, entityID, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost,
		"/api/plugins/telemetry/"+entityType+"/"+entityID+"/SERVER_SCOPE", strings.NewReader(body))
	request.Header.Set("X-Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	HandleAttributeRest(response, request)
	return response
}

func persistedAttributeCount(t *testing.T, db *sql.DB, entityID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM attribute_kv WHERE entity_id=$1`, entityID).Scan(&count); err != nil {
		t.Fatalf("count persisted attributes: %v", err)
	}
	return count
}

func TestAttributeModelRejectAndErrorsPrecedeBothPersistenceBoundaries(t *testing.T) {
	db, spy := newAttributeModelDB(t)
	tenantAdmin := modelWriteJWT(t, modelTenantA, "TENANT_ADMIN")
	otherAdmin := modelWriteJWT(t, modelTenantB, "TENANT_ADMIN")

	response := postModelAttributes(t, tenantAdmin, "DEVICE", modelRejectDevice,
		`{"temperature":99,"label":"x"}`)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "model") {
		t.Fatalf("reject status=%d body=%s, want 400 model error", response.Code, response.Body.String())
	}
	if got := persistedAttributeCount(t, db, modelRejectDevice); got != 0 || len(spy.calls) != 0 {
		t.Fatalf("reject crossed a boundary: attribute_kv=%d merge_calls=%d", got, len(spy.calls))
	}

	response = postModelAttributes(t, otherAdmin, "DEVICE", modelRejectDevice, `{"temperature":1}`)
	if response.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant status=%d body=%s", response.Code, response.Body.String())
	}
	if got := persistedAttributeCount(t, db, modelRejectDevice); got != 0 || len(spy.calls) != 0 {
		t.Fatalf("cross-tenant write crossed a boundary: attribute_kv=%d merge_calls=%d", got, len(spy.calls))
	}

	for _, test := range []struct {
		name, entityID string
	}{
		{name: "dangling pin", entityID: modelDangling},
		{name: "corrupt schema", entityID: modelCorrupt},
	} {
		response = postModelAttributes(t, tenantAdmin, "DEVICE", test.entityID, `{"temperature":1}`)
		if response.Code != http.StatusInternalServerError {
			t.Errorf("%s status=%d body=%s, want 500", test.name, response.Code, response.Body.String())
		}
		if got := persistedAttributeCount(t, db, test.entityID); got != 0 {
			t.Errorf("%s persisted %d PostgreSQL rows", test.name, got)
		}
	}
	if len(spy.calls) != 0 {
		t.Fatalf("error paths invoked KV merge %d times", len(spy.calls))
	}

	response = postModelAttributes(t, tenantAdmin, "GATEWAY", modelRejectDevice, `{"temperature":1}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown category status=%d body=%s, want 400", response.Code, response.Body.String())
	}
	response = postModelAttributes(t, tenantAdmin, "", modelRejectDevice, `{"temperature":1}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("empty category status=%d body=%s, want 400", response.Code, response.Body.String())
	}
	dbpkg.SetPoolForTest(t, nil)
	response = postModelAttributes(t, tenantAdmin, "DEVICE", modelRejectDevice, `{"temperature":1}`)
	dbpkg.SetPoolForTest(t, db)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("database outage status=%d body=%s, want 500", response.Code, response.Body.String())
	}
	if len(spy.calls) != 0 {
		t.Fatalf("invalid categories/database outage invoked KV merge %d times", len(spy.calls))
	}
}

func TestAttributeWriteRejectsInvalidBodiesAndPostgresFailures(t *testing.T) {
	db, spy := newAttributeModelDB(t)
	tenantAdmin := modelWriteJWT(t, modelTenantA, "TENANT_ADMIN")
	for _, body := range []string{"null", `{"temperature":1} {"extra":true}`} {
		response := postModelAttributes(t, tenantAdmin, "DEVICE", modelNoRowDevice, body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body %q status=%d body=%s, want 400", body, response.Code, response.Body.String())
		}
	}
	if got := persistedAttributeCount(t, db, modelNoRowDevice); got != 0 || len(spy.calls) != 0 {
		t.Fatalf("invalid body crossed a boundary: attribute_kv=%d merge_calls=%d", got, len(spy.calls))
	}
	if _, err := db.Exec(`DROP TABLE attribute_kv`); err != nil {
		t.Fatalf("drop attribute table: %v", err)
	}
	response := postModelAttributes(t, tenantAdmin, "DEVICE", modelNoRowDevice, `{"write_failure_key":"kept"}`)
	if response.Code != http.StatusInternalServerError || len(spy.calls) != 0 {
		t.Fatalf("PostgreSQL failure status=%d merges=%d body=%s, want 500/0", response.Code, len(spy.calls), response.Body.String())
	}
	var dictionaryRows int
	if err := db.QueryRow(`SELECT count(*) FROM key_dictionary WHERE key='write_failure_key'`).Scan(&dictionaryRows); err != nil {
		t.Fatalf("count failed-write dictionary keys: %v", err)
	}
	if dictionaryRows != 0 {
		t.Fatalf("failed attribute write left %d key_dictionary rows", dictionaryRows)
	}
}

func TestAttributeModelWarnNoModelAndSysAdminPersistWithCanonicalIdentity(t *testing.T) {
	db, spy := newAttributeModelDB(t)
	tenantAdmin := modelWriteJWT(t, modelTenantA, "TENANT_ADMIN")
	sysAdmin := modelWriteJWT(t, "", "SYS_ADMIN")

	response := postModelAttributes(t, tenantAdmin, "asset", modelWarnAsset,
		`{"temperature":99,"label":"x"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("warn status=%d body=%s", response.Code, response.Body.String())
	}
	if got := persistedAttributeCount(t, db, modelWarnAsset); got != 2 || len(spy.calls) != 1 {
		t.Fatalf("warn boundaries: attribute_kv=%d merge_calls=%d, want 2/1", got, len(spy.calls))
	}
	if call := spy.calls[0]; call.tenantID != modelTenantA || call.entityType != "ASSET" || call.entityID != modelWarnAsset {
		t.Fatalf("warn canonical merge identity=%+v", call)
	}

	for _, entityID := range []string{modelNoRowDevice, modelNullDevice} {
		before := len(spy.calls)
		response = postModelAttributes(t, tenantAdmin, "DEVICE", entityID, `{"freeform":"kept"}`)
		if response.Code != http.StatusOK {
			t.Fatalf("no-model %s status=%d body=%s", entityID, response.Code, response.Body.String())
		}
		if got := persistedAttributeCount(t, db, entityID); got != 1 || len(spy.calls) != before+1 {
			t.Fatalf("no-model %s boundaries: attribute_kv=%d merge_calls=%d", entityID, got, len(spy.calls)-before)
		}
	}

	// An invalid write by SYS_ADMIN must find tenant A's reject model. Passing
	// through here would prove the handler incorrectly used the empty token
	// tenant rather than resolving the entity's actual tenant first.
	before := len(spy.calls)
	response = postModelAttributes(t, sysAdmin, "DEVICE", modelRejectDevice, `{"temperature":99}`)
	if response.Code != http.StatusBadRequest || persistedAttributeCount(t, db, modelRejectDevice) != 0 || len(spy.calls) != before {
		t.Fatalf("SYS_ADMIN actual-tenant reject status=%d pg=%d merges=%d body=%s",
			response.Code, persistedAttributeCount(t, db, modelRejectDevice), len(spy.calls)-before, response.Body.String())
	}
	response = postModelAttributes(t, sysAdmin, "DEVICE", modelRejectDevice, `{"temperature":1}`)
	if response.Code != http.StatusOK || persistedAttributeCount(t, db, modelRejectDevice) != 1 || len(spy.calls) != before+1 {
		t.Fatalf("SYS_ADMIN valid write status=%d pg=%d merge_delta=%d body=%s",
			response.Code, persistedAttributeCount(t, db, modelRejectDevice), len(spy.calls)-before, response.Body.String())
	}
	if call := spy.calls[len(spy.calls)-1]; call.tenantID != modelTenantA || call.entityType != "DEVICE" {
		t.Fatalf("SYS_ADMIN merge did not use actual tenant/category: %+v", call)
	}
}

// Phase 2 deliberately enforces the modeled state mutation that exists today:
// classic attributes. Catch-all telemetry remains outside this boundary.
// Phase 3 feature-state writes must reuse twinmodel.ValidateAttributes before
// either PostgreSQL or KV mutation; they must not create a bypassing validator.
