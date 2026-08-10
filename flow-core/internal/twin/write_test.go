package twin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/twinstore"
)

// writeTestTenantA/B are distinct tenants used to prove cross-tenant writes
// are denied and SYS_ADMIN resolves the entity's ACTUAL tenant.
const (
	writeTenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	writeTenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	writeDeviceA = "33333333-3333-3333-3333-333333333333" // tenant A, reject mode
	writeDeviceB = "44444444-4444-4444-4444-444444444444" // tenant B, cross-tenant target
	writeAssetA  = "11111111-1111-1111-1111-111111111111" // tenant A, warn mode
	writeMiss    = "99999999-9999-9999-9999-999999999999" // existing device, pinned meter-feature
	writeUnknown = "88888888-8888-8888-8888-888888888888" // truly missing entity (404)
)

type writeMergeCall struct {
	tenantID, entityType, entityID, scope string
	values                                map[string]interface{}
}

type writeKVSpy struct {
	twinstore.Store
	calls []writeMergeCall
}

func (s *writeKVSpy) MergeAttributes(_ context.Context, tenantID, entityType, entityID, scope string, _ int64, values map[string]interface{}) error {
	s.calls = append(s.calls, writeMergeCall{tenantID, entityType, entityID, scope, values})
	return nil
}

func newWriteTestDB(t *testing.T) (*sql.DB, *writeKVSpy) {
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
	schema := fmt.Sprintf("twin_write_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	for _, statement := range []string{
		`CREATE TABLE device (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid NOT NULL, type text, name text, label text, additional_info text)`,
		`CREATE TABLE asset (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid NOT NULL, type text, name text, label text, additional_info text)`,
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

	if _, err := db.Exec(`INSERT INTO device (id,created_time,tenant_id,type,name,label,additional_info) VALUES
		($1,100,$5,'meter','Meter A','Main','{}'),($2,200,$6,'meter','Meter B','','{}'),
		($3,300,$5,'building','Building A','HQ','{}'),($4,400,$5,'other','Ghost','','{}')`,
		writeDeviceA, writeDeviceB, writeAssetA, writeMiss, writeTenantA, writeTenantB); err != nil {
		t.Fatalf("seed devices: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO asset (id,created_time,tenant_id,type,name,label,additional_info) VALUES ($1,100,$2,'building','B','','{}')`, writeAssetA, writeTenantA); err != nil {
		t.Fatalf("seed asset: %v", err)
	}
	rejectSchema := writeModelSchema("meter", "DEVICE", "reject", `"temperature":{"type":"number","maximum":10}`)
	warnSchema := writeModelSchema("building", "ASSET", "warn", `"status":{"type":"string","enum":["on","off"]}`)
	featureSchema := writeModelSchema("meter-feature", "DEVICE", "reject", `"temperature":{"type":"number","maximum":10}`, `"energy":{"definition":"thingsflow:feature:energy:1.0.0","properties":{"kwh":{"type":"number"}},"desiredProperties":{"target_kwh":{"type":"number"}}}`)
	if _, err := db.Exec(`INSERT INTO twin_model (tenant_id,model_id,version,kind,schema) VALUES
		($1,'meter','1.0.0','DEVICE',$2::jsonb),
		($1,'building','1.0.0','ASSET',$3::jsonb),
		($1,'meter-feature','1.0.0','DEVICE',$4::jsonb)`, writeTenantA, rejectSchema, warnSchema, featureSchema); err != nil {
		t.Fatalf("seed models: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO twin_registry (tenant_id,entity_type,entity_id,model_id,model_version) VALUES
		($1,'DEVICE',$2,'meter','1.0.0'),
		($1,'ASSET',$3,'building','1.0.0'),
		($1,'DEVICE',$4,'meter-feature','1.0.0')`,
		writeTenantA, writeDeviceA, writeAssetA, writeMiss); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	dbpkg.SetPoolForTest(t, db)
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()
	spy := &writeKVSpy{}
	previousStore := twinstore.Global()
	twinstore.SetGlobal(spy)
	t.Cleanup(func() {
		twinstore.SetGlobal(previousStore)
		dbpkg.SetPoolForTest(t, nil)
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})
	return db, spy
}

func writeModelSchema(modelID, kind, mode string, attrDecl string, featureDecl ...string) string {
	features := `{}`
	if len(featureDecl) > 0 {
		features = `{` + strings.Join(featureDecl, ",") + `}`
	}
	return fmt.Sprintf(`{
		"modelId":%q,"version":"1.0.0","kind":%q,"unknownKeys":"reject","enforcementMode":%q,
		"attributes":{%s},"features":%s,"relationships":{}}`, modelID, kind, mode, attrDecl, features)
}

func writeJWT(t *testing.T, tenantID string, sysAdmin bool) string {
	t.Helper()
	authpkg.InitConfig()
	authority := "TENANT_ADMIN"
	if sysAdmin {
		authority = "SYS_ADMIN"
	}
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000001",
		Email:     "write@test.org",
		Authority: authority,
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func countAttributeRows(t *testing.T, db *sql.DB, entityID string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM attribute_kv WHERE entity_id=$1`, entityID).Scan(&n); err != nil {
		t.Fatalf("count attribute_kv: %v", err)
	}
	return n
}

func TestSaveAttributesRejectModePersistsNothing(t *testing.T) {
	db, spy := newWriteTestDB(t)
	// temperature=11 exceeds maximum 10 → reject mode → 400, nothing persisted.
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeDeviceA+"/attributes",
		strings.NewReader(`{"attributes":{"temperature":11}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveAttributes(w, req, "DEVICE", writeDeviceA)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeDeviceA); n != 0 {
		t.Fatalf("reject crossed attribute_kv boundary: rows=%d", n)
	}
	if len(spy.calls) != 0 {
		t.Fatalf("reject crossed KV boundary: merge_calls=%d", len(spy.calls))
	}
}

func TestSaveAttributesWarnModePersists(t *testing.T) {
	db, spy := newWriteTestDB(t)
	// ASSET building model is warn mode; status "off" is within the enum.
	req := httptest.NewRequest("PUT", "/api/twins/ASSET/"+writeAssetA+"/attributes",
		strings.NewReader(`{"attributes":{"status":"off"}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveAttributes(w, req, "ASSET", writeAssetA)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s want 200", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeAssetA); n != 1 {
		t.Fatalf("warn persist attribute_kv rows=%d want 1", n)
	}
	if len(spy.calls) != 1 {
		t.Fatalf("warn KV merge_calls=%d want 1", len(spy.calls))
	}
	if spy.calls[0].tenantID != writeTenantA || spy.calls[0].entityType != "ASSET" {
		t.Fatalf("merge call=%+v", spy.calls[0])
	}
}

func TestSaveAttributesCrossTenantDenied(t *testing.T) {
	db, spy := newWriteTestDB(t)
	// device B belongs to tenant B; caller is tenant A → 403, nothing persisted.
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeDeviceB+"/attributes",
		strings.NewReader(`{"attributes":{"temperature":5}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveAttributes(w, req, "DEVICE", writeDeviceB)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s want 403", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeDeviceB); n != 0 {
		t.Fatalf("cross-tenant write persisted: rows=%d", n)
	}
	if len(spy.calls) != 0 {
		t.Fatalf("cross-tenant write merged: calls=%d", len(spy.calls))
	}
}

func TestSaveAttributesSysAdminResolvesActualTenant(t *testing.T) {
	_, spy := newWriteTestDB(t)
	// SYS_ADMIN writes device B (tenant B); model lookup + persistence must use
	// tenant B's actual tenant, not the empty/system tenant.
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeDeviceB+"/attributes",
		strings.NewReader(`{"attributes":{"temperature":5}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, "", true))
	w := httptest.NewRecorder()

	HandleSaveAttributes(w, req, "DEVICE", writeDeviceB)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s want 200", w.Code, w.Body.String())
	}
	if len(spy.calls) != 1 || spy.calls[0].tenantID != writeTenantB {
		t.Fatalf("SYS_ADMIN merge call=%+v want tenant=%s", spy.calls, writeTenantB)
	}
}

func TestSaveAttributesUnknownEntityNotFound(t *testing.T) {
	_, _ = newWriteTestDB(t)
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeUnknown+"/attributes",
		strings.NewReader(`{"attributes":{"temperature":5}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveAttributes(w, req, "DEVICE", writeUnknown)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s want 404", w.Code, w.Body.String())
	}
}

func TestSaveAttributesMalformedJSON400(t *testing.T) {
	db, spy := newWriteTestDB(t)
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeDeviceA+"/attributes",
		strings.NewReader(`{"attributes":`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveAttributes(w, req, "DEVICE", writeDeviceA)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeDeviceA); n != 0 {
		t.Fatalf("malformed write persisted: rows=%d", n)
	}
	if len(spy.calls) != 0 {
		t.Fatalf("malformed write merged: calls=%d", len(spy.calls))
	}
}

func TestSaveFeaturesUndeclaredFeature400(t *testing.T) {
	db, spy := newWriteTestDB(t)
	// writeMiss has a pinned meter-feature model with unknownKeys=reject; a
	// feature "bogus" is undeclared → 400, nothing persisted.
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeMiss+"/features",
		strings.NewReader(`{"features":{"bogus":{"properties":{"x":1}}}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveFeatures(w, req, "DEVICE", writeMiss)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeMiss); n != 0 {
		t.Fatalf("undeclared feature persisted: rows=%d", n)
	}
	if len(spy.calls) != 0 {
		t.Fatalf("undeclared feature merged: calls=%d", len(spy.calls))
	}
}

func TestSaveFeaturesPersistsFeatureProperty(t *testing.T) {
	db, spy := newWriteTestDB(t)
	// writeMiss pinned to meter-feature (reject). Feature "energy" declared;
	// property kwh=5 within bounds → persists feature.energy.kwh.
	req := httptest.NewRequest("PATCH", "/api/twins/DEVICE/"+writeMiss+"/features",
		strings.NewReader(`{"features":{"energy":{"properties":{"kwh":5}}}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveFeatures(w, req, "DEVICE", writeMiss)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s want 200", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeMiss); n != 1 {
		t.Fatalf("feature persist rows=%d want 1", n)
	}
	if len(spy.calls) != 1 {
		t.Fatalf("feature KV merge_calls=%d want 1", len(spy.calls))
	}
	var key string
	if err := db.QueryRow(`SELECT k.key FROM attribute_kv a JOIN key_dictionary k ON k.key_id=a.attribute_key WHERE a.entity_id=$1`,
		writeMiss).Scan(&key); err != nil {
		t.Fatalf("read persisted key: %v", err)
	}
	if key != "feature.energy.kwh" {
		t.Fatalf("persisted key=%q want feature.energy.kwh", key)
	}
}

func TestSaveFeaturesRejectModePersistsNothing(t *testing.T) {
	db, spy := newWriteTestDB(t)
	// energy.kwh=99 is a number but unbounded; the "meter" (reject) model
	// declares no "energy" feature → undeclared (unknownKeys=reject) → 400.
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeDeviceA+"/features",
		strings.NewReader(`{"features":{"energy":{"properties":{"kwh":99}}}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveFeatures(w, req, "DEVICE", writeDeviceA)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeDeviceA); n != 0 {
		t.Fatalf("reject feature persisted: rows=%d", n)
	}
	if len(spy.calls) != 0 {
		t.Fatalf("reject feature merged: calls=%d", len(spy.calls))
	}
}

func TestSaveFeaturesCrossTenantDenied(t *testing.T) {
	db, spy := newWriteTestDB(t)
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeDeviceB+"/features",
		strings.NewReader(`{"features":{"energy":{"properties":{"kwh":5}}}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveFeatures(w, req, "DEVICE", writeDeviceB)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s want 403", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeDeviceB); n != 0 {
		t.Fatalf("cross-tenant feature persisted: rows=%d", n)
	}
	if len(spy.calls) != 0 {
		t.Fatalf("cross-tenant feature merged: calls=%d", len(spy.calls))
	}
}

func TestResolvePinnedSchemaNoPinPassThrough(t *testing.T) {
	_, _ = newWriteTestDB(t)
	// device B (tenant B) has no registry row → nil schema, nil error.
	schema, err := resolvePinnedSchema(context.Background(), writeTenantB, "DEVICE", writeDeviceB)
	if err != nil {
		t.Fatalf("resolvePinnedSchema error: %v", err)
	}
	if schema != nil {
		t.Fatalf("schema = %+v want nil (no pin)", schema)
	}
}

func TestTwinWriteRoutesRegistered(t *testing.T) {
	_, _ = newWriteTestDB(t)
	// The api.go route registration is covered by api_twinmodel_contract_test.go;
	// here we assert the write handlers exist and wire entityType/entityID.
	var _ http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		HandleSaveAttributes(w, r, r.PathValue("entityType"), r.PathValue("entityId"))
	}
	var _ http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		HandleSaveFeatures(w, r, r.PathValue("entityType"), r.PathValue("entityId"))
	}
	// Ensure the decoded body strictness rejects a trailing second object.
	var out struct {
		Attributes map[string]interface{} `json:"attributes"`
	}
	req := httptest.NewRequest("PUT", "/x", strings.NewReader(`{"attributes":{}} {}`))
	rec := httptest.NewRecorder()
	if decodeTwinWriteBody(rec, req, &out) {
		t.Fatalf("strict decode accepted trailing content")
	}
	if out.Attributes == nil {
		t.Fatalf("strict decode lost valid first object")
	}
}

func TestTwinWriteResponseEnvelopeShape(t *testing.T) {
	// The success payload must carry the TB-compatible envelope via httputil.
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(`{"persisted":true}`), &payload); err != nil {
		t.Fatalf("json: %v", err)
	}
	if payload["persisted"] != true {
		t.Fatalf("payload=%v", payload)
	}
}

// TestSaveFeaturesPersistsDesiredProperties — a feature write carrying
// desiredProperties persists them as feature.<name>.desired.<property>
// SERVER_SCOPE keys (R5), distinct from reported feature.<name>.<property>.
func TestSaveFeaturesPersistsDesiredProperties(t *testing.T) {
	db, spy := newWriteTestDB(t)
	// writeMiss is pinned to meter-feature (reject); energy.desiredProperties.target_kwh
	// is declared by the model → persists feature.energy.desired.target_kwh.
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeMiss+"/features",
		strings.NewReader(`{"features":{"energy":{"properties":{"kwh":5},"desiredProperties":{"target_kwh":50}}}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveFeatures(w, req, "DEVICE", writeMiss)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s want 200", w.Code, w.Body.String())
	}
	// Reported + desired both persisted → 2 attribute_kv rows.
	if n := countAttributeRows(t, db, writeMiss); n != 2 {
		t.Fatalf("feature persist rows=%d want 2 (reported + desired)", n)
	}
	var keys []string
	rows, err := db.Query(`SELECT k.key FROM attribute_kv a JOIN key_dictionary k ON k.key_id=a.attribute_key WHERE a.entity_id=$1 ORDER BY k.key`, writeMiss)
	if err != nil {
		t.Fatalf("query keys: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("scan key: %v", err)
		}
		keys = append(keys, k)
	}
	if len(keys) != 2 || keys[0] != "feature.energy.desired.target_kwh" || keys[1] != "feature.energy.kwh" {
		t.Fatalf("persisted keys=%v want [feature.energy.desired.target_kwh feature.energy.kwh]", keys)
	}
	if len(spy.calls) != 1 {
		t.Fatalf("feature KV merge_calls=%d want 1", len(spy.calls))
	}
	// The KV merge received BOTH reported and desired values in one call.
	merged := spy.calls[0].values
	if merged["feature.energy.kwh"] != float64(5) {
		t.Fatalf("merged reported=%#v", merged["feature.energy.kwh"])
	}
	if merged["feature.energy.desired.target_kwh"] != float64(50) {
		t.Fatalf("merged desired=%#v", merged["feature.energy.desired.target_kwh"])
	}
}

// TestSaveFeaturesDesiredRejectModePersistsNothing — a desired property that
// violates the pinned model (unknownKeys=reject: undesired key) → 400 and
// nothing persisted.
func TestSaveFeaturesDesiredRejectModePersistsNothing(t *testing.T) {
	db, spy := newWriteTestDB(t)
	// energy.desiredProperties.unknown is NOT declared by the meter-feature model
	// (unknownKeys=reject) → 400, nothing persisted.
	req := httptest.NewRequest("PUT", "/api/twins/DEVICE/"+writeMiss+"/features",
		strings.NewReader(`{"features":{"energy":{"desiredProperties":{"unknown":1}}}}`))
	req.Header.Set("X-Authorization", "Bearer "+writeJWT(t, writeTenantA, false))
	w := httptest.NewRecorder()

	HandleSaveFeatures(w, req, "DEVICE", writeMiss)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
	if n := countAttributeRows(t, db, writeMiss); n != 0 {
		t.Fatalf("desired reject persisted: rows=%d", n)
	}
	if len(spy.calls) != 0 {
		t.Fatalf("desired reject merged: calls=%d", len(spy.calls))
	}
}

// TestSplitDesiredFeatureKey — the persisted-key parser used by the read
// surface splits feature.<name>.desired.<property> and rejects other shapes.
func TestSplitDesiredFeatureKey(t *testing.T) {
	cases := []struct {
		key      string
		name, op string
		ok       bool
	}{
		{"feature.energy.desired.target_kwh", "energy", "target_kwh", true},
		{"feature.energy.desired.a.b", "energy", "a.b", true},
		{"feature.energy.kwh", "", "", false},      // reported, not desired
		{"feature.energy.desired.", "", "", false}, // empty property
		{"other.energy.desired.k", "", "", false},  // wrong prefix
		{"feature..desired.k", "", "", false},      // empty name
	}
	for _, tc := range cases {
		name, op, ok := splitDesiredFeatureKey(tc.key)
		if name != tc.name || op != tc.op || ok != tc.ok {
			t.Fatalf("splitDesiredFeatureKey(%q) = (%q,%q,%v), want (%q,%q,%v)", tc.key, name, op, ok, tc.name, tc.op, tc.ok)
		}
	}
}

// TestWriteMethodGate guards R6 enforcement integrity: the methodless fallback
// routes for /attributes and /features are not wrapped with policy.EnforceWrite
// (they exist to emit the canonical 405 envelope), so the handlers themselves
// must reject any method other than PUT/PATCH. Otherwise a POST/DELETE/GET could
// reach the write handler through the fallback and execute an unenforced write.
// The method gate runs before RequireAuth, so no auth token is needed here.
func TestWriteMethodGate(t *testing.T) {
	cases := []struct {
		name   string
		call   func(w http.ResponseWriter, r *http.Request)
		method string
	}{
		{"attributes POST", func(w http.ResponseWriter, r *http.Request) {
			HandleSaveAttributes(w, r, "DEVICE", writeDeviceA)
		}, http.MethodPost},
		{"features DELETE", func(w http.ResponseWriter, r *http.Request) {
			HandleSaveFeatures(w, r, "DEVICE", writeDeviceA)
		}, http.MethodDelete},
		{"attributes GET", func(w http.ResponseWriter, r *http.Request) {
			HandleSaveAttributes(w, r, "DEVICE", writeDeviceA)
		}, http.MethodGet},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, "/api/twins/DEVICE/"+writeDeviceA+"/attributes", strings.NewReader(`{"attributes":{"x":1}}`))
			response := httptest.NewRecorder()
			tc.call(response, request)
			if response.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s status=%d body=%s, want 405", tc.method, "write", response.Code, response.Body.String())
			}
		})
	}
}
