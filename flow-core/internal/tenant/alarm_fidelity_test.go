package tenant

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P3):
//
//   - handleAlarmById hardcoded originator.entityType to "DEVICE", collapsed
//     the four-state status derivation (cleared-but-unacked came back as
//     CLEARED_ACK), and omitted customerId/assigneeId/propagate*/ack-clear-
//     assign timestamps that the sibling list query already computed.
//   - Both reads set `assignee` to nil unconditionally, even with a real
//     assignee_id — the field api.go documents as v2's addition over v1.
//   - POST /api/alarmsQuery/find never read its body, so it answered the
//     whole tenant's alarms however narrowly the caller scoped the request.
//
// Uses a throwaway schema (CREATE SCHEMA / DROP SCHEMA CASCADE) rather than
// dropping shared tables, so it is safe to run against a real database.

const (
	afTenant   = "aaaaaaaa-0000-0000-0000-00000000000a"
	afOtherTen = "bbbbbbbb-0000-0000-0000-00000000000b"
	afAssetID  = "11111111-0000-0000-0000-000000000001"
	afDeviceID = "22222222-0000-0000-0000-000000000002"
	afUserID   = "33333333-0000-0000-0000-000000000003"
	afCustID   = "44444444-0000-0000-0000-000000000004"
	afAlarmAst = "55555555-0000-0000-0000-000000000005" // originator = ASSET, cleared & unacked
	afAlarmDev = "66666666-0000-0000-0000-000000000006" // originator = DEVICE, assigned
)

func newAlarmFidelityDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	schema := fmt.Sprintf("alarm_fidelity_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	for _, s := range []string{
		`CREATE TABLE device (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, name text, type text, label text)`,
		`CREATE TABLE asset (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, name text, type text, label text)`,
		`CREATE TABLE customer (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, title text)`,
		`CREATE TABLE tb_user (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, email text, first_name text, last_name text)`,
		`CREATE TABLE alarm (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, type text, severity text,
			originator_id uuid, originator_type int, acknowledged boolean, cleared boolean,
			start_ts bigint, end_ts bigint, ack_ts bigint, clear_ts bigint, assign_ts bigint,
			customer_id uuid, assignee_id uuid, additional_info text,
			propagate boolean, propagate_to_owner boolean, propagate_to_tenant boolean,
			propagate_relation_types text)`,
		`CREATE TABLE entity_alarm (
			tenant_id uuid, entity_type text, entity_id uuid, created_time bigint,
			alarm_type text, customer_id uuid, alarm_id uuid)`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v\n%s", err, s)
		}
	}
	seeds := []struct {
		q    string
		args []any
	}{
		{`INSERT INTO asset (id,created_time,tenant_id,name,type,label) VALUES ($1,10,$2,'Building A','building','HQ')`, []any{afAssetID, afTenant}},
		{`INSERT INTO device (id,created_time,tenant_id,name,type,label) VALUES ($1,20,$2,'Meter A','meter','M-1')`, []any{afDeviceID, afTenant}},
		{`INSERT INTO customer (id,created_time,tenant_id,title) VALUES ($1,30,$2,'Acme')`, []any{afCustID, afTenant}},
		{`INSERT INTO tb_user (id,created_time,tenant_id,email,first_name,last_name) VALUES ($1,40,$2,'jane@acme.org','Jane','Roe')`, []any{afUserID, afTenant}},
		// originator_type 4 = ASSET; cleared but NOT acknowledged.
		{`INSERT INTO alarm (id,created_time,tenant_id,type,severity,originator_id,originator_type,
			acknowledged,cleared,start_ts,end_ts,ack_ts,clear_ts,assign_ts,customer_id,assignee_id,
			propagate,propagate_to_owner,propagate_to_tenant)
		  VALUES ($1,100,$2,'HighTemp','CRITICAL',$3,4,false,true,1,2,0,9,0,$4,NULL,true,false,false)`,
			[]any{afAlarmAst, afTenant, afAssetID, afCustID}},
		// originator_type 5 = DEVICE; assigned to a real user.
		{`INSERT INTO alarm (id,created_time,tenant_id,type,severity,originator_id,originator_type,
			acknowledged,cleared,start_ts,end_ts,ack_ts,clear_ts,assign_ts,customer_id,assignee_id,
			propagate,propagate_to_owner,propagate_to_tenant)
		  VALUES ($1,200,$2,'LowBattery','WARNING',$3,5,false,false,3,4,0,0,7,NULL,$4,false,false,false)`,
			[]any{afAlarmDev, afTenant, afDeviceID, afUserID}},
		{`INSERT INTO entity_alarm (tenant_id,entity_type,entity_id,created_time,alarm_type,alarm_id)
		  VALUES ($1,'ASSET',$2,100,'HighTemp',$3)`, []any{afTenant, afAssetID, afAlarmAst}},
		{`INSERT INTO entity_alarm (tenant_id,entity_type,entity_id,created_time,alarm_type,alarm_id)
		  VALUES ($1,'DEVICE',$2,200,'LowBattery',$3)`, []any{afTenant, afDeviceID, afAlarmDev}},
	}
	for _, s := range seeds {
		if _, err := db.Exec(s.q, s.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, s.q)
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

func afJWT(t *testing.T, tenantID string) string {
	t.Helper()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "00000000-0000-0000-0000-000000000009", Email: "caller@x.org",
		Authority: "TENANT_ADMIN", TenantID: tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func TestAlarmById_FieldFidelity(t *testing.T) {
	newAlarmFidelityDB(t)

	rec := httptest.NewRecorder()
	handleAlarmById(rec, afTenant, afAlarmAst)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	originator, _ := got["originator"].(map[string]interface{})
	if originator["entityType"] != "ASSET" {
		t.Errorf("originator.entityType: got %v, want ASSET (a prior version hardcoded DEVICE)", originator["entityType"])
	}
	if got["status"] != "CLEARED_UNACK" {
		t.Errorf("status: got %v, want CLEARED_UNACK (a prior version collapsed this to CLEARED_ACK)", got["status"])
	}
	if got["originatorName"] != "Building A" {
		t.Errorf("originatorName: got %v, want %q", got["originatorName"], "Building A")
	}
	// Fields the by-id read used to omit entirely.
	for _, key := range []string{"customerId", "assigneeId", "ackTs", "clearTs", "assignTs",
		"propagate", "propagateToOwner", "propagateToTenant", "propagateRelationTypes", "originatorLabel"} {
		if _, ok := got[key]; !ok {
			t.Errorf("%s missing (the sibling list query already returned it)", key)
		}
	}
	if cust, ok := got["customerId"].(map[string]interface{}); !ok || cust["id"] != afCustID {
		t.Errorf("customerId: got %v, want the seeded customer", got["customerId"])
	}
	if got["clearTs"] != float64(9) {
		t.Errorf("clearTs: got %v, want 9", got["clearTs"])
	}
}

func TestAlarm_AssigneeResolves(t *testing.T) {
	newAlarmFidelityDB(t)

	t.Run("by id", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handleAlarmById(rec, afTenant, afAlarmDev)
		var got map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		assignee, ok := got["assignee"].(map[string]interface{})
		if !ok {
			t.Fatalf("assignee: got %v, want the resolved user (a prior version was always nil)", got["assignee"])
		}
		if assignee["email"] != "jane@acme.org" || assignee["firstName"] != "Jane" {
			t.Errorf("assignee: got %v", assignee)
		}
	})

	t.Run("in the list", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/alarms", nil)
		req.Header.Set("X-Authorization", "Bearer "+afJWT(t, afTenant))
		rec := httptest.NewRecorder()
		HandleAlarmsQueryFind(rec, req)

		var resp struct {
			Data []map[string]interface{} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		var assigned map[string]interface{}
		for _, item := range resp.Data {
			if id, _ := item["id"].(map[string]interface{}); id != nil && id["id"] == afAlarmDev {
				assigned = item
			}
		}
		if assigned == nil {
			t.Fatalf("the assigned alarm is missing from the list: %s", rec.Body.String())
		}
		if _, ok := assigned["assignee"].(map[string]interface{}); !ok {
			t.Errorf("assignee: got %v, want the resolved user (a prior version was always nil)", assigned["assignee"])
		}
		// An unassigned alarm must still report nil, not an empty object.
		for _, item := range resp.Data {
			if id, _ := item["id"].(map[string]interface{}); id != nil && id["id"] == afAlarmAst {
				if item["assignee"] != nil {
					t.Errorf("unassigned alarm got assignee %v, want nil", item["assignee"])
				}
			}
		}
	})

	t.Run("a foreign tenant's user does not resolve", func(t *testing.T) {
		if got := resolveAssignee(afOtherTen, afUserID); got != nil {
			t.Errorf("resolved another tenant's user: %v", got)
		}
	})
}

func TestAlarmsQueryFind_HonorsPostedEntityFilter(t *testing.T) {
	newAlarmFidelityDB(t)
	tok := afJWT(t, afTenant)

	post := func(body string) []map[string]interface{} {
		t.Helper()
		req := httptest.NewRequest("POST", "/api/alarmsQuery/find", strings.NewReader(body))
		req.Header.Set("X-Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		HandleAlarmsQueryFind(rec, req)
		if rec.Code != 200 {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		var resp struct {
			Data          []map[string]interface{} `json:"data"`
			TotalElements int                      `json:"totalElements"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return resp.Data
	}

	t.Run("scopes to the posted entity", func(t *testing.T) {
		got := post(`{"entityFilter":{"type":"singleEntity","singleEntity":{"entityType":"DEVICE","id":"` + afDeviceID + `"}}}`)
		if len(got) != 1 {
			t.Fatalf("got %d alarms, want only the device's 1 (a prior version ignored the body and returned the whole tenant)", len(got))
		}
		id, _ := got[0]["id"].(map[string]interface{})
		if id["id"] != afAlarmDev {
			t.Errorf("got alarm %v, want the device's", id["id"])
		}
	})

	t.Run("legacy flat entityId shape", func(t *testing.T) {
		got := post(`{"entityId":{"entityType":"ASSET","id":"` + afAssetID + `"}}`)
		if len(got) != 1 {
			t.Fatalf("got %d alarms, want only the asset's 1", len(got))
		}
	})

	t.Run("no filter still returns the whole tenant", func(t *testing.T) {
		if got := post(`{}`); len(got) != 2 {
			t.Errorf("got %d alarms, want both (an unscoped query must keep working)", len(got))
		}
	})

	t.Run("a malformed entity ref degrades to unscoped rather than erroring", func(t *testing.T) {
		if got := post(`{"entityId":{"entityType":"DEVICE","id":"not-a-uuid"}}`); len(got) != 2 {
			t.Errorf("got %d alarms, want both", len(got))
		}
	})

	t.Run("GET /api/alarms is unaffected", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/alarms", nil)
		req.Header.Set("X-Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		HandleAlarmsQueryFind(rec, req)
		var resp struct {
			Data []map[string]interface{} `json:"data"`
		}
		json.Unmarshal(rec.Body.Bytes(), &resp)
		if len(resp.Data) != 2 {
			t.Errorf("got %d alarms, want both", len(resp.Data))
		}
	})
}
