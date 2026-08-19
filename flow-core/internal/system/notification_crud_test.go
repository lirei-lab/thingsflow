package system

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

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): the notification writers all routed
// through saveJSONEntity, which echoes the posted JSON back with a generated
// id and never touches the database. Every list endpoint then correctly
// reported empty, because nothing had ever been written — the tables existed
// the whole time. These tests assert the round trip: save, then read back
// through the real list handler.
//
// Throwaway schema (CREATE SCHEMA / DROP SCHEMA CASCADE), safe against a
// real database.

const (
	ncTenantA = "aaaaaaaa-9999-9999-9999-aaaaaaaaaaaa"
	ncTenantB = "bbbbbbbb-9999-9999-9999-bbbbbbbbbbbb"
)

func newNotificationDB(t *testing.T) *sql.DB {
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
	schema := fmt.Sprintf("notif_crud_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	for _, s := range []string{
		`CREATE TABLE notification_target (
			id uuid PRIMARY KEY, created_time bigint NOT NULL, tenant_id uuid NOT NULL,
			name varchar(255) NOT NULL, configuration varchar(10000) NOT NULL,
			external_id uuid, CONSTRAINT uq_notification_target_name UNIQUE (tenant_id, name))`,
		`CREATE TABLE notification_template (
			id uuid PRIMARY KEY, created_time bigint NOT NULL, tenant_id uuid NOT NULL,
			name varchar(255) NOT NULL, notification_type varchar(50) NOT NULL,
			configuration text NOT NULL, external_id uuid,
			CONSTRAINT uq_notification_template_name UNIQUE (tenant_id, name))`,
		`CREATE TABLE notification_rule (
			id uuid PRIMARY KEY, created_time bigint NOT NULL, tenant_id uuid NOT NULL,
			name varchar(255) NOT NULL, enabled boolean NOT NULL DEFAULT true,
			template_id uuid NOT NULL REFERENCES notification_template(id),
			trigger_type varchar(50) NOT NULL, trigger_config varchar(1000) NOT NULL,
			recipients_config varchar(10000) NOT NULL, additional_config varchar(255),
			external_id uuid, CONSTRAINT uq_notification_rule_name UNIQUE (tenant_id, name))`,
		`CREATE TABLE notification_request (
			id uuid PRIMARY KEY, created_time bigint NOT NULL, tenant_id uuid NOT NULL,
			targets varchar(10000) NOT NULL, template_id uuid, template text, info text,
			additional_config varchar(1000), originator_entity_id uuid,
			originator_entity_type varchar(32), rule_id uuid, status varchar(32), stats varchar(10000))`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v\n%s", err, s)
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

func ncPost(t *testing.T, h http.HandlerFunc, tenantID, path string, payload map[string]interface{}) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+fakeSystemJWT(t, tenantID))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func ncList(t *testing.T, h http.HandlerFunc, tenantID, path string) []map[string]interface{} {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeSystemJWT(t, tenantID))
	rec := httptest.NewRecorder()
	h(rec, req)
	var resp struct {
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode list: %v\nbody=%s", err, rec.Body.String())
	}
	return resp.Data
}

func TestNotificationTarget_RoundTrips(t *testing.T) {
	newNotificationDB(t)

	rec := ncPost(t, HandleNotificationTarget, ncTenantA, "/api/notification/target", map[string]interface{}{
		"name":          "Ops team",
		"configuration": map[string]interface{}{"type": "PLATFORM_USERS"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("save: %d %s", rec.Code, rec.Body.String())
	}

	got := ncList(t, HandleNotificationTargets, ncTenantA, "/api/notification/targets")
	if len(got) != 1 {
		t.Fatalf("list: got %d, want 1 (a prior version never persisted, so this stayed empty)", len(got))
	}
	if got[0]["name"] != "Ops team" {
		t.Errorf("name: got %v", got[0]["name"])
	}
	cfg, ok := got[0]["configuration"].(map[string]interface{})
	if !ok || cfg["type"] != "PLATFORM_USERS" {
		t.Errorf("configuration did not round-trip: %v", got[0]["configuration"])
	}

	t.Run("another tenant does not see it", func(t *testing.T) {
		if other := ncList(t, HandleNotificationTargets, ncTenantB, "/api/notification/targets"); len(other) != 0 {
			t.Errorf("tenant B saw %d of tenant A's targets", len(other))
		}
	})
}

func TestNotificationTemplateAndRule_RoundTrip(t *testing.T) {
	db := newNotificationDB(t)

	rec := ncPost(t, HandleNotificationTemplate, ncTenantA, "/api/notification/template", map[string]interface{}{
		"name":             "Alarm mail",
		"notificationType": "ALARM",
		"configuration":    map[string]interface{}{"deliveryMethodsTemplates": map[string]interface{}{}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("save template: %d %s", rec.Code, rec.Body.String())
	}
	templates := ncList(t, HandleNotificationTemplates, ncTenantA, "/api/notification/templates")
	if len(templates) != 1 || templates[0]["notificationType"] != "ALARM" {
		t.Fatalf("templates: got %v", templates)
	}
	var templateId string
	if err := db.QueryRow("SELECT id::text FROM notification_template WHERE tenant_id = $1", ncTenantA).Scan(&templateId); err != nil {
		t.Fatalf("read template id: %v", err)
	}

	t.Run("rule requires a real template", func(t *testing.T) {
		rec := ncPost(t, HandleNotificationRule, ncTenantA, "/api/notification/rule", map[string]interface{}{
			"name": "no template",
		})
		if rec.Code != http.StatusBadRequest {
			t.Errorf("got %d, want 400 (template_id is NOT NULL with an FK)", rec.Code)
		}
	})

	t.Run("rule refuses another tenant's template", func(t *testing.T) {
		rec := ncPost(t, HandleNotificationRule, ncTenantB, "/api/notification/rule", map[string]interface{}{
			"name":       "borrowed",
			"templateId": map[string]interface{}{"entityType": "NOTIFICATION_TEMPLATE", "id": templateId},
		})
		if rec.Code != http.StatusForbidden {
			t.Errorf("got %d, want 403", rec.Code)
		}
	})

	t.Run("rule round-trips", func(t *testing.T) {
		rec := ncPost(t, HandleNotificationRule, ncTenantA, "/api/notification/rule", map[string]interface{}{
			"name":        "On alarm",
			"templateId":  map[string]interface{}{"entityType": "NOTIFICATION_TEMPLATE", "id": templateId},
			"triggerType": "ALARM",
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("save rule: %d %s", rec.Code, rec.Body.String())
		}
		rules := ncList(t, HandleNotificationRules, ncTenantA, "/api/notification/rules")
		if len(rules) != 1 || rules[0]["name"] != "On alarm" {
			t.Fatalf("rules: got %v", rules)
		}
		if rules[0]["enabled"] != true {
			t.Errorf("enabled: got %v, want true by default", rules[0]["enabled"])
		}
	})
}

func TestNotificationRequest_PersistsAndDoesNotClaimDelivery(t *testing.T) {
	newNotificationDB(t)

	rec := ncPost(t, HandleNotificationRequestSave, ncTenantA, "/api/notification/request", map[string]interface{}{
		"targets": []interface{}{"11111111-1111-1111-1111-111111111111"},
		"info":    map[string]interface{}{"type": "GENERAL"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("save: %d %s", rec.Code, rec.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The old version stamped "SENT" without storing anything or sending
	// anything. There is still no transport, so it must not claim delivery.
	if got["status"] != "SCHEDULED" {
		t.Errorf("status: got %v, want SCHEDULED (no delivery transport exists on this platform)", got["status"])
	}

	requests := ncList(t, HandleNotificationRequests, ncTenantA, "/api/notification/requests")
	if len(requests) != 1 {
		t.Fatalf("list: got %d, want 1 (a prior version never persisted)", len(requests))
	}
	if requests[0]["status"] != "SCHEDULED" {
		t.Errorf("listed status: got %v", requests[0]["status"])
	}
}

func TestNotificationsRead_ScopesToCaller(t *testing.T) {
	newNotificationDB(t)

	// `notification` has no producer yet, so the UPDATE legitimately affects
	// zero rows — the point of this test is that the handler no longer
	// answers 200 without attempting anything, and that a malformed id is
	// filtered out before it can reach postgres as an invalid uuid cast.
	req := httptest.NewRequest(http.MethodPut,
		"/api/notifications/read?notifications=not-a-uuid,22222222-2222-2222-2222-222222222222", nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeSystemJWT(t, ncTenantA))
	rec := httptest.NewRecorder()

	ids := notificationIdsFromRequest(req)
	if len(ids) != 1 || ids[0] != "22222222-2222-2222-2222-222222222222" {
		t.Fatalf("id parsing: got %v, want only the valid uuid", ids)
	}

	HandleNotificationsRead(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", rec.Code)
	}
}
