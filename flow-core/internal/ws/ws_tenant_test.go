package ws

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/testdb"
)

// Phase 5b — the WebSocket plane was never covered by the Phase 2 HTTP tenant
// work: every WS read keyed off an attacker-supplied entityId with no tenant
// predicate and no ownership check, while session.TenantID (set from the
// VERIFIED JWT claim at authCmd time) was sitting right there. These tests pin
// the fix: a session of tenant A gets NOTHING for a tenant B entity on each
// path, while tenant A keeps getting its own data (the TB dashboard must not
// regress — the 373-endpoint contract gate does not cover WS).

const (
	wsTenantA = "aaaaaaaa-0000-0000-0000-00000000000a"
	wsTenantB = "bbbbbbbb-0000-0000-0000-00000000000b"

	wsAssetA  = "11111111-1111-1111-1111-11111111111a"
	wsAssetB  = "11111111-1111-1111-1111-11111111111b"
	wsDeviceA = "22222222-2222-2222-2222-22222222222a"
	wsDeviceB = "22222222-2222-2222-2222-22222222222b"

	// sysTenant is ThingsBoard's system-tenant sentinel: a real SYS_ADMIN token
	// carries this (non-empty) tenantId while its SYS_ADMIN scope lets it cross.
	wsSysTenant = "13814000-1dd2-11b2-8080-808080808080"
)

// newWSTenantTestDB builds the minimal slice of the TB schema the WS reads
// touch, seeded with a mirror-image pair of entities (one per tenant) so every
// assertion has both a positive and a negative case.
func newWSTenantTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	pool, err := sql.Open("postgres", testdb.Scoped(t, dsn))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := pool.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// Default (postgres ts_kv / ts_kv_latest) read backend — the path the gap
	// lives on. TWIN_STATE_STORE unset so nothing short-circuits the DB reads.
	t.Setenv("USAGE_READ_BACKEND", "postgres")
	t.Setenv("TWIN_STATE_STORE", "")

	stmts := []string{
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS entity_view CASCADE`,
		`DROP TABLE IF EXISTS api_usage_state CASCADE`,
		`DROP TABLE IF EXISTS attribute_kv CASCADE`,
		`DROP TABLE IF EXISTS ts_kv CASCADE`,
		`DROP TABLE IF EXISTS ts_kv_latest CASCADE`,
		`DROP TABLE IF EXISTS key_dictionary CASCADE`,
		`DROP TABLE IF EXISTS topology_edge CASCADE`,
		`DROP TABLE IF EXISTS relation CASCADE`,
		`CREATE TABLE asset (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			name text, label text, type text)`,
		`CREATE TABLE device (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			name text, label text, type text)`,
		`CREATE TABLE entity_view (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			name text, type text)`,
		`CREATE TABLE api_usage_state (id uuid PRIMARY KEY, tenant_id uuid, entity_id uuid)`,
		`CREATE TABLE key_dictionary (key_id serial PRIMARY KEY, key text UNIQUE)`,
		`CREATE TABLE attribute_kv (entity_id uuid, attribute_type int, attribute_key int,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v json,
			last_update_ts bigint)`,
		`CREATE TABLE ts_kv (entity_id uuid, key int, ts bigint,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v text)`,
		`CREATE TABLE ts_kv_latest (entity_id uuid, key int, ts bigint,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v text)`,
		`CREATE TABLE topology_edge (tenant_id uuid not null, from_id uuid not null,
			from_type text not null, to_id uuid not null, to_type text not null,
			relation_type text not null, relation_type_group text not null default 'COMMON',
			direction text not null default 'DIRECTED', metadata jsonb not null default '{}'::jsonb,
			created_time bigint not null default 0, updated_time bigint not null default 0,
			version bigint not null default 1)`,
		`CREATE TABLE relation (from_id uuid, from_type text, to_id uuid, to_type text,
			relation_type_group text, relation_type text, additional_info text)`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(s); err != nil {
			t.Fatalf("schema (%s): %v", s, err)
		}
	}

	seed := []struct {
		asset, device, tenant, suffix string
	}{
		{wsAssetA, wsDeviceA, wsTenantA, "A"},
		{wsAssetB, wsDeviceB, wsTenantB, "B"},
	}
	for _, s := range seed {
		if _, err := pool.Exec(
			`INSERT INTO asset (id, created_time, tenant_id, name, label, type)
			 VALUES ($1, 1, $2, $3, $4, 'building')`,
			s.asset, s.tenant, "asset"+s.suffix, "label"+s.suffix); err != nil {
			t.Fatalf("seed asset: %v", err)
		}
		if _, err := pool.Exec(
			`INSERT INTO device (id, created_time, tenant_id, name, label, type)
			 VALUES ($1, 1, $2, $3, $4, 'meter')`,
			s.device, s.tenant, "device"+s.suffix, "label"+s.suffix); err != nil {
			t.Fatalf("seed device: %v", err)
		}
		// asset -Contains-> device, so a relations walk from the asset yields
		// exactly one hydrated device per tenant.
		if _, err := pool.Exec(
			`INSERT INTO topology_edge (tenant_id, from_id, from_type, to_id, to_type, relation_type)
			 VALUES ($1, $2, 'ASSET', $3, 'DEVICE', 'Contains')`,
			s.tenant, s.asset, s.device); err != nil {
			t.Fatalf("seed topology_edge: %v", err)
		}
	}

	dbpkg.SetPoolForTest(t, pool)

	// One attribute + one ts_kv/ts_kv_latest point per asset and device.
	// dbpkg caches key ids process-wide, so after the first test in this
	// package recreated key_dictionary the cache would point at rows that no
	// longer exist. Ask the cache first, then materialise exactly those ids —
	// keeps the dictionary and the production code path in agreement.
	powerKey := dbpkg.GetOrInsertKeyID("power")
	siteKey := dbpkg.GetOrInsertKeyID("site")
	if powerKey <= 0 || siteKey <= 0 {
		t.Fatalf("key_dictionary seed failed (power=%d site=%d)", powerKey, siteKey)
	}
	for _, kv := range []struct {
		id  int
		key string
	}{{powerKey, "power"}, {siteKey, "site"}} {
		if _, err := pool.Exec(
			`INSERT INTO key_dictionary (key_id, key) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
			kv.id, kv.key); err != nil {
			t.Fatalf("seed key_dictionary: %v", err)
		}
	}
	ts := time.Now().UnixMilli()
	for _, ent := range []struct {
		id, site string
		power    float64
	}{
		{wsAssetA, "siteA", 11},
		{wsAssetB, "siteB", 22},
		{wsDeviceA, "siteA", 11},
		{wsDeviceB, "siteB", 22},
	} {
		if _, err := pool.Exec(
			`INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, str_v, last_update_ts)
			 VALUES ($1, 2, $2, $3, $4)`, ent.id, siteKey, ent.site, ts); err != nil {
			t.Fatalf("seed attribute_kv: %v", err)
		}
		if _, err := pool.Exec(
			`INSERT INTO ts_kv (entity_id, key, ts, dbl_v) VALUES ($1,$2,$3,$4)`,
			ent.id, powerKey, ts, ent.power); err != nil {
			t.Fatalf("seed ts_kv: %v", err)
		}
		if _, err := pool.Exec(
			`INSERT INTO ts_kv_latest (entity_id, key, ts, dbl_v) VALUES ($1,$2,$3,$4)`,
			ent.id, powerKey, ts, ent.power); err != nil {
			t.Fatalf("seed ts_kv_latest: %v", err)
		}
	}

	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func scopeA() tenantScope { return tenantScope{TenantID: wsTenantA} }
func scopeB() tenantScope { return tenantScope{TenantID: wsTenantB} }

// TestTenantScopeAllows — the ownership primitive every WS read now goes
// through: own entity yes, foreign entity no, empty tenant nothing at all,
// unknown entity type fail-closed, SYS_ADMIN crosses.
func TestTenantScopeAllows(t *testing.T) {
	newWSTenantTestDB(t)

	cases := []struct {
		name       string
		scope      tenantScope
		entityType string
		entityID   string
		want       bool
	}{
		{"own asset", scopeA(), "ASSET", wsAssetA, true},
		{"own device", scopeA(), "DEVICE", wsDeviceA, true},
		{"foreign asset", scopeA(), "ASSET", wsAssetB, false},
		{"foreign device", scopeA(), "DEVICE", wsDeviceB, false},
		{"other side foreign", scopeB(), "ASSET", wsAssetA, false},
		{"empty tenant reads nothing", tenantScope{}, "ASSET", wsAssetA, false},
		{"unknown entity type", scopeA(), "WIDGET_TYPE", wsAssetA, false},
		{"missing entity", scopeA(), "ASSET", "33333333-3333-3333-3333-333333333333", false},
		{"tenant owns itself", scopeA(), "TENANT", wsTenantA, true},
		{"tenant is not another tenant", scopeA(), "TENANT", wsTenantB, false},
		{"sysadmin crosses", tenantScope{TenantID: wsSysTenant, SysAdmin: true}, "ASSET", wsAssetB, true},
		{"sysadmin without tenant still denied", tenantScope{SysAdmin: true}, "ASSET", wsAssetB, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.scope.allows(tc.entityType, tc.entityID); got != tc.want {
				t.Fatalf("allows(%s,%s) = %v, want %v", tc.entityType, tc.entityID, got, tc.want)
			}
		})
	}
}

// TestLoadEntityRowIsTenantScoped — relations-walk hydration must not return a
// foreign entity's name/label/type.
func TestLoadEntityRowIsTenantScoped(t *testing.T) {
	newWSTenantTestDB(t)

	if row := loadEntityRow(scopeA(), "ASSET", wsAssetA); row == nil || row["name"] != "assetA" {
		t.Fatalf("own asset row = %#v, want name=assetA", row)
	}
	if row := loadEntityRow(scopeA(), "ASSET", wsAssetB); row != nil {
		t.Fatalf("foreign asset row leaked: %#v", row)
	}
	if row := loadEntityRow(tenantScope{}, "ASSET", wsAssetA); row != nil {
		t.Fatalf("tenant-less session read a row: %#v", row)
	}
	if row := loadEntityRow(tenantScope{TenantID: wsSysTenant, SysAdmin: true}, "ASSET", wsAssetB); row == nil {
		t.Fatalf("SYS_ADMIN must be able to cross tenants")
	}
}

// TestFetchEntityAttributesIsTenantScoped — attribute_kv has no tenant column,
// so the read carries an EXISTS ownership predicate.
func TestFetchEntityAttributesIsTenantScoped(t *testing.T) {
	newWSTenantTestDB(t)

	own := fetchEntityAttributes(scopeA(), "ASSET", wsAssetA, []string{"site"})
	if len(own) != 1 {
		t.Fatalf("own attributes = %#v, want site", own)
	}
	if v := own["site"].(map[string]interface{})["value"]; v != "siteA" {
		t.Fatalf("own site = %#v, want siteA", v)
	}
	if got := fetchEntityAttributes(scopeA(), "ASSET", wsAssetB, []string{"site"}); len(got) != 0 {
		t.Fatalf("foreign attributes leaked: %#v", got)
	}
	if got := fetchEntityAttributes(tenantScope{}, "ASSET", wsAssetA, []string{"site"}); len(got) != 0 {
		t.Fatalf("tenant-less session read attributes: %#v", got)
	}
	// An entity type we cannot prove ownership for reads nothing.
	if got := fetchEntityAttributes(scopeA(), "WIDGET_TYPE", wsAssetA, []string{"site"}); len(got) != 0 {
		t.Fatalf("unknown entity type read attributes: %#v", got)
	}
}

// TestWalkRelationsQueryRejectsForeignRoot — the attacker-supplied rootId was
// never ownership-checked, so a session could walk another tenant's relation
// graph. Own root still walks (the drill-down dashboards depend on it).
func TestWalkRelationsQueryRejectsForeignRoot(t *testing.T) {
	newWSTenantTestDB(t)

	fields := []struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	}{{Type: "ENTITY_FIELD", Key: "name"}}
	rels := map[string]bool{"Contains": true}
	types := map[string]bool{"DEVICE": true}

	own := walkRelationsQuery(scopeA(), wsAssetA, "ASSET", "FROM", 2, rels, types, fields, []string{"site"})
	if len(own) != 1 {
		t.Fatalf("own walk returned %d items, want 1: %#v", len(own), own)
	}
	gotID := own[0]["entityId"].(map[string]interface{})["id"]
	if gotID != wsDeviceA {
		t.Fatalf("own walk hit = %v, want %s", gotID, wsDeviceA)
	}
	if attrs, ok := own[0]["latest"].(map[string]interface{})["ATTRIBUTE"]; !ok {
		t.Fatalf("own walk lost its ATTRIBUTE channel: %#v", own[0])
	} else if attrs.(map[string]interface{})["site"].(map[string]interface{})["value"] != "siteA" {
		t.Fatalf("own walk attribute = %#v", attrs)
	}

	if got := walkRelationsQuery(scopeA(), wsAssetB, "ASSET", "FROM", 2, rels, types, fields, []string{"site"}); len(got) != 0 {
		t.Fatalf("walk from foreign root leaked %d items: %#v", len(got), got)
	}

	// A cross-tenant edge must not open the graph behind it: topology.Neighbors
	// reads the edge tables without a tenant predicate, so the walker itself
	// refuses to report or traverse through a node it does not own.
	if _, err := dbpkg.Pool.Exec(
		`INSERT INTO topology_edge (tenant_id, from_id, from_type, to_id, to_type, relation_type)
		 VALUES ($1, $2, 'ASSET', $3, 'DEVICE', 'Contains')`,
		wsTenantA, wsAssetA, wsDeviceB); err != nil {
		t.Fatalf("seed cross-tenant edge: %v", err)
	}
	crossed := walkRelationsQuery(scopeA(), wsAssetA, "ASSET", "FROM", 2, rels, types, fields, nil)
	if len(crossed) != 1 {
		t.Fatalf("walk followed a cross-tenant edge: %#v", crossed)
	}
	if id := crossed[0]["entityId"].(map[string]interface{})["id"]; id != wsDeviceA {
		t.Fatalf("walk hit = %v, want only %s", id, wsDeviceA)
	}

	if got := walkRelationsQuery(tenantScope{}, wsAssetA, "ASSET", "FROM", 2, rels, types, fields, nil); len(got) != 0 {
		t.Fatalf("tenant-less walk leaked %d items", len(got))
	}
}

// TestWalkRelationsQueryBudget — the walk issues 1-3 queries per node, so it is
// bounded by a visited-node budget on top of the maxLevel clamp.
func TestWalkRelationsQueryBudget(t *testing.T) {
	newWSTenantTestDB(t)

	old := maxWalkNodes
	maxWalkNodes = 1 // root only: the budget trips before expanding it
	t.Cleanup(func() { maxWalkNodes = old })

	fields := []struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	}{{Type: "ENTITY_FIELD", Key: "name"}}
	got := walkRelationsQuery(scopeA(), wsAssetA, "ASSET", "FROM", 10,
		map[string]bool{"Contains": true}, map[string]bool{"DEVICE": true}, fields, nil)
	if len(got) != 0 {
		t.Fatalf("budget not enforced: walked %d items", len(got))
	}
}

// TestFilterOwnedSubCmds — a subscription is a standing grant: the broadcasters
// fan out by entityId alone, so a sub on a foreign entity would keep receiving
// that tenant's live pushes. Foreign subs are dropped before registration.
func TestFilterOwnedSubCmds(t *testing.T) {
	newWSTenantTestDB(t)

	cmds := []WsCmd{
		{EntityId: wsDeviceA, EntityType: "DEVICE", CmdId: 1},
		{EntityId: wsDeviceB, EntityType: "DEVICE", CmdId: 2},
		{EntityId: wsAssetB, EntityType: "ASSET", CmdId: 3},
		{EntityId: wsDeviceA, CmdId: 4},                    // no entityType ⇒ DEVICE
		{EntityId: wsDeviceB, CmdId: 5},                    // no entityType ⇒ DEVICE, foreign
		{EntityId: wsDeviceB, CmdId: 6, Unsubscribe: true}, // unsubscribes always pass
		{EntityId: "", CmdId: 7},                           // caller skips these later
	}
	got := filterOwnedSubCmds(scopeA(), append([]WsCmd(nil), cmds...), "telemetry")
	var ids []int
	for _, c := range got {
		ids = append(ids, c.CmdId)
	}
	want := []int{1, 4, 6, 7}
	if len(ids) != len(want) {
		t.Fatalf("kept cmdIds = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("kept cmdIds = %v, want %v", ids, want)
		}
	}

	if got := filterOwnedSubCmds(tenantScope{}, append([]WsCmd(nil), cmds...), "telemetry"); len(got) != 2 {
		// only the unsubscribe and the empty-entity command survive
		t.Fatalf("tenant-less session kept %d subscribe cmds", len(got))
	}
}

// --- end-to-end WS handler tests -------------------------------------------
//
// These drive the real HandleWebSocket over a real socket so the assertions
// cover the handler wiring (ownership gate → EntityCmdMap registration →
// ts_kv_latest / ts_kv reads), not just the helpers.

func wsToken(t *testing.T, tenantID, authority string) string {
	t.Helper()
	authpkg.InitConfig()
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

// dialWS opens an authenticated session for the given tenant.
func dialWS(t *testing.T, tenantID string) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(HandleWebSocket))
	t.Cleanup(srv.Close)

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	if err := conn.WriteJSON(map[string]interface{}{
		"authCmd": map[string]interface{}{"cmdId": 0, "token": wsToken(t, tenantID, "TENANT_ADMIN")},
	}); err != nil {
		t.Fatalf("authCmd: %v", err)
	}
	return conn
}

func readJSON(t *testing.T, conn *websocket.Conn) map[string]interface{} {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var msg map[string]interface{}
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatalf("read: %v", err)
	}
	return msg
}

func entityDataCmd(cmdID int, entityType, entityID string) map[string]interface{} {
	return map[string]interface{}{
		"cmds": []map[string]interface{}{{
			"type":  "ENTITY_DATA",
			"cmdId": cmdID,
			"query": map[string]interface{}{
				"entityFilter": map[string]interface{}{
					"type":         "singleEntity",
					"singleEntity": map[string]interface{}{"id": entityID, "entityType": entityType},
				},
				"pageLink":     map[string]interface{}{"pageSize": 10, "page": 0},
				"entityFields": []map[string]interface{}{{"type": "ENTITY_FIELD", "key": "name"}},
				"latestValues": []map[string]interface{}{{"type": "TIME_SERIES", "key": "power"}},
			},
			"historyCmd": map[string]interface{}{
				"keys":    []string{"power"},
				"startTs": 0,
				"endTs":   time.Now().Add(time.Hour).UnixMilli(),
				"limit":   100,
			},
		}},
	}
}

// TestWebSocketEntityDataCrossTenant — tenant A asking for tenant B's ASSET
// gets an empty page (no ts_kv_latest value, no ts_kv history), while the same
// command against its own ASSET still returns both. This is the ENTITY_DATA
// half of the IDOR: both reads are keyed by entity_id on tables with no tenant
// column.
func TestWebSocketEntityDataCrossTenant(t *testing.T) {
	newWSTenantTestDB(t)
	conn := dialWS(t, wsTenantA)

	// Foreign entity → denied, empty page.
	if err := conn.WriteJSON(entityDataCmd(1, "ASSET", wsAssetB)); err != nil {
		t.Fatalf("write: %v", err)
	}
	msg := readJSON(t, conn)
	data, _ := msg["data"].(map[string]interface{})
	if data == nil {
		t.Fatalf("no data envelope in %#v", msg)
	}
	if n, _ := data["totalElements"].(float64); n != 0 {
		t.Fatalf("cross-tenant ENTITY_DATA returned %v elements: %#v", n, msg)
	}
	if items, _ := data["data"].([]interface{}); len(items) != 0 {
		t.Fatalf("cross-tenant ENTITY_DATA leaked rows: %#v", items)
	}
	raw, _ := json.Marshal(msg)
	if strings.Contains(string(raw), "22.0") || strings.Contains(string(raw), "\"22\"") {
		t.Fatalf("tenant B value leaked in payload: %s", raw)
	}

	// Own entity → still served (no regression for the real dashboard).
	if err := conn.WriteJSON(entityDataCmd(2, "ASSET", wsAssetA)); err != nil {
		t.Fatalf("write: %v", err)
	}
	msg = readJSON(t, conn)
	update, _ := msg["update"].([]interface{})
	if len(update) != 1 {
		t.Fatalf("own ENTITY_DATA update = %#v", msg)
	}
	entity := update[0].(map[string]interface{})
	latest := entity["latest"].(map[string]interface{})
	tsLatest, ok := latest["TIME_SERIES"].(map[string]interface{})
	if !ok {
		t.Fatalf("own ENTITY_DATA lost TIME_SERIES latest: %#v", latest)
	}
	if v := tsLatest["power"].(map[string]interface{})["value"]; v != 11.0 {
		t.Fatalf("own ts_kv_latest value = %#v, want 11", v)
	}
	series, ok := entity["timeseries"].(map[string]interface{})["power"].([]interface{})
	if !ok || len(series) == 0 {
		t.Fatalf("own ts_kv history empty: %#v", entity["timeseries"])
	}
}

// TestWebSocketAttributeSubCrossTenant — attrSubCmds registers a standing
// subscription and fires sendInitialAttributes. A sub on tenant B's device must
// never be registered nor answered; the sub on tenant A's device still is.
func TestWebSocketAttributeSubCrossTenant(t *testing.T) {
	newWSTenantTestDB(t)
	conn := dialWS(t, wsTenantA)

	if err := conn.WriteJSON(map[string]interface{}{
		"attrSubCmds": []map[string]interface{}{
			{"entityId": wsDeviceB, "entityType": "DEVICE", "cmdId": 10, "keys": "site", "scope": "SERVER_SCOPE"},
			{"entityId": wsDeviceA, "entityType": "DEVICE", "cmdId": 11, "keys": "site", "scope": "SERVER_SCOPE"},
		},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Exactly one initial-attributes payload must arrive: the owned one.
	msg := readJSON(t, conn)
	if id, _ := msg["subscriptionId"].(float64); id != 11 {
		t.Fatalf("unexpected first payload (cross-tenant sub answered?): %#v", msg)
	}
	data, _ := msg["data"].(map[string]interface{})
	if _, ok := data["site"]; !ok {
		t.Fatalf("own attribute sub returned no data: %#v", msg)
	}

	// A LIVE push for the rejected entity must deliver nothing: the foreign
	// sub was never registered, and BroadcastAttributes fans out by entityId
	// with no tenant check of its own (see its INVARIANT comment) — the
	// registration gate is the only defense. Ordering proves silence: the
	// owned-entity push that follows must be the next frame on the wire.
	BroadcastAttributes(wsDeviceB, "SERVER_SCOPE", map[string]interface{}{"site": "leak-b"})
	BroadcastAttributes(wsDeviceA, "SERVER_SCOPE", map[string]interface{}{"site": "live-a"})
	live := readJSON(t, conn)
	if id, _ := live["subscriptionId"].(float64); id != 11 {
		t.Fatalf("live frame went to sub %v (%#v) — rejected cross-tenant sub received a push?", id, live)
	}
	if got := legacyAttrValue(t, live, "site"); got != "live-a" {
		t.Fatalf("live push value = %q, want \"live-a\" (and never tenant B's)", got)
	}

	_ = conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	var extra map[string]interface{}
	if err := conn.ReadJSON(&extra); err == nil {
		t.Fatalf("cross-tenant attribute sub was answered: %#v", extra)
	}
}
