package transport

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dbpkg "flow-core/internal/db"
)

// swapFetchAttributes replaces the FetchAttributes hook for one test and
// restores the previous value afterwards (the hook is normally wired by
// main.go, so it is nil inside this package's tests).
func swapFetchAttributes(t *testing.T, fn func(string, int, []string, map[string]interface{})) {
	t.Helper()
	prev := FetchAttributes
	FetchAttributes = fn
	t.Cleanup(func() { FetchAttributes = prev })
}

// TestAttributesGetMapsQueryParamsToCanonicalScopes pins the query-param →
// attribute_type mapping of the device-facing GET. The canonical encoding is
// CLIENT_SCOPE=0, SHARED_SCOPE=1, SERVER_SCOPE=2 (tenant_handler.go:620-626);
// ?sharedKeys= historically read SERVER_SCOPE (2) instead, which made shared
// attributes written via the UI invisible to devices.
func TestAttributesGetMapsQueryParamsToCanonicalScopes(t *testing.T) {
	calls := map[int][]string{}
	swapFetchAttributes(t, func(deviceID string, attrType int, keys []string, dest map[string]interface{}) {
		calls[attrType] = keys
	})

	req := httptest.NewRequest("GET", "/api/v1/tok/attributes?clientKeys=cfg&sharedKeys=site,limit", nil)
	w := httptest.NewRecorder()
	handleHttpAttributesGet(w, req, "22222222-2222-2222-2222-222222222224")

	if got, ok := calls[0]; !ok || len(got) != 1 || got[0] != "cfg" {
		t.Fatalf("clientKeys fetched with attrType/keys = %v, want attrType 0 keys [cfg]", calls)
	}
	if got, ok := calls[1]; !ok || len(got) != 2 || got[0] != "site" || got[1] != "limit" {
		t.Fatalf("sharedKeys fetched with attrType/keys = %v, want attrType 1 (SHARED_SCOPE) keys [site limit]", calls)
	}
	if _, leaked := calls[2]; leaked {
		t.Fatalf("a fetch went to attrType 2 (SERVER_SCOPE): %v — sharedKeys must read SHARED_SCOPE", calls)
	}
}

// setupAttributeTables adds the attribute slice of the schema on top of
// setupTransportTables, for the shared-attribute round trip.
func setupAttributeTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS attribute_kv CASCADE`,
		`DROP TABLE IF EXISTS key_dictionary CASCADE`,
		`CREATE TABLE key_dictionary (key_id serial PRIMARY KEY, key text UNIQUE)`,
		`CREATE TABLE attribute_kv (entity_id uuid, attribute_type int, attribute_key int,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v json,
			last_update_ts bigint,
			CONSTRAINT attribute_kv_pkey PRIMARY KEY (entity_id, attribute_type, attribute_key))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
}

// dbFetchAttributes is a DB-backed FetchAttributes for the round trip. The
// production implementation (fetchAttributes, flow-core/postgres.go) lives in
// package main and cannot be imported from here, so this mirrors its canonical
// read — the point of the round trip is that the handler's attrType argument
// reaches real SQL against a really seeded attribute_type=1 row, not a fake.
func dbFetchAttributes(deviceID string, attrType int, keys []string, dest map[string]interface{}) {
	if len(keys) == 0 || dbpkg.Pool == nil {
		return
	}
	placeholders := make([]string, len(keys))
	args := []interface{}{deviceID, attrType}
	for i, key := range keys {
		placeholders[i] = fmt.Sprintf("$%d", i+3)
		args = append(args, key)
	}
	rows, err := dbpkg.Pool.Query(`
		SELECT k.key, a.str_v FROM attribute_kv a
		  JOIN key_dictionary k ON a.attribute_key = k.key_id
		 WHERE a.entity_id = $1 AND a.attribute_type = $2
		   AND k.key IN (`+strings.Join(placeholders, ",")+`)`, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var strV *string
		if err := rows.Scan(&key, &strV); err != nil {
			continue
		}
		if strV != nil {
			dest[key] = *strV
		}
	}
}

// TestAttributesGetSharedKeysRoundTrip is the ROADMAP criterion as written: a
// SHARED_SCOPE attribute (attribute_type=1, the scope the UI POST
// .../SHARED_SCOPE writes) seeded in attribute_kv must come back from
// GET /api/v1/{token}/attributes?sharedKeys=. Before the fix the handler read
// attribute_type 2 and this returned nothing.
func TestAttributesGetSharedKeysRoundTrip(t *testing.T) {
	db := newTestDB(t)
	setupTransportTables(t, db)
	setupAttributeTables(t, db)

	const tenantID = "11111111-1111-1111-1111-111111111111"
	const deviceID = "22222222-2222-2222-2222-222222222225"
	const profileID = "33333333-3333-3333-3333-333333333333"
	seedDeviceWithToken(t, db, tenantID, deviceID, profileID, "SharedRoundTripToken1")

	var keyID int
	if err := db.QueryRow(
		`INSERT INTO key_dictionary (key) VALUES ('site') RETURNING key_id`).Scan(&keyID); err != nil {
		t.Fatalf("seed key_dictionary: %v", err)
	}
	// attribute_type = 1 — SHARED_SCOPE per the canonical mapping.
	if _, err := db.Exec(`INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, str_v, last_update_ts)
		VALUES ($1, 1, $2, 'hq', $3)`, deviceID, keyID, time.Now().UnixMilli()); err != nil {
		t.Fatalf("seed attribute_kv: %v", err)
	}

	swapFetchAttributes(t, dbFetchAttributes)

	req := httptest.NewRequest("GET", "/api/v1/SharedRoundTripToken1/attributes?sharedKeys=site", nil)
	w := httptest.NewRecorder()
	Handle(w, req)

	if w.Code != 200 {
		t.Fatalf("GET attributes status = %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Shared map[string]interface{} `json:"shared"`
		Client map[string]interface{} `json:"client"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (%s)", err, w.Body.String())
	}
	if got := resp.Shared["site"]; got != "hq" {
		t.Fatalf("shared.site = %#v, want \"hq\" — device cannot read the SHARED_SCOPE attribute", got)
	}
}
