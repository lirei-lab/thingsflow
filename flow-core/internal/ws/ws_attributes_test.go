package ws

// Live attribute-push coverage (milestone 3 phase 1): the KV watch is the
// single attribute publisher and BroadcastAttributes now serves BOTH protocol
// generations. These tests pin the two behaviours the pre-execution critique
// flagged as silent no-ops/crashes:
//   - the ENTITY_DATA channel is the type the subscription DECLARED
//     (SHARED_SCOPE here, not always "ATTRIBUTE"), and keys without an
//     attribute-type registration are never pushed (TB UI v4 crashes on
//     unregistered `${name}_${type}` pairs);
//   - legacy attrSubCmds are filtered by their declared scope, mirroring
//     sendInitialAttributes' hydration semantics.
// Negative cases are asserted by ordering (a silent broadcast followed by a
// pushing one — frames are FIFO per connection) instead of read timeouts,
// because a timed-out read poisons a gorilla connection for later reads.

import (
	"testing"

	"github.com/gorilla/websocket"
)

func writeEntityDataAttrSub(t *testing.T, conn *websocket.Conn, cmdID int, entityID string) {
	t.Helper()
	if err := conn.WriteJSON(map[string]interface{}{
		"cmds": []map[string]interface{}{{
			"type":  "ENTITY_DATA",
			"cmdId": cmdID,
			"query": map[string]interface{}{
				"entityFilter": map[string]interface{}{
					"type":         "singleEntity",
					"singleEntity": map[string]interface{}{"id": entityID, "entityType": "DEVICE"},
				},
				"pageLink":     map[string]interface{}{"pageSize": 10, "page": 0},
				"entityFields": []map[string]interface{}{{"type": "ENTITY_FIELD", "key": "name"}},
				"latestValues": []map[string]interface{}{
					{"type": "SHARED_SCOPE", "key": "config"},
					{"type": "TIME_SERIES", "key": "power"},
				},
			},
		}},
	}); err != nil {
		t.Fatalf("write ENTITY_DATA cmd: %v", err)
	}
}

// TestBroadcastAttributesEntityDataTypedChannel — a subscription that declared
// `config` on the SHARED_SCOPE channel gets pushes for that key ONLY from
// SHARED_SCOPE broadcasts, in that exact channel, and a key registered only as
// TIME_SERIES (`power`) is never pushed as an attribute.
func TestBroadcastAttributesEntityDataTypedChannel(t *testing.T) {
	newWSTenantTestDB(t)
	conn := dialWS(t, wsTenantA)

	writeEntityDataAttrSub(t, conn, 31, wsDeviceA)
	initial := readJSON(t, conn)
	if initial["cmdUpdateType"] != "ENTITY_DATA" {
		t.Fatalf("unexpected initial frame: %#v", initial)
	}

	// Silent by design: wrong scope for the declared channel, then an
	// attribute push for a TIME_SERIES-registered key. Neither may emit.
	BroadcastAttributes(wsDeviceA, "SERVER_SCOPE", map[string]interface{}{"config": "wrong-scope"})
	BroadcastAttributes(wsDeviceA, "SHARED_SCOPE", map[string]interface{}{"power": 5.0})
	// This one must emit — and must be the NEXT frame on the wire, which
	// proves the two calls above emitted nothing.
	BroadcastAttributes(wsDeviceA, "SHARED_SCOPE", map[string]interface{}{"config": "v1"})

	msg := readJSON(t, conn)
	if msg["cmdUpdateType"] != "ENTITY_DATA" || msg["cmdId"].(float64) != 31 {
		t.Fatalf("push frame = %#v, want ENTITY_DATA update for cmdId 31", msg)
	}
	update, _ := msg["update"].([]interface{})
	if len(update) != 1 {
		t.Fatalf("update = %#v, want exactly one entity", msg["update"])
	}
	entity := update[0].(map[string]interface{})
	if id := entity["entityId"].(map[string]interface{}); id["id"] != wsDeviceA || id["entityType"] != "DEVICE" {
		t.Fatalf("entityId = %#v", id)
	}
	latest := entity["latest"].(map[string]interface{})
	shared, ok := latest["SHARED_SCOPE"].(map[string]interface{})
	if !ok {
		t.Fatalf("latest = %#v, want the SHARED_SCOPE channel (the DECLARED type, not ATTRIBUTE)", latest)
	}
	if _, leaked := latest["ATTRIBUTE"]; leaked {
		t.Fatalf("push landed in ATTRIBUTE as well as the declared channel: %#v", latest)
	}
	cfg, ok := shared["config"].(map[string]interface{})
	if !ok || cfg["value"] != "v1" {
		t.Fatalf("SHARED_SCOPE.config = %#v, want native value \"v1\"", shared["config"])
	}
	if raw, _ := cfg["ts"].(float64); raw <= 0 {
		t.Fatalf("SHARED_SCOPE.config.ts = %#v, want a real timestamp", cfg["ts"])
	}
	if _, leaked := shared["power"]; leaked {
		t.Fatalf("TIME_SERIES-registered key pushed as attribute (UI-crashing pair): %#v", shared)
	}
}

// TestBroadcastAttributesLegacyScopeFilter — a legacy attrSubCmd that declared
// SERVER_SCOPE no longer receives other scopes, while an ANY_SCOPE sub keeps
// receiving everything (exactly the hydration semantics of
// sendInitialAttributes).
func TestBroadcastAttributesLegacyScopeFilter(t *testing.T) {
	newWSTenantTestDB(t)
	conn := dialWS(t, wsTenantA)

	if err := conn.WriteJSON(map[string]interface{}{
		"attrSubCmds": []map[string]interface{}{
			{"entityId": wsDeviceA, "entityType": "DEVICE", "cmdId": 21, "keys": "site", "scope": "SERVER_SCOPE"},
			{"entityId": wsDeviceA, "entityType": "DEVICE", "cmdId": 22, "keys": "site", "scope": "ANY_SCOPE"},
		},
	}); err != nil {
		t.Fatalf("write attrSubCmds: %v", err)
	}
	// Two initial payloads, one per sub, in goroutine order (not
	// deterministic) — drain both before broadcasting.
	seen := map[float64]bool{}
	for i := 0; i < 2; i++ {
		msg := readJSON(t, conn)
		id, _ := msg["subscriptionId"].(float64)
		seen[id] = true
	}
	if !seen[21] || !seen[22] {
		t.Fatalf("initial payloads = %v, want subs 21 and 22", seen)
	}

	// SHARED_SCOPE broadcast: only the ANY_SCOPE sub (22) may push.
	BroadcastAttributes(wsDeviceA, "SHARED_SCOPE", map[string]interface{}{"site": "leak"})
	// SERVER_SCOPE broadcast: both subs push, in subs-slice order 21 → 22.
	BroadcastAttributes(wsDeviceA, "SERVER_SCOPE", map[string]interface{}{"site": "hq2"})

	frame1 := readJSON(t, conn)
	if id, _ := frame1["subscriptionId"].(float64); id != 22 {
		t.Fatalf("first live frame went to sub %v (%#v) — SERVER_SCOPE sub received a SHARED_SCOPE push?", id, frame1)
	}
	if got := legacyAttrValue(t, frame1, "site"); got != "leak" {
		t.Fatalf("ANY_SCOPE sub got %q for the SHARED_SCOPE broadcast, want \"leak\"", got)
	}

	frame2 := readJSON(t, conn)
	if id, _ := frame2["subscriptionId"].(float64); id != 21 {
		t.Fatalf("second live frame = %#v, want the SERVER_SCOPE sub (21)", frame2)
	}
	if got := legacyAttrValue(t, frame2, "site"); got != "hq2" {
		t.Fatalf("SERVER_SCOPE sub got %q, want \"hq2\" — and never the SHARED_SCOPE value", got)
	}

	frame3 := readJSON(t, conn)
	if id, _ := frame3["subscriptionId"].(float64); id != 22 {
		t.Fatalf("third live frame = %#v, want the ANY_SCOPE sub (22)", frame3)
	}
	if got := legacyAttrValue(t, frame3, "site"); got != "hq2" {
		t.Fatalf("ANY_SCOPE sub got %q for the SERVER_SCOPE broadcast, want \"hq2\"", got)
	}
}

// legacyAttrValue digs the stringified value out of a legacy attribute frame
// ({"subscriptionId": n, "data": {key: [[ts, "value"]]}}).
func legacyAttrValue(t *testing.T, msg map[string]interface{}, key string) string {
	t.Helper()
	data, _ := msg["data"].(map[string]interface{})
	pairs, _ := data[key].([]interface{})
	if len(pairs) == 0 {
		t.Fatalf("frame %#v carries no %q pairs", msg, key)
	}
	pair, _ := pairs[0].([]interface{})
	if len(pair) != 2 {
		t.Fatalf("frame %#v pair shape = %#v", msg, pairs[0])
	}
	s, _ := pair[1].(string)
	return s
}
