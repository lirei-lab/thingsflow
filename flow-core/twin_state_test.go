package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
	"flow-core/internal/tenant"
	"flow-core/internal/twinstore"
)

func stateWith(tenantID, entityID string, telem map[string]twinstore.Value, attrs map[string]map[string]twinstore.Value) twinstore.State {
	return twinstore.State{
		TenantID:   tenantID,
		EntityType: "DEVICE",
		EntityID:   entityID,
		Telemetry:  telem,
		Attributes: attrs,
	}
}

func TestChangedTelemetry(t *testing.T) {
	old := stateWith("t1", "d1", map[string]twinstore.Value{
		"temp":  {TS: 1000, Value: 21.5},
		"power": {TS: 900, Value: 5.0},
	}, nil)
	next := stateWith("t1", "d1", map[string]twinstore.Value{
		"temp":  {TS: 1000, Value: 21.5}, // unchanged (same ts+value) → skipped
		"power": {TS: 1100, Value: 6.0},  // moved → included
		"co2":   {TS: 1050, Value: 400},  // new key → included
	}, nil)

	changed, ts := changedTelemetry(old, next)
	if len(changed) != 2 || changed["power"] != 6.0 || changed["co2"] != 400 {
		t.Fatalf("changed = %#v, want power+co2 only", changed)
	}
	if ts != 1100 {
		t.Fatalf("ts = %d, want max changed ts 1100", ts)
	}
}

func TestChangedAttributes(t *testing.T) {
	old := stateWith("t1", "d1", nil, map[string]map[string]twinstore.Value{
		"SERVER_SCOPE": {
			"site":  {TS: 1000, Value: "hq"},
			"floor": {TS: 1000, Value: 2.0},
		},
	})
	next := stateWith("t1", "d1", nil, map[string]map[string]twinstore.Value{
		"SERVER_SCOPE": {
			"site":  {TS: 1000, Value: "hq"},    // unchanged → skipped
			"floor": {TS: 1200, Value: 3.0},     // moved → included
			"zone":  {TS: 1100, Value: "north"}, // new key → included
		},
		"SHARED_SCOPE": { // new scope → included
			"config": {TS: 1150, Value: "v2"},
		},
	})

	changed := changedAttributes(old, next)
	server := changed["SERVER_SCOPE"]
	if len(server) != 2 || server["floor"] != 3.0 || server["zone"] != "north" {
		t.Fatalf("SERVER_SCOPE diff = %#v, want floor+zone only", server)
	}
	if changed["SHARED_SCOPE"]["config"] != "v2" {
		t.Fatalf("SHARED_SCOPE diff = %#v, want config", changed["SHARED_SCOPE"])
	}
}

func TestChangedAttributesFromEmptyPreviousState(t *testing.T) {
	next := stateWith("t1", "d1", nil, map[string]map[string]twinstore.Value{
		"CLIENT_SCOPE": {"cfg": {TS: 10, Value: true}},
	})
	changed := changedAttributes(twinstore.State{}, next)
	if changed["CLIENT_SCOPE"]["cfg"] != true {
		t.Fatalf("diff from empty state = %#v, want CLIENT_SCOPE.cfg", changed)
	}
}

// TestApplyTwinChangeAntiStorm pins the merge-not-replace fold of the watch
// loop. The data plane writes per-key synthesized states (one telemetry key,
// EMPTY attributes — twinstore/nats.go Watch fallback). If such an event
// replaced the last-known state, the next full-document write would re-diff
// every attribute against nothing and re-broadcast the whole set — a storm at
// data-plane write rate (~thousands/s).
func TestApplyTwinChangeAntiStorm(t *testing.T) {
	last := map[string]twinstore.State{}
	attrs := map[string]map[string]twinstore.Value{
		"SERVER_SCOPE": {"site": {TS: 1000, Value: "hq"}},
	}

	// 1. Full document: first sight of the attribute → broadcast expected.
	fullDoc := stateWith("t1", "d1", map[string]twinstore.Value{"temp": {TS: 1000, Value: 21.5}}, attrs)
	_, _, changedAttrs := applyTwinChange(last, twinstore.Change{New: fullDoc})
	if changedAttrs["SERVER_SCOPE"]["site"] != "hq" {
		t.Fatalf("first full doc must broadcast attributes, got %#v", changedAttrs)
	}

	// 2. Per-key data-plane event: one telemetry key, no attributes.
	perKey := stateWith("t1", "d1", map[string]twinstore.Value{"power": {TS: 2000, Value: 5.0}}, nil)
	changedTelem, _, changedAttrs := applyTwinChange(last, twinstore.Change{New: perKey})
	if len(changedTelem) != 1 || changedTelem["power"] != 5.0 {
		t.Fatalf("per-key event telemetry diff = %#v, want power only", changedTelem)
	}
	if len(changedAttrs) != 0 {
		t.Fatalf("per-key event must not produce attribute diffs, got %#v", changedAttrs)
	}

	// 3. Full document again, same attributes, newer telemetry: the attribute
	// set must NOT be re-broadcast (the per-key event in step 2 must not have
	// wiped last.Attributes) and unchanged telemetry keys must stay silent.
	fullDoc2 := stateWith("t1", "d1", map[string]twinstore.Value{
		"temp":  {TS: 3000, Value: 22.0},
		"power": {TS: 2000, Value: 5.0},
	}, attrs)
	changedTelem, _, changedAttrs = applyTwinChange(last, twinstore.Change{New: fullDoc2})
	if len(changedAttrs) != 0 {
		t.Fatalf("anti-storm violated: unchanged attributes re-broadcast after per-key event: %#v", changedAttrs)
	}
	if len(changedTelem) != 1 || changedTelem["temp"] != 22.0 {
		t.Fatalf("full-doc telemetry diff = %#v, want temp only", changedTelem)
	}
}

type attrPush struct {
	entityID string
	scope    string
	data     map[string]interface{}
}

// TestAttributeRestWriteProducesExactlyOnePush is the single-publisher
// regression test: a REST attribute write must reach WS subscribers exactly
// once, via the KV watch. Before milestone 3 phase 1 the tenant handler ALSO
// broadcast directly (tenant.Broadcaster, wired in main.go), so with the watch
// diffusing attributes every REST write became two frames per subscriber.
func TestAttributeRestWriteProducesExactlyOnePush(t *testing.T) {
	store := twinstore.NewMemoryStore()
	twinstore.SetGlobal(store)
	t.Cleanup(func() { twinstore.SetGlobal(nil) })

	pushes := make(chan attrPush, 8)
	record := func(entityID, scope string, data map[string]interface{}) {
		pushes <- attrPush{entityID: entityID, scope: scope, data: data}
	}

	prevBA := broadcastAttributes
	broadcastAttributes = record
	t.Cleanup(func() { broadcastAttributes = prevBA })
	prevBT := broadcastTelemetry
	broadcastTelemetry = func(string, map[string]interface{}, int64) {}
	t.Cleanup(func() { broadcastTelemetry = prevBT })

	// Mirror the main.go boot wiring: if the handler still called its direct
	// Broadcaster hook, this test would observe a second push.
	prevHook := tenant.Broadcaster
	tenant.Broadcaster = func(entityID, scope string, data map[string]interface{}) {
		record(entityID, scope, data)
	}
	t.Cleanup(func() { tenant.Broadcaster = prevHook })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// Subscribe synchronously so the write below cannot race the watch
	// registration, then run the loop under test.
	changes, err := store.Watch(ctx, "")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	go watchTwinStateChanges(ctx, changes)

	authpkg.InitConfig()
	token, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-00000000000a",
		Email:     "u@x.org",
		Authority: "TENANT_ADMIN",
		TenantID:  "11111111-1111-1111-1111-111111111111",
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}

	const deviceID = "22222222-2222-2222-2222-222222222226"
	req := httptest.NewRequest("POST",
		"/api/plugins/telemetry/DEVICE/"+deviceID+"/SHARED_SCOPE",
		strings.NewReader(`{"config":"v1"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	// dbpkg.Pool is nil here: saveAttributeKV no-ops (GetOrInsertKeyID
	// returns -1), which is fine — the push under test rides the twin store.
	tenant.HandleAttributeRest(w, req)
	if w.Code != 200 {
		t.Fatalf("attribute POST status = %d body=%s", w.Code, w.Body.String())
	}

	var first attrPush
	select {
	case first = <-pushes:
	case <-time.After(2 * time.Second):
		t.Fatal("no attribute push arrived — the KV watch is not publishing")
	}
	if first.entityID != deviceID || first.scope != "SHARED_SCOPE" || first.data["config"] != "v1" {
		t.Fatalf("push = %#v, want SHARED_SCOPE config=v1 for %s", first, deviceID)
	}

	select {
	case second := <-pushes:
		t.Fatalf("second push for a single write (double publisher): %#v", second)
	case <-time.After(300 * time.Millisecond):
		// exactly one push — single publisher holds
	}
}
