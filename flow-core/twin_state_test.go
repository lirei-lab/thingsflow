package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestApplyTwinChangePrefersLastOverEventOld pins the diff base: what the
// watch loop LAST OBSERVED wins over the event-carried Old. MemoryStore.notify
// drops changes when a watch buffer is full, so an event's Old can describe a
// state we never diffed against; trusting it would silently suppress the keys
// whose moves rode the dropped event. change.Old is used only for the very
// first event of an entity.
func TestApplyTwinChangePrefersLastOverEventOld(t *testing.T) {
	last := map[string]twinstore.State{}
	first := stateWith("t1", "d1", map[string]twinstore.Value{"temp": {TS: 1000, Value: 21.5}}, nil)
	if changed, _, _ := applyTwinChange(last, twinstore.Change{New: first}); changed["temp"] != 21.5 {
		t.Fatalf("first event diff = %#v, want temp broadcast", changed)
	}

	// The event's Old already claims temp=23.0@2000 (i.e. it is the state
	// AFTER a change we never received). If applyTwinChange trusted it, the
	// move 21.5→23.0 would produce an empty diff and never reach WS.
	moved := stateWith("t1", "d1", map[string]twinstore.Value{"temp": {TS: 2000, Value: 23.0}}, nil)
	changed, ts, _ := applyTwinChange(last, twinstore.Change{Old: moved, New: moved})
	if changed["temp"] != 23.0 || ts != 2000 {
		t.Fatalf("diff = %#v ts=%d, want temp=23.0@2000 (last-observed state must be the diff base)", changed, ts)
	}
}

// TestApplyTwinChangeBoundsLastMap — the `last` map obeys TWIN_WATCH_LAST_MAX
// ("no store grows unbounded"). Eviction is harmless: the next event for an
// evicted entity just re-broadcasts once.
func TestApplyTwinChangeBoundsLastMap(t *testing.T) {
	prev := twinWatchLastMax
	twinWatchLastMax = 2
	t.Cleanup(func() { twinWatchLastMax = prev })

	last := map[string]twinstore.State{}
	for i := 0; i < 5; i++ {
		doc := stateWith("t1", fmt.Sprintf("d%d", i), map[string]twinstore.Value{"temp": {TS: 1000, Value: float64(i)}}, nil)
		applyTwinChange(last, twinstore.Change{New: doc})
		if len(last) > 2 {
			t.Fatalf("last map grew to %d entries, cap is 2", len(last))
		}
	}
	// Broadcasts still work after eviction: a fresh event for a (possibly
	// evicted) entity still produces a diff.
	doc := stateWith("t1", "d0", map[string]twinstore.Value{"temp": {TS: 2000, Value: 99.0}}, nil)
	if changed, _, _ := applyTwinChange(last, twinstore.Change{New: doc}); changed["temp"] != 99.0 {
		t.Fatalf("post-eviction diff = %#v, want temp=99", changed)
	}
}

// TestMergeLastStateClonesFirstEvent — the first event for an entity must be
// CLONED into `last`, not adopted: adopting next's maps lets the fold mutate
// state the store (or another holder of the Change) still aliases.
func TestMergeLastStateClonesFirstEvent(t *testing.T) {
	next := stateWith("t1", "d1", map[string]twinstore.Value{"temp": {TS: 1000, Value: 21.5}},
		map[string]map[string]twinstore.Value{"SERVER_SCOPE": {"site": {TS: 1000, Value: "hq"}}})
	merged := mergeLastState(twinstore.State{}, next)

	// Mutate through the merged state; the original event must be untouched.
	merged.Telemetry["temp"] = twinstore.Value{TS: 2000, Value: 99.0}
	merged.Attributes["SERVER_SCOPE"]["site"] = twinstore.Value{TS: 2000, Value: "poisoned"}
	if next.Telemetry["temp"].Value != 21.5 {
		t.Fatalf("first-event fold aliased the event's telemetry map: %#v", next.Telemetry)
	}
	if next.Attributes["SERVER_SCOPE"]["site"].Value != "hq" {
		t.Fatalf("first-event fold aliased the event's attribute maps: %#v", next.Attributes)
	}
}

type attrPush struct {
	entityID string
	scope    string
	data     map[string]interface{}
}

// swapBroadcastSeams routes the watch loop's outputs into a channel for the
// duration of one test.
func swapBroadcastSeams(t *testing.T) chan attrPush {
	t.Helper()
	pushes := make(chan attrPush, 16)
	prevBA := broadcastAttributes
	broadcastAttributes = func(entityID, scope string, data map[string]interface{}) {
		pushes <- attrPush{entityID: entityID, scope: scope, data: data}
	}
	t.Cleanup(func() { broadcastAttributes = prevBA })
	prevBT := broadcastTelemetry
	broadcastTelemetry = func(string, map[string]interface{}, int64) {}
	t.Cleanup(func() { broadcastTelemetry = prevBT })
	return pushes
}

// TestAttributeRestWriteProducesExactlyOnePush is the single-publisher
// regression test: a REST attribute write must reach WS subscribers exactly
// once, via the KV watch. Before milestone 3 phase 1 the tenant handler ALSO
// broadcast directly; that hook (tenant.Broadcaster) has since been deleted
// outright, so the property is now enforced structurally — internal/tenant has
// no WS dependency left — and this test pins the remaining behavioural half:
// the watch emits exactly one push per write, never two.
// Postgres-gated: the shared root harness supplies a real device plus a
// migration-0014-shaped, unpinned registry row for genuine no-model behavior.
func TestAttributeRestWriteProducesExactlyOnePush(t *testing.T) {
	newRootAttrDB(t)

	store := twinstore.NewMemoryStore()
	twinstore.SetGlobal(store)
	t.Cleanup(func() { twinstore.SetGlobal(nil) })

	pushes := swapBroadcastSeams(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// Subscribe synchronously so the write below cannot race the watch
	// registration, then run the loop under test.
	changes, err := store.Watch(ctx, "")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	go watchTwinStateChanges(ctx, changes, map[string]twinstore.State{})

	token := attrTestJWT(t, attrTestTenantA, "TENANT_ADMIN")
	req := httptest.NewRequest("POST",
		"/api/plugins/telemetry/DEVICE/"+attrTestDeviceA+"/SHARED_SCOPE",
		strings.NewReader(`{"config":"v1"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
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
	if first.entityID != attrTestDeviceA || first.scope != "SHARED_SCOPE" || first.data["config"] != "v1" {
		t.Fatalf("push = %#v, want SHARED_SCOPE config=v1 for %s", first, attrTestDeviceA)
	}

	select {
	case second := <-pushes:
		t.Fatalf("second push for a single write (double publisher): %#v", second)
	case <-time.After(300 * time.Millisecond):
		// exactly one push — single publisher holds
	}
}

// fakeWatchStore implements twinstore.Store for the resubscribe test: every
// Watch call hands out a fresh channel the test controls and signals on
// `subscribed`. The read methods are never exercised by the watch loop.
type fakeWatchStore struct {
	mu         sync.Mutex
	chans      []chan twinstore.Change
	subscribed chan struct{}
}

func (s *fakeWatchStore) Watch(ctx context.Context, prefix string) (<-chan twinstore.Change, error) {
	ch := make(chan twinstore.Change, 8)
	s.mu.Lock()
	s.chans = append(s.chans, ch)
	s.mu.Unlock()
	s.subscribed <- struct{}{}
	return ch, nil
}

func (s *fakeWatchStore) channel(i int) chan twinstore.Change {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chans[i]
}

func (s *fakeWatchStore) GetEntityState(context.Context, string, string, string) (twinstore.State, error) {
	return twinstore.State{}, twinstore.ErrNotFound
}
func (s *fakeWatchStore) GetTelemetryKeys(context.Context, string, string, string) ([]string, error) {
	return nil, nil
}
func (s *fakeWatchStore) GetLatestTelemetry(context.Context, string, string, string, []string) (map[string]twinstore.Value, error) {
	return nil, nil
}
func (s *fakeWatchStore) MergeTelemetry(context.Context, string, string, string, int64, map[string]interface{}) error {
	return nil
}
func (s *fakeWatchStore) MergeAttributes(context.Context, string, string, string, string, int64, map[string]interface{}) error {
	return nil
}

// TestTwinStateWatchResubscribesAfterChannelClose — a closed watch channel
// (NATS consumer death) must not kill the publisher: the loop resubscribes and
// the next change still broadcasts. The `last` map survives the restart, so an
// unchanged document re-delivered after resubscribe stays silent.
func TestTwinStateWatchResubscribesAfterChannelClose(t *testing.T) {
	prevRetry := twinWatchRetryBase
	twinWatchRetryBase = time.Millisecond
	t.Cleanup(func() { twinWatchRetryBase = prevRetry })

	pushes := swapBroadcastSeams(t)
	store := &fakeWatchStore{subscribed: make(chan struct{}, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go runTwinStateWatch(ctx, store)

	waitSubscribed := func(step string) {
		select {
		case <-store.subscribed:
		case <-time.After(2 * time.Second):
			t.Fatalf("watch loop did not subscribe (%s)", step)
		}
	}
	expectPush := func(step string) attrPush {
		select {
		case p := <-pushes:
			return p
		case <-time.After(2 * time.Second):
			t.Fatalf("no push arrived (%s)", step)
			return attrPush{}
		}
	}

	waitSubscribed("initial")
	doc := stateWith("t1", "d1", nil, map[string]map[string]twinstore.Value{
		"SERVER_SCOPE": {"site": {TS: 1000, Value: "hq"}},
	})
	store.channel(0) <- twinstore.Change{New: doc}
	if p := expectPush("before close"); p.data["site"] != "hq" {
		t.Fatalf("push before close = %#v", p)
	}

	close(store.channel(0))
	waitSubscribed("after close — the loop must resubscribe")

	// Same document again: `last` survived the restart, so no re-broadcast.
	// Then a real change: it must be the next (and only) push — ordering on
	// the single pushes channel proves the duplicate stayed silent.
	store.channel(1) <- twinstore.Change{New: doc}
	moved := stateWith("t1", "d1", nil, map[string]map[string]twinstore.Value{
		"SERVER_SCOPE": {"site": {TS: 2000, Value: "hq2"}},
	})
	store.channel(1) <- twinstore.Change{New: moved}
	if p := expectPush("after resubscribe"); p.data["site"] != "hq2" {
		t.Fatalf("push after resubscribe = %#v, want the changed value only (unchanged doc must not re-broadcast)", p)
	}
}
