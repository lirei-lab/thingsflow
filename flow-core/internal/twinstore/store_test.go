package twinstore

import (
	"context"
	"testing"
)

// Merge staleness is per key (see the decision comment in MergeTelemetry):
// an EXISTING key rejects writes older than the value it holds, while a NEW
// key always enters with its own timestamp — even when the document has
// already seen newer keys. Both behaviors are pinned here so the rule is not
// re-litigated silently.
func TestMemoryStoreMergesTelemetryPerKeyLastWriteWins(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	if err := store.MergeTelemetry(ctx, "tenant-1", "DEVICE", "device-1", 1000, map[string]interface{}{
		"temperature": 21.5,
		"active":      true,
	}); err != nil {
		t.Fatalf("merge initial telemetry: %v", err)
	}
	// ts 900 < doc UpdatedTS 1000: "temperature" exists and must keep its
	// newer value; "humidity" is NEW and must enter despite the older ts.
	if err := store.MergeTelemetry(ctx, "tenant-1", "DEVICE", "device-1", 900, map[string]interface{}{
		"temperature": 99.0,
		"humidity":    55.0,
	}); err != nil {
		t.Fatalf("merge older telemetry: %v", err)
	}
	if err := store.MergeTelemetry(ctx, "tenant-1", "DEVICE", "device-1", 1100, map[string]interface{}{
		"active": false,
	}); err != nil {
		t.Fatalf("merge newer telemetry: %v", err)
	}

	latest, err := store.GetLatestTelemetry(ctx, "tenant-1", "DEVICE", "device-1", nil)
	if err != nil {
		t.Fatalf("latest telemetry: %v", err)
	}
	if got := latest["temperature"].Value; got != 21.5 {
		t.Fatalf("temperature = %v, want 21.5 (stale write on existing key must be dropped)", got)
	}
	if got := latest["humidity"].Value; got != 55.0 {
		t.Fatalf("humidity = %v, want 55.0 (new key must enter even when doc is newer)", got)
	}
	if got := latest["humidity"].TS; got != int64(900) {
		t.Fatalf("humidity ts = %d, want its own ts 900", got)
	}
	if got := latest["active"].Value; got != false {
		t.Fatalf("active = %v, want false", got)
	}
	if got := latest["active"].TS; got != int64(1100) {
		t.Fatalf("active ts = %d, want 1100", got)
	}
}

func TestMemoryStoreListsAndFiltersTelemetryKeys(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	_ = store.MergeTelemetry(ctx, "tenant-1", "DEVICE", "device-1", 1000, map[string]interface{}{
		"b_key": 2,
		"a_key": 1,
	})

	keys, err := store.GetTelemetryKeys(ctx, "tenant-1", "DEVICE", "device-1")
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if len(keys) != 2 || keys[0] != "a_key" || keys[1] != "b_key" {
		t.Fatalf("keys = %v, want [a_key b_key]", keys)
	}

	latest, err := store.GetLatestTelemetry(ctx, "tenant-1", "DEVICE", "device-1", []string{"b_key"})
	if err != nil {
		t.Fatalf("filtered latest: %v", err)
	}
	if len(latest) != 1 || latest["b_key"].Value != 2 {
		t.Fatalf("filtered latest = %v", latest)
	}
}

// Attribute mirror of the per-key rule: existing key keeps its newer value,
// new key enters with its own (older) timestamp.
func TestMemoryStoreMergesAttributesPerKeyLastWriteWins(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.MergeAttributes(ctx, "tenant-1", "DEVICE", "device-1", "SERVER_SCOPE", 1000, map[string]interface{}{
		"firmware": "1.0",
	}); err != nil {
		t.Fatalf("merge attributes: %v", err)
	}
	if err := store.MergeAttributes(ctx, "tenant-1", "DEVICE", "device-1", "SERVER_SCOPE", 900, map[string]interface{}{
		"firmware": "old",
		"serial":   "S-1",
	}); err != nil {
		t.Fatalf("merge older attributes: %v", err)
	}

	state, err := store.GetEntityState(ctx, "tenant-1", "DEVICE", "device-1")
	if err != nil {
		t.Fatalf("entity state: %v", err)
	}
	if got := state.Attributes["SERVER_SCOPE"]["firmware"].Value; got != "1.0" {
		t.Fatalf("firmware = %v, want 1.0 (stale write on existing key must be dropped)", got)
	}
	serial, ok := state.Attributes["SERVER_SCOPE"]["serial"]
	if !ok {
		t.Fatal("serial missing: new attribute key must enter even when doc is newer")
	}
	if serial.Value != "S-1" || serial.TS != int64(900) {
		t.Fatalf("serial = %+v, want value S-1 at its own ts 900", serial)
	}
}

// Change.Old must be the PRE-write state. Before the fix, ensureState returned
// a State sharing the old state's maps, so the in-place merge mutated "old"
// too and watchers always saw Old == New.
func TestMemoryStoreTelemetryChangeOldIsPreWriteSnapshot(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := store.Watch(ctx, "")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	if err := store.MergeTelemetry(ctx, "tenant-1", "DEVICE", "device-1", 1000, map[string]interface{}{
		"temperature": 20.0,
	}); err != nil {
		t.Fatalf("first merge: %v", err)
	}
	if err := store.MergeTelemetry(ctx, "tenant-1", "DEVICE", "device-1", 2000, map[string]interface{}{
		"temperature": 30.0,
	}); err != nil {
		t.Fatalf("second merge: %v", err)
	}

	first := <-ch
	if len(first.Old.Telemetry) != 0 {
		t.Fatalf("first change Old.Telemetry = %v, want empty (no prior state)", first.Old.Telemetry)
	}
	if got := first.New.Telemetry["temperature"].Value; got != 20.0 {
		t.Fatalf("first change New temperature = %v, want 20.0", got)
	}

	second := <-ch
	oldVal := second.Old.Telemetry["temperature"]
	if oldVal.Value != 20.0 || oldVal.TS != int64(1000) {
		t.Fatalf("second change Old temperature = %+v, want the FIRST write (20.0 @ 1000) — Old==New means the aliasing bug is back", oldVal)
	}
	newVal := second.New.Telemetry["temperature"]
	if newVal.Value != 30.0 || newVal.TS != int64(2000) {
		t.Fatalf("second change New temperature = %+v, want 30.0 @ 2000", newVal)
	}
}

// Same pre-write guarantee for the attributes side of the document.
func TestMemoryStoreAttributesChangeOldIsPreWriteSnapshot(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := store.Watch(ctx, "")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	if err := store.MergeAttributes(ctx, "tenant-1", "DEVICE", "device-1", "SHARED_SCOPE", 1000, map[string]interface{}{
		"targetTemp": 21.0,
	}); err != nil {
		t.Fatalf("first merge: %v", err)
	}
	if err := store.MergeAttributes(ctx, "tenant-1", "DEVICE", "device-1", "SHARED_SCOPE", 2000, map[string]interface{}{
		"targetTemp": 23.5,
	}); err != nil {
		t.Fatalf("second merge: %v", err)
	}

	<-ch // first change: Old empty, already covered by the telemetry test
	second := <-ch
	oldVal := second.Old.Attributes["SHARED_SCOPE"]["targetTemp"]
	if oldVal.Value != 21.0 || oldVal.TS != int64(1000) {
		t.Fatalf("second change Old targetTemp = %+v, want the FIRST write (21.0 @ 1000)", oldVal)
	}
	newVal := second.New.Attributes["SHARED_SCOPE"]["targetTemp"]
	if newVal.Value != 23.5 || newVal.TS != int64(2000) {
		t.Fatalf("second change New targetTemp = %+v, want 23.5 @ 2000", newVal)
	}
}
