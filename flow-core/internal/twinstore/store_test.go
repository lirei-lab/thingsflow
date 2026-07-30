package twinstore

import (
	"context"
	"testing"
)

func TestMemoryStoreMergesTelemetryAndRejectsStaleValues(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	if err := store.MergeTelemetry(ctx, "tenant-1", "DEVICE", "device-1", 1000, map[string]interface{}{
		"temperature": 21.5,
		"active":      true,
	}); err != nil {
		t.Fatalf("merge initial telemetry: %v", err)
	}
	if err := store.MergeTelemetry(ctx, "tenant-1", "DEVICE", "device-1", 900, map[string]interface{}{
		"temperature": 99.0,
		"humidity":    55.0,
	}); err != nil {
		t.Fatalf("merge stale telemetry: %v", err)
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
		t.Fatalf("temperature = %v, want 21.5", got)
	}
	if _, ok := latest["humidity"]; ok {
		t.Fatalf("stale new key was inserted: %v", latest["humidity"])
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

func TestMemoryStoreMergesAttributesByScope(t *testing.T) {
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
		t.Fatalf("merge stale attributes: %v", err)
	}

	state, err := store.GetEntityState(ctx, "tenant-1", "DEVICE", "device-1")
	if err != nil {
		t.Fatalf("entity state: %v", err)
	}
	if got := state.Attributes["SERVER_SCOPE"]["firmware"].Value; got != "1.0" {
		t.Fatalf("firmware = %v, want 1.0", got)
	}
	if _, ok := state.Attributes["SERVER_SCOPE"]["serial"]; ok {
		t.Fatalf("stale new attribute inserted: %v", state.Attributes["SERVER_SCOPE"]["serial"])
	}
}
