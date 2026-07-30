package deviceactivity

import (
	"context"
	"testing"
	"time"

	"flow-core/internal/twinstore"
)

func TestActiveFromTwinUsesLatestTelemetryTimestamp(t *testing.T) {
	store := twinstore.NewMemoryStore()
	twinstore.SetGlobal(store)
	t.Cleanup(func() { twinstore.SetGlobal(nil) })

	now := time.UnixMilli(1_700_000_000_000)
	tenantID := "tenant-a"
	deviceID := "device-a"
	if err := store.MergeTelemetry(context.Background(), tenantID, "DEVICE", deviceID, now.Add(-10*time.Second).UnixMilli(), map[string]interface{}{
		"temperature": 21.5,
	}); err != nil {
		t.Fatalf("seed twin telemetry: %v", err)
	}

	active, ok := ActiveFromTwin(context.Background(), tenantID, deviceID, now)
	if !ok {
		t.Fatal("ActiveFromTwin() ok=false, want true")
	}
	if !active {
		t.Fatal("ActiveFromTwin() active=false, want true")
	}
}

func TestActiveFromTwinMarksStaleTelemetryInactive(t *testing.T) {
	store := twinstore.NewMemoryStore()
	twinstore.SetGlobal(store)
	t.Cleanup(func() { twinstore.SetGlobal(nil) })

	now := time.UnixMilli(1_700_000_000_000)
	tenantID := "tenant-a"
	deviceID := "device-a"
	if err := store.MergeTelemetry(context.Background(), tenantID, "DEVICE", deviceID, now.Add(-2*time.Minute).UnixMilli(), map[string]interface{}{
		"temperature": 21.5,
	}); err != nil {
		t.Fatalf("seed twin telemetry: %v", err)
	}

	active, ok := ActiveFromTwin(context.Background(), tenantID, deviceID, now)
	if !ok {
		t.Fatal("ActiveFromTwin() ok=false, want true")
	}
	if active {
		t.Fatal("ActiveFromTwin() active=true, want false")
	}
}
