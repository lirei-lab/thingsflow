package ws

import (
	"context"
	"testing"

	"flow-core/internal/twinstore"
)

func TestFetchEntityTimeseriesLatestUsesTwinState(t *testing.T) {
	store := twinstore.NewMemoryStore()
	twinstore.SetGlobal(store)
	t.Cleanup(func() { twinstore.SetGlobal(nil) })

	const tenantID = "tenant-a"
	const deviceID = "device-a"
	if err := store.MergeTelemetry(context.Background(), tenantID, "DEVICE", deviceID, 1234, map[string]interface{}{
		"temperature": 21.5,
	}); err != nil {
		t.Fatalf("seed twin telemetry: %v", err)
	}

	got := fetchEntityTimeseriesLatest(tenantID, "DEVICE", deviceID, []string{"temperature", "co2"})
	if got["temperature"].(map[string]interface{})["value"] != 21.5 {
		t.Fatalf("temperature latest = %#v", got["temperature"])
	}
	if got["co2"].(map[string]interface{})["value"] != nil {
		t.Fatalf("missing co2 should be emitted as nil, got %#v", got["co2"])
	}
}
