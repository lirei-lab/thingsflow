package entityquery

import (
	"testing"

	"flow-core/internal/twinstore"
)

func TestDeviceTypesFromFilterSupportsThingsBoardArrayShape(t *testing.T) {
	got := deviceTypesFromFilter(map[string]interface{}{
		"deviceTypes": []interface{}{"office_thermostat", "office_air_quality"},
	})
	want := []string{"office_thermostat", "office_air_quality"}
	if len(got) != len(want) {
		t.Fatalf("deviceTypes len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("deviceTypes[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestDeviceTypesFromFilterSupportsLegacySingleType(t *testing.T) {
	got := deviceTypesFromFilter(map[string]interface{}{
		"deviceType": "thermostat",
	})
	if len(got) != 1 || got[0] != "thermostat" {
		t.Fatalf("deviceTypes = %v, want [thermostat]", got)
	}
}

func TestFetchLatestTimeseriesUsesTwinStateStoreFirst(t *testing.T) {
	store := twinstore.NewMemoryStore()
	twinstore.SetGlobal(store)
	t.Cleanup(func() { twinstore.SetGlobal(nil) })
	if err := store.MergeTelemetry(nil, "tenant-1", "DEVICE", "device-1", 1234, map[string]interface{}{
		"temperature": 22.5,
	}); err != nil {
		t.Fatalf("merge telemetry: %v", err)
	}

	got := fetchLatestTimeseries("tenant-1", "DEVICE", "device-1", "temperature")
	if got["ts"] != int64(1234) || got["value"] != 22.5 {
		t.Fatalf("latest = %v, want ts=1234 value=22.5", got)
	}
}

func TestLegacyFindKeysResponseMatchesThingsBoardAutocompleteContract(t *testing.T) {
	got := legacyFindKeysResponse([]map[string]string{
		{"type": "TIME_SERIES", "key": "active_power"},
		{"type": "TIME_SERIES", "key": "active_power"},
		{"type": "ATTRIBUTE", "key": "site"},
	})

	timeseries, ok := got["timeseries"].([]string)
	if !ok || len(timeseries) != 1 || timeseries[0] != "active_power" {
		t.Fatalf("timeseries = %#v, want [active_power]", got["timeseries"])
	}

	attributes, ok := got["attribute"].([]string)
	if !ok || len(attributes) != 1 || attributes[0] != "site" {
		t.Fatalf("attribute = %#v, want [site]", got["attribute"])
	}

	entityTypes, ok := got["entityTypes"].([]string)
	if !ok || len(entityTypes) != 0 {
		t.Fatalf("entityTypes = %#v, want empty string slice", got["entityTypes"])
	}
}
