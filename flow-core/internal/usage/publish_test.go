package usage

import (
	"encoding/json"
	"testing"
)

// TestEntityPoint asserts the published envelope marshals to JSON with the
// seven snake_case keys the 03-01 Bento materializer consumes, and that
// publishEntityPoint is a safe no-op when the publisher is disabled (nc nil).
func TestEntityPoint(t *testing.T) {
	t.Run("marshals snake_case fields", func(t *testing.T) {
		b, err := json.Marshal(entityPoint{
			TenantID:     "tenant-1",
			EntityType:   "API_USAGE_STATE",
			EntityID:     "state-1",
			TelemetryKey: "transportMsgCount",
			ValueString:  "42",
			ValueKind:    "number",
			TS:           1718634000000,
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		for _, key := range []string{
			"tenant_id", "entity_type", "entity_id",
			"telemetry_key", "value_string", "value_kind", "ts",
		} {
			if _, ok := m[key]; !ok {
				t.Errorf("envelope missing snake_case key %q; got %v", key, m)
			}
		}
		if m["tenant_id"] != "tenant-1" {
			t.Errorf("tenant_id = %v, want tenant-1", m["tenant_id"])
		}
		if m["entity_id"] != "state-1" {
			t.Errorf("entity_id = %v, want state-1", m["entity_id"])
		}
		if m["value_kind"] != "number" {
			t.Errorf("value_kind = %v, want number", m["value_kind"])
		}
	})

	t.Run("publishEntityPoint no-op when disabled", func(t *testing.T) {
		// nc is nil unless InitPublisher connected successfully; this must
		// never panic (best-effort drop, mirroring the old ignored Exec errors).
		nc = nil
		publishEntityPoint("tenant-1", "API_USAGE_STATE", "state-1", "k", "v", "string", 1)
	})
}
