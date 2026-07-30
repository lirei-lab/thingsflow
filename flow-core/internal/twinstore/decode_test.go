package twinstore

import "testing"

// Two shapes share this bucket: whole State documents written by flow-core, and
// bare per-key Values written by the data-plane pipeline. decodeState must tell
// them apart, because callers use the error to decide which parser to use.
//
// When it could not, live dashboards silently stopped updating: a bare Value
// decoded into an empty State without error, the watcher took the success path,
// and the resulting "state" had no telemetry to broadcast.
func TestDecodeState_RejectsBareTelemetryValue(t *testing.T) {
	// Exactly what the latest-kv pipeline writes per telemetry key.
	_, err := decodeState([]byte(`{"ts":1785254997899,"value":777777}`))
	if err == nil {
		t.Fatal("a bare {ts,value} decoded as a State; the per-key fallback would never run")
	}
}

func TestDecodeState_AcceptsRealState(t *testing.T) {
	payload := []byte(`{"schema":"v1","tenantId":"t1","entityType":"DEVICE",
		"entityId":"d1","updatedTs":42,"telemetry":{"power":{"ts":42,"value":1.5}}}`)
	state, err := decodeState(payload)
	if err != nil {
		t.Fatalf("decodeState: %v", err)
	}
	if state.EntityID != "d1" || len(state.Telemetry) != 1 {
		t.Fatalf("decoded = %+v, want the entity and its telemetry", state)
	}
}

// A state carrying only attributes or activity is still a state; rejecting it
// would send attribute updates down the telemetry fallback, which cannot parse
// them.
func TestDecodeState_AcceptsStateWithoutTelemetry(t *testing.T) {
	for name, payload := range map[string]string{
		"attributes": `{"entityId":"d1","attributes":{"SERVER_SCOPE":{"k":{"ts":1,"value":2}}}}`,
		"activity":   `{"entityId":"d1","activity":{"active":true}}`,
		"identity":   `{"entityId":"d1","entityType":"DEVICE","tenantId":"t1"}`,
	} {
		if _, err := decodeState([]byte(payload)); err != nil {
			t.Errorf("%s: decodeState rejected a real state: %v", name, err)
		}
	}
}

func TestDecodeState_RejectsMalformedJSON(t *testing.T) {
	if _, err := decodeState([]byte(`not json`)); err == nil {
		t.Fatal("malformed JSON decoded without error")
	}
}
