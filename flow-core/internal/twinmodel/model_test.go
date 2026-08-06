package twinmodel

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeNormativeEnvelope(t *testing.T) {
	raw := json.RawMessage(`{
		"modelId":"Energy Meter","version":"1.0.0","kind":"DEVICE",
		"displayName":"Energy meter","unknownKeys":"allow","enforcementMode":"warn",
		"attributes":{"sem_device_id":{"type":"string","required":true},"circuit_index":{"type":"integer","minimum":0}},
		"features":{"electrical":{"definition":"thingsflow:feature:electrical:1.0.0","properties":{"voltage":{"type":"number","minimum":0,"maximum":300,"unit":"V","description":"wire voltage"}},"desiredProperties":{"sample_interval":{"type":"integer","minimum":1,"writable":true}}}},
		"relationships":{"ConnectedTo":{"target":["energy_meter"],"targetEntityTypes":["DEVICE"],"maxCardinality":8,"bidirectional":true}},
		"x-owner":"energy-platform"
	}`)

	model, derivedJSON, err := Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if model.ModelID != "energy_meter" || model.Version != "1.0.0" || model.Kind != "DEVICE" {
		t.Fatalf("unexpected normalized identity: %#v", model)
	}
	authoredJSON, err := json.Marshal(model)
	if err != nil {
		t.Fatalf("marshal authored model: %v", err)
	}
	var authored map[string]interface{}
	if err := json.Unmarshal(authoredJSON, &authored); err != nil {
		t.Fatalf("decode authored round trip: %v", err)
	}
	if authored["x-owner"] != "energy-platform" {
		t.Fatalf("top-level metadata was not preserved: %s", authoredJSON)
	}
	properties := authored["features"].(map[string]interface{})["electrical"].(map[string]interface{})["properties"].(map[string]interface{})
	if properties["voltage"].(map[string]interface{})["description"] != "wire voltage" {
		t.Fatalf("property metadata was not preserved: %s", authoredJSON)
	}

	var derived map[string]interface{}
	if err := json.Unmarshal(derivedJSON, &derived); err != nil {
		t.Fatalf("decode derived schema: %v", err)
	}
	if _, exists := derived["displayName"]; exists {
		t.Fatalf("displayName leaked into derived schema: %s", derivedJSON)
	}
	if _, exists := derived["x-owner"]; exists {
		t.Fatalf("authored metadata leaked into derived schema: %s", derivedJSON)
	}
	dVoltage := derived["features"].(map[string]interface{})["electrical"].(map[string]interface{})["properties"].(map[string]interface{})["voltage"].(map[string]interface{})
	if _, exists := dVoltage["description"]; exists {
		t.Fatalf("property metadata leaked into derived schema: %s", derivedJSON)
	}
	if got := dVoltage["maximum"]; got != float64(300) {
		t.Fatalf("maximum = %#v, want 300", got)
	}
	expectedDerived := json.RawMessage(`{
		"modelId":"energy_meter","version":"1.0.0","kind":"DEVICE","unknownKeys":"allow","enforcementMode":"warn",
		"attributes":{"sem_device_id":{"type":"string","required":true},"circuit_index":{"type":"integer","minimum":0}},
		"features":{"electrical":{"definition":"thingsflow:feature:electrical:1.0.0","properties":{"voltage":{"type":"number","minimum":0,"maximum":300,"unit":"V"}},"desiredProperties":{"sample_interval":{"type":"integer","minimum":1,"writable":true}}}},
		"relationships":{"ConnectedTo":{"target":["energy_meter"],"targetEntityTypes":["DEVICE"],"maxCardinality":8,"bidirectional":true}}
	}`)
	assertJSONEqual(t, derivedJSON, expectedDerived)
}

func TestNormalizeDefaultsAndIdempotence(t *testing.T) {
	raw := json.RawMessage(`{"modelId":"Meter","version":"1.0.0","kind":"device","attributes":{}}`)
	model, derived, err := Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if model.UnknownKeys != "allow" || model.EnforcementMode != "warn" {
		t.Fatalf("defaults not materialized: %#v", model)
	}
	if model.Features == nil || model.Relationships == nil || model.Attributes == nil {
		t.Fatalf("collection defaults must be non-nil: %#v", model)
	}
	_, second, err := Normalize(derived)
	if err != nil {
		t.Fatalf("Normalize(derived): %v", err)
	}
	if string(derived) != string(second) {
		t.Fatalf("Normalize not idempotent:\nfirst:  %s\nsecond: %s", derived, second)
	}
}

func TestModelIDCanonicalizationAndBounds(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"Energy Meter", "energy_meter"},
		{"__Meter---Panel__", "meter_panel"},
		{"A/B.C", "a_b_c"},
		{strings.Repeat("a", 255), strings.Repeat("a", 255)},
	}
	for _, tt := range tests {
		t.Run(tt.raw[:min(len(tt.raw), 24)], func(t *testing.T) {
			got, err := NormalizeModelID(tt.raw)
			if err != nil {
				t.Fatalf("NormalizeModelID(%q): %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("NormalizeModelID(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
	for _, raw := range []string{"", "___", "---", strings.Repeat("a", 256), strings.Repeat("a", 300)} {
		if _, err := NormalizeModelID(raw); err == nil {
			t.Errorf("NormalizeModelID(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestNormalizeVersionContract(t *testing.T) {
	for _, version := range []string{"0.0.0", "1.0.0", "2147483647.2147483647.2147483647"} {
		raw := json.RawMessage(`{"modelId":"meter","version":"` + version + `","kind":"DEVICE"}`)
		if _, _, err := Normalize(raw); err != nil {
			t.Errorf("version %q rejected: %v", version, err)
		}
	}
	for _, version := range []string{"1", "1.0", "01.0.0", "1.00.0", "1.0.0-beta", "1.0.0+build", "2147483648.0.0", "999999999999.0.0"} {
		raw := json.RawMessage(`{"modelId":"meter","version":"` + version + `","kind":"DEVICE"}`)
		if _, _, err := Normalize(raw); err == nil || !strings.Contains(err.Error(), "version") {
			t.Errorf("version %q error = %v, want version-path error", version, err)
		}
	}
}

func TestNormalizeRejectsInvalidLanguageWithPath(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		path string
	}{
		{"unsupported keyword", `{"modelId":"m","version":"1.0.0","kind":"DEVICE","attributes":{"p":{"type":"number","oneOf":[{"type":"number"}]}}}`, "attributes/p/oneOf"},
		{"invalid pattern", `{"modelId":"m","version":"1.0.0","kind":"DEVICE","attributes":{"p":{"type":"string","pattern":"["}}}`, "attributes/p/pattern"},
		{"numeric bounds", `{"modelId":"m","version":"1.0.0","kind":"DEVICE","attributes":{"p":{"type":"number","minimum":2,"maximum":1}}}`, "attributes/p/maximum"},
		{"string bounds", `{"modelId":"m","version":"1.0.0","kind":"DEVICE","attributes":{"p":{"type":"string","minLength":2,"maxLength":1}}}`, "attributes/p/maxLength"},
		{"missing type", `{"modelId":"m","version":"1.0.0","kind":"DEVICE","attributes":{"p":{"unit":"V"}}}`, "attributes/p/type"},
		{"bad feature definition", `{"modelId":"m","version":"1.0.0","kind":"DEVICE","features":{"electrical":{"definition":"electrical","properties":{}}}}`, "features/electrical/definition"},
		{"duplicate target", `{"modelId":"m","version":"1.0.0","kind":"DEVICE","relationships":{"ConnectedTo":{"target":["meter","Meter"],"targetEntityTypes":["DEVICE"],"maxCardinality":1,"bidirectional":true}}}`, "relationships/ConnectedTo/target/1"},
		{"missing bidirectional", `{"modelId":"m","version":"1.0.0","kind":"DEVICE","relationships":{"ConnectedTo":{"target":["meter"],"targetEntityTypes":["DEVICE"],"maxCardinality":1}}}`, "relationships/ConnectedTo/bidirectional"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Normalize(json.RawMessage(tt.raw))
			if err == nil || !strings.Contains(err.Error(), tt.path) {
				t.Fatalf("Normalize error = %v, want path %q", err, tt.path)
			}
		})
	}
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue, wantValue interface{}
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("decode got JSON: %v", err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("decode want JSON: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}

func TestNormalizeSourceGroundedFixtures(t *testing.T) {
	for _, name := range []string{"energy_meter.json", "building.json", "main_total.json"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/" + name)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			model, derived, err := Normalize(raw)
			if err != nil {
				t.Fatalf("Normalize fixture: %v", err)
			}
			_, second, err := Normalize(derived)
			if err != nil || !reflect.DeepEqual(derived, second) {
				t.Fatalf("fixture is not idempotent: err=%v first=%s second=%s", err, derived, second)
			}
			if name == "energy_meter.json" {
				want := []interface{}{"kitchen", "general", "mechanical", "hvac", "main", "unknown"}
				if !reflect.DeepEqual(model.Attributes["location"].Enum, want) {
					t.Fatalf("location enum = %#v, want %#v", model.Attributes["location"].Enum, want)
				}
			}
			if name == "main_total.json" && !enumContainsNumber(model.Attributes["circuit_index"].Enum, 999) {
				t.Fatalf("main_total circuit_index enum = %#v, want 999", model.Attributes["circuit_index"].Enum)
			}
		})
	}
}
