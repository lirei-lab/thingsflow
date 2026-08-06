package twinmodel

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestValidateKeywordsTypesAndOrdering(t *testing.T) {
	schema := fixtureSchema(t, "energy_meter.json")
	values := map[string]interface{}{
		"attributes": map[string]interface{}{
			"sem_device_id": 7.0,
			"circuit_index": -1.0,
			"location":      "garage",
		},
		"features": map[string]interface{}{
			"electrical": map[string]interface{}{"properties": map[string]interface{}{
				"voltage":      301.0,
				"current":      "bad",
				"active_power": 7.0,
				"mystery":      true,
			}},
			"energy": map[string]interface{}{"properties": map[string]interface{}{
				"energy_in_kwh": -0.1,
			}},
			"telemetry": map[string]interface{}{"properties": map[string]interface{}{"unmodeled": true}},
		},
	}
	got := Validate(schema, "", values)
	wantPointers := []string{
		"attributes/circuit_index",
		"attributes/location",
		"attributes/sem_device_id",
		"features/electrical/properties/current",
		"features/electrical/properties/mystery",
		"features/electrical/properties/voltage",
		"features/energy/properties/energy_in_kwh",
	}
	if len(got) != len(wantPointers) {
		t.Fatalf("violations = %#v, want pointers %#v", got, wantPointers)
	}
	for i, want := range wantPointers {
		if got[i].Pointer != want {
			t.Fatalf("violation[%d].Pointer = %q, want %q; all=%#v", i, got[i].Pointer, want, got)
		}
	}
	if got[len(got)-2].Keyword != "maximum" {
		t.Fatalf("voltage keyword = %q, want maximum", got[len(got)-2].Keyword)
	}
}

func TestValidateNumericCoercionNullAndComplete(t *testing.T) {
	schema := DerivedSchema{
		UnknownKeys: "reject",
		Attributes: map[string]PropertySpec{
			"count": {Type: "integer", Required: true},
			"ratio": {Type: "number"},
		},
		Features: map[string]FeatureSchema{
			"control": {
				Definition: "thingsflow:feature:control:1.0.0",
				Properties: map[string]PropertySpec{
					"name": {Type: "string", Required: true},
				},
				DesiredProperties: map[string]PropertySpec{
					"enabled": {Type: "boolean", Writable: true},
				},
			},
		},
	}
	partial := map[string]interface{}{
		"attributes": map[string]interface{}{"count": 7.0, "ratio": 7},
		"features": map[string]interface{}{"control": map[string]interface{}{
			"properties":        map[string]interface{}{"name": nil},
			"desiredProperties": map[string]interface{}{"enabled": nil},
		}},
	}
	if got := Validate(schema, "root", partial); len(got) != 0 {
		t.Fatalf("partial integral numbers and synthetic nulls should validate: %#v", got)
	}
	complete := map[string]interface{}{"attributes": map[string]interface{}{}, "features": map[string]interface{}{}}
	got := Validate(schema, "root", complete, true)
	want := []Violation{
		{Pointer: "root/attributes/count", Keyword: "required", Message: "required value is missing", Got: nil},
		{Pointer: "root/features/control/properties/name", Keyword: "required", Message: "required value is missing", Got: nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complete violations = %#v, want %#v", got, want)
	}
}

func TestValidateEveryPropertyKeyword(t *testing.T) {
	minimum, maximum := 1.0, 2.0
	minLength, maxLength := 2, 3
	schema := DerivedSchema{Attributes: map[string]PropertySpec{
		"string":  {Type: "string", MinLength: &minLength, MaxLength: &maxLength, Pattern: "^[a-z]+$", Enum: []interface{}{"ab", "abc"}},
		"object":  {Type: "object"},
		"array":   {Type: "array"},
		"boolean": {Type: "boolean"},
		"number":  {Type: "number", Minimum: &minimum, Maximum: &maximum},
		"integer": {Type: "integer"},
	}}
	valid := map[string]interface{}{"attributes": map[string]interface{}{
		"string": "ab", "object": map[string]interface{}{}, "array": []interface{}{},
		"boolean": true, "number": 1.5, "integer": 2.0,
	}}
	if got := Validate(schema, "", valid); len(got) != 0 {
		t.Fatalf("all supported keywords should validate: %#v", got)
	}
	invalid := map[string]interface{}{"attributes": map[string]interface{}{
		"string": "A", "object": []interface{}{}, "array": map[string]interface{}{},
		"boolean": "true", "number": 3.0, "integer": 2.5,
	}}
	if got := Validate(schema, "", invalid); len(got) < 8 {
		t.Fatalf("expected violations across every keyword, got %#v", got)
	}
}

func TestValidateEscapesPointerSegments(t *testing.T) {
	schema := DerivedSchema{Attributes: map[string]PropertySpec{
		"sensor/a~raw": {Type: "string"},
	}}
	got := Validate(schema, "", map[string]interface{}{
		"attributes": map[string]interface{}{"sensor/a~raw": 1.0},
	})
	if len(got) != 1 || got[0].Pointer != "attributes/sensor~1a~0raw" {
		t.Fatalf("escaped pointer violation = %#v", got)
	}
}

func TestValidateSourceWirePayloads(t *testing.T) {
	schema := fixtureSchema(t, "energy_meter.json")
	realWire := map[string]interface{}{
		"timestamp":      float64(1775481000),
		"sem_device_id":  "SEM-98A316E0A35A",
		"circuit_name":   "refrigerator",
		"circuit_index":  float64(0),
		"location":       "kitchen",
		"voltage":        float64(229.8),
		"current":        float64(1.24),
		"active_power":   float64(185.4),
		"energy_in_kwh":  float64(15.2),
		"energy_out_kwh": float64(0),
	}
	modeled := semWireDocument(realWire)
	if got := Validate(schema, "", modeled); len(got) != 0 {
		t.Fatalf("source-grounded partial payload violations: %#v", got)
	}

	modeled["features"].(map[string]interface{})["electrical"].(map[string]interface{})["properties"].(map[string]interface{})["voltage"] = 301.0
	got := Validate(schema, "", modeled)
	if len(got) != 1 || got[0].Pointer != "features/electrical/properties/voltage" || got[0].Keyword != "maximum" {
		t.Fatalf("voltage maximum violation = %#v", got)
	}

	// Synthetic/forward-looking: current SEM scaling always emits numbers, but
	// null remains accepted defensively for other producers and partial merges.
	nullWire := map[string]interface{}{
		"sem_device_id": "SEM-98A316E0A35A", "circuit_name": "main_total",
		"circuit_index": 999.0, "location": "main", "voltage": nil,
		"current": nil, "active_power": nil, "energy_in_kwh": nil, "energy_out_kwh": nil,
	}
	if got := Validate(schema, "", semWireDocument(nullWire)); len(got) != 0 {
		t.Fatalf("synthetic null payload violations: %#v", got)
	}
}

func fixtureSchema(t *testing.T, name string) DerivedSchema {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	_, derived, err := Normalize(raw)
	if err != nil {
		t.Fatalf("Normalize fixture: %v", err)
	}
	var schema DerivedSchema
	if err := json.Unmarshal(derived, &schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	return schema
}

func semWireDocument(wire map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"attributes": map[string]interface{}{
			"sem_device_id": wire["sem_device_id"], "circuit_name": wire["circuit_name"],
			"circuit_index": wire["circuit_index"], "location": wire["location"],
		},
		"features": map[string]interface{}{
			"electrical": map[string]interface{}{"properties": map[string]interface{}{
				"voltage": wire["voltage"], "current": wire["current"], "active_power": wire["active_power"],
			}},
			"energy": map[string]interface{}{"properties": map[string]interface{}{
				"energy_in_kwh": wire["energy_in_kwh"], "energy_out_kwh": wire["energy_out_kwh"],
			}},
			"telemetry": map[string]interface{}{"properties": map[string]interface{}{"timestamp": wire["timestamp"]}},
		},
	}
}
