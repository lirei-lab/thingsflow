package bootstrap

import "testing"

// The bundles reference symbols by FQN, so an incorrect derivation silently leaves the
// SCADA bundles empty — exactly the bug this loader fixes. These cases are taken from the
// shipped symbol set and the FQNs the stock bundles expect.
func TestScadaSymbolFQNMatchesBundleReferences(t *testing.T) {
	cases := map[string]string{
		"HP Solar panel":         "hp_solar_panel",
		"HP Wind turbine":        "hp_wind_turbine",
		"HP Fuel generator":      "hp_fuel_generator",
		"Bottom Flow Meter":      "bottom_flow_meter",
		"HP Horizontal  Breaker": "hp_horizontal_breaker", // collapses repeated separators
		"  Padded Title  ":       "padded_title",
	}
	for title, want := range cases {
		if got := scadaSymbolFQN(title); got != want {
			t.Errorf("scadaSymbolFQN(%q) = %q, want %q", title, got, want)
		}
	}
}

func TestParseScadaSymbolMetadataStripsCDATA(t *testing.T) {
	svg := []byte(`<svg xmlns="http://www.w3.org/2000/svg">
<tb:metadata xmlns:tb="https://thingsboard.io/svg"><![CDATA[{
  "title": "HP Solar panel",
  "description": "Solar panel with various states.",
  "searchTags": ["energy","power"],
  "widgetSizeX": 2,
  "widgetSizeY": 3
}]]></tb:metadata>
</svg>`)
	meta, ok := parseScadaSymbolMetadata(svg)
	if !ok {
		t.Fatal("expected metadata to parse")
	}
	if meta.Title != "HP Solar panel" {
		t.Errorf("title = %q", meta.Title)
	}
	if meta.WidgetSizeX != 2 || meta.WidgetSizeY != 3 {
		t.Errorf("size = %v x %v", meta.WidgetSizeX, meta.WidgetSizeY)
	}
	if len(meta.SearchTags) != 2 {
		t.Errorf("tags = %v", meta.SearchTags)
	}
}

func TestParseScadaSymbolMetadataRejectsMissing(t *testing.T) {
	if _, ok := parseScadaSymbolMetadata([]byte(`<svg></svg>`)); ok {
		t.Error("expected failure when <tb:metadata> is absent")
	}
	// Present but titleless: the FQN would be empty, so it must not be accepted.
	if _, ok := parseScadaSymbolMetadata([]byte(`<svg><tb:metadata>{"description":"x"}</tb:metadata></svg>`)); ok {
		t.Error("expected failure when title is missing")
	}
}

// The descriptor drives what the widget renders; a template whose defaultConfig is a JSON
// string must come back out as a JSON string with the symbol URL injected.
func TestBuildScadaWidgetDescriptorInjectsSymbol(t *testing.T) {
	template := `{"defaultConfig":"{\"title\":\"SCADA symbol\",\"settings\":{}}","sizeX":1,"sizeY":1,"controllerScript":"var x = {previewWidth: '100px', previewHeight: '120px'};"}`
	meta := scadaSymbolMetadata{Title: "HP Solar panel", WidgetSizeX: 2, WidgetSizeY: 3}
	out, err := buildScadaWidgetDescriptor(template, meta, "tb-image;/api/images/system/solar-panel-hp.svg")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{
		`\"scadaSymbolUrl\":\"tb-image;/api/images/system/solar-panel-hp.svg\"`,
		`\"title\":\"HP Solar panel\"`,
		`previewWidth: '200px'`,
		`previewHeight: '320px'`,
	} {
		if !contains(out, want) {
			t.Errorf("descriptor missing %q\ngot: %s", want, out)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
