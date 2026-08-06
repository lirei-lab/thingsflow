package bootstrap

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestDemoOptedIn(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"", false},
		{"false", false},
		{"0", false},
		{"no", false},
		{"true", true},
		{"TRUE", true},
		{"1", true},
		{"yes", true},
		{" true ", true},
	}
	for _, c := range cases {
		t.Setenv("THINGSFLOW_LOAD_DEMO", c.val)
		if got := demoOptedIn(); got != c.want {
			t.Errorf("THINGSFLOW_LOAD_DEMO=%q → demoOptedIn() = %v, want %v", c.val, got, c.want)
		}
	}
	os.Unsetenv("THINGSFLOW_LOAD_DEMO")
	if demoOptedIn() {
		t.Error("unset env should be off")
	}
}

func TestLoadDemoNoOpWithoutEnv(t *testing.T) {
	os.Unsetenv("THINGSFLOW_LOAD_DEMO")
	// dbpkg.Pool is nil in this test process — if LoadDemo proceeded
	// past the env gate it would log a "Pool nil" warning instead of
	// returning silently. The test passes by virtue of the function
	// not panicking and not hitting the DB.
	LoadDemo()
}

func TestDemoDevicesMatchStockDashboardContract(t *testing.T) {
	var foundThermostat bool
	got := map[string]demoDeviceSeed{}
	for _, d := range demoDevices() {
		got[d.name] = d
		if d.name == "Thermostat Demo" {
			foundThermostat = true
			if d.deviceType != "thermostat" {
				t.Fatalf("Thermostat Demo type = %q, want thermostat", d.deviceType)
			}
			if d.token != "DEMO_THERMOSTAT_TOKEN" {
				t.Fatalf("Thermostat Demo token = %q", d.token)
			}
		}
	}
	if !foundThermostat {
		t.Fatal("Thermostat Demo seed missing")
	}

	want := map[string]struct {
		deviceType string
		token      string
	}{
		"DHT22 Demo Sensor":  {"default", "DEMO_DHT22_TOKEN"},
		"Raspberry Pi Demo":  {"default", "DEMO_RPI_TOKEN"},
		"Thermostat Demo":    {"thermostat", "DEMO_THERMOSTAT_TOKEN"},
		"Energy Meter Demo":  {"energy_meter", "DEMO_ENERGY_METER_TOKEN"},
		"Motion Sensor Demo": {"motion_sensor", "DEMO_MOTION_SENSOR_TOKEN"},
		"Air Quality Demo":   {"air_quality", "DEMO_AIR_QUALITY_TOKEN"},
		"Water Tank Demo":    {"water_tank", "DEMO_WATER_TANK_TOKEN"},
		"Pump Demo":          {"pump", "DEMO_PUMP_TOKEN"},
		"Flow Meter Demo":    {"flow_meter", "DEMO_FLOW_METER_TOKEN"},
		"Pressure Sensor Demo": {
			"pressure_sensor",
			"DEMO_PRESSURE_SENSOR_TOKEN",
		},
		"Valve Demo":                 {"valve", "DEMO_VALVE_TOKEN"},
		"Office Thermostat Demo":     {"office_thermostat", "DEMO_OFFICE_THERMOSTAT_TOKEN"},
		"Office Occupancy Demo":      {"office_occupancy", "DEMO_OFFICE_OCCUPANCY_TOKEN"},
		"Office IAQ Demo":            {"office_air_quality", "DEMO_OFFICE_IAQ_TOKEN"},
		"Office Smart Light Demo":    {"smart_light", "DEMO_OFFICE_LIGHT_TOKEN"},
		"Office Door Access Demo":    {"door_access", "DEMO_OFFICE_DOOR_TOKEN"},
		"Conference Room Meter Demo": {"conference_room", "DEMO_CONFERENCE_ROOM_TOKEN"},
		"Elevator Monitor Demo":      {"elevator_monitor", "DEMO_ELEVATOR_TOKEN"},
	}
	if len(got) != len(want) {
		t.Fatalf("demoDevices() returned %d devices, want %d", len(got), len(want))
	}
	for name, expected := range want {
		d, ok := got[name]
		if !ok {
			t.Fatalf("demo device %q missing", name)
		}
		if d.deviceType != expected.deviceType {
			t.Fatalf("%s type = %q, want %q", name, d.deviceType, expected.deviceType)
		}
		if d.token != expected.token {
			t.Fatalf("%s token = %q, want %q", name, d.token, expected.token)
		}
	}
}

func TestDemoOtaPackagesMatchClassicFirmwareSoftwareContract(t *testing.T) {
	got := map[string]demoOtaSeed{}
	for _, pkg := range demoOtaPackages() {
		got[pkg.title] = pkg
	}
	want := map[string]struct {
		packageType string
		version     string
		fileName    string
	}{
		"Demo Thermostat Firmware": {"FIRMWARE", "1.0.0", "demo-thermostat-fw.bin"},
		"Demo Gateway Software":    {"SOFTWARE", "2026.05", "demo-gateway-sw.tar.gz"},
	}
	if len(got) != len(want) {
		t.Fatalf("demoOtaPackages() returned %d packages, want %d", len(got), len(want))
	}
	for title, expected := range want {
		pkg, ok := got[title]
		if !ok {
			t.Fatalf("demo OTA package %q missing", title)
		}
		if pkg.packageType != expected.packageType {
			t.Fatalf("%s type = %q, want %q", title, pkg.packageType, expected.packageType)
		}
		if pkg.version != expected.version {
			t.Fatalf("%s version = %q, want %q", title, pkg.version, expected.version)
		}
		if pkg.fileName != expected.fileName {
			t.Fatalf("%s fileName = %q, want %q", title, pkg.fileName, expected.fileName)
		}
	}
}

func TestClassicDemoDashboardsIncludeScadaAndOffice(t *testing.T) {
	files := map[string]string{
		"../../../flow-core/tb-resources/demo_dashboards/scada_process.json":         "SCADA Process Demo",
		"../../../flow-core/tb-resources/demo_dashboards/smart_building_office.json": "Smart Building Office Demo",
	}
	for path, wantTitle := range files {
		buf, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("demo dashboard %s missing: %v", path, err)
		}
		var doc struct {
			Title         string          `json:"title"`
			Configuration json.RawMessage `json:"configuration"`
		}
		if err := json.Unmarshal(buf, &doc); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if doc.Title != wantTitle {
			t.Fatalf("%s title = %q, want %q", path, doc.Title, wantTitle)
		}
		if len(doc.Configuration) == 0 || string(doc.Configuration) == "null" {
			t.Fatalf("%s configuration is empty", path)
		}
		var config struct {
			Widgets map[string]struct {
				ID          string `json:"id"`
				TypeFullFqn string `json:"typeFullFqn"`
			} `json:"widgets"`
		}
		if err := json.Unmarshal(doc.Configuration, &config); err != nil {
			t.Fatalf("parse %s configuration: %v", path, err)
		}
		if len(config.Widgets) == 0 {
			t.Fatalf("%s has no widgets", path)
		}
		for widgetID, widget := range config.Widgets {
			if widget.ID != widgetID {
				t.Fatalf("%s widget %s id = %q, want matching widget id", path, widgetID, widget.ID)
			}
			if widget.TypeFullFqn == "" {
				t.Fatalf("%s widget %s missing typeFullFqn", path, widgetID)
			}
		}
	}
}

func TestDemoDashboardWidgetContract(t *testing.T) {
	entries, err := os.ReadDir("../../../flow-core/tb-resources/demo_dashboards")
	if err != nil {
		t.Fatalf("read demo dashboards: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := "../../../flow-core/tb-resources/demo_dashboards/" + entry.Name()
		t.Run(entry.Name(), func(t *testing.T) {
			buf, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read dashboard: %v", err)
			}
			var doc struct {
				Title         string          `json:"title"`
				Configuration json.RawMessage `json:"configuration"`
			}
			if err := json.Unmarshal(buf, &doc); err != nil {
				t.Fatalf("parse dashboard: %v", err)
			}
			if doc.Title == "" {
				t.Fatal("title is empty")
			}
			var config struct {
				Widgets map[string]struct {
					ID          string `json:"id"`
					Type        string `json:"type"`
					TypeFullFqn string `json:"typeFullFqn"`
					Config      any    `json:"config"`
				} `json:"widgets"`
				States map[string]struct {
					Layouts map[string]struct {
						Widgets map[string]any `json:"widgets"`
					} `json:"layouts"`
				} `json:"states"`
			}
			if err := json.Unmarshal(doc.Configuration, &config); err != nil {
				t.Fatalf("parse configuration: %v", err)
			}
			if len(config.Widgets) == 0 {
				t.Fatal("configuration.widgets is empty")
			}
			for widgetID, widget := range config.Widgets {
				if widget.ID != widgetID {
					t.Fatalf("widget %s id = %q, want matching id", widgetID, widget.ID)
				}
				if widget.Type == "" {
					t.Fatalf("widget %s missing type", widgetID)
				}
				if widget.TypeFullFqn == "" {
					t.Fatalf("widget %s missing typeFullFqn", widgetID)
				}
				if widget.Config == nil {
					t.Fatalf("widget %s missing config", widgetID)
				}
			}
			for stateID, state := range config.States {
				for layoutID, layout := range state.Layouts {
					for widgetID := range layout.Widgets {
						if _, ok := config.Widgets[widgetID]; !ok {
							t.Fatalf("state %s layout %s references missing widget %s", stateID, layoutID, widgetID)
						}
					}
				}
			}
		})
	}
}

// Demo seeding reports created entities to the twin registry through the
// boot-injected hook. The guard must pass the demo tenant plus the entity
// identity, and must stay nil-safe (LoadDemo runs before wiring in some
// test processes).
func TestNotifyTwinRegistryInvokesInjectedHookWithDemoTenant(t *testing.T) {
	var got []string
	TwinRegistrySync = func(tenantID, entityType, entityID string) {
		got = append(got, tenantID+"/"+entityType+"/"+entityID)
	}
	t.Cleanup(func() { TwinRegistrySync = nil })

	notifyTwinRegistry("ASSET", demoAssetID)
	notifyTwinRegistry("DEVICE", demoDeviceAID)

	want := []string{
		demoTenantID + "/ASSET/" + demoAssetID,
		demoTenantID + "/DEVICE/" + demoDeviceAID,
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("hook calls = %v, want %v", got, want)
	}

	// Nil hook must be a no-op, not a panic.
	TwinRegistrySync = nil
	notifyTwinRegistry("DEVICE", demoDeviceBID)
}
