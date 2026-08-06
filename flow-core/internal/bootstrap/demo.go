// Demo data loader — TB classic ships an `--load-demo` install flag that
// seeds a sample customer, customer user, a handful of devices with
// access tokens, an asset, and the relations between them. flow-core
// exposes the same option through THINGSFLOW_LOAD_DEMO=true: the function
// no-ops unless that env var is set so production deployments stay
// minimal by default.
//
// Idempotent: each insert short-circuits when its target row already
// exists, so re-running the bridge with the env var on is a no-op once
// the demo set is in place.
package bootstrap

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	dbpkg "flow-core/internal/db"
)

// Deterministic UUIDs for demo entities so re-running on a clean DB
// produces the same set, and so tests / dashboards can reference them
// by ID without an extra lookup. Pattern mirrors TB's "ddd...808080"
// convention for known seeded rows.
const (
	demoTenantID = "aaaaaaaa-1dd2-11b2-8080-808080808080" // == default tenant from 98_seed-tenant.sql

	demoCustomerID      = "11111111-2222-3333-4444-555555555501"
	demoCustomerUserID  = "11111111-2222-3333-4444-555555555502"
	demoCustomerCredsID = "11111111-2222-3333-4444-555555555503"
	demoAssetID         = "11111111-2222-3333-4444-555555555510"
	demoDeviceAID       = "11111111-2222-3333-4444-555555555520"
	demoDeviceACredsID  = "11111111-2222-3333-4444-555555555521"
	demoDeviceBID       = "11111111-2222-3333-4444-555555555522"
	demoDeviceBCredsID  = "11111111-2222-3333-4444-555555555523"
	demoDeviceCID       = "11111111-2222-3333-4444-555555555524"
	demoDeviceCCredsID  = "11111111-2222-3333-4444-555555555525"
	demoDeviceDID       = "11111111-2222-3333-4444-555555555526"
	demoDeviceDCredsID  = "11111111-2222-3333-4444-555555555527"
	demoDeviceEID       = "11111111-2222-3333-4444-555555555528"
	demoDeviceECredsID  = "11111111-2222-3333-4444-555555555529"
	demoDeviceFID       = "11111111-2222-3333-4444-55555555552a"
	demoDeviceFCredsID  = "11111111-2222-3333-4444-55555555552b"
	demoDeviceGID       = "11111111-2222-3333-4444-555555555530"
	demoDeviceGCredsID  = "11111111-2222-3333-4444-555555555531"
	demoDeviceHID       = "11111111-2222-3333-4444-555555555532"
	demoDeviceHCredsID  = "11111111-2222-3333-4444-555555555533"
	demoDeviceIID       = "11111111-2222-3333-4444-555555555534"
	demoDeviceICredsID  = "11111111-2222-3333-4444-555555555535"
	demoDeviceJID       = "11111111-2222-3333-4444-555555555536"
	demoDeviceJCredsID  = "11111111-2222-3333-4444-555555555537"
	demoDeviceKID       = "11111111-2222-3333-4444-555555555538"
	demoDeviceKCredsID  = "11111111-2222-3333-4444-555555555539"
	demoFirmwarePkgID   = "11111111-2222-3333-4444-555555555540"
	demoSoftwarePkgID   = "11111111-2222-3333-4444-555555555541"
	demoDeviceLID       = "11111111-2222-3333-4444-555555555542"
	demoDeviceLCredsID  = "11111111-2222-3333-4444-555555555543"
	demoDeviceMID       = "11111111-2222-3333-4444-555555555544"
	demoDeviceMCredsID  = "11111111-2222-3333-4444-555555555545"
	demoDeviceNID       = "11111111-2222-3333-4444-555555555546"
	demoDeviceNCredsID  = "11111111-2222-3333-4444-555555555547"
	demoDeviceOID       = "11111111-2222-3333-4444-555555555548"
	demoDeviceOCredsID  = "11111111-2222-3333-4444-555555555549"
	demoDevicePID       = "11111111-2222-3333-4444-55555555554a"
	demoDevicePCredsID  = "11111111-2222-3333-4444-55555555554b"
	demoDeviceQID       = "11111111-2222-3333-4444-55555555554c"
	demoDeviceQCredsID  = "11111111-2222-3333-4444-55555555554d"
	demoDeviceRID       = "11111111-2222-3333-4444-55555555554e"
	demoDeviceRCredsID  = "11111111-2222-3333-4444-55555555554f"
)

// TwinRegistrySync is injected at boot (main.go) with internal/twin's registry
// upsert so demo-seeded devices/assets get their twin registry row on first
// boot instead of waiting for the next backfill pass. Sibling domains never
// import each other; nil until wired.
var TwinRegistrySync func(tenantID, entityType, entityID string)

// notifyTwinRegistry nil-guards the hook — demo seeding must keep working
// (and tests must not panic) when the hook is not wired.
func notifyTwinRegistry(entityType, entityID string) {
	if TwinRegistrySync != nil {
		TwinRegistrySync(demoTenantID, entityType, entityID)
	}
}

type demoDeviceSeed struct {
	id, credsID, name, deviceType, token string
	lat, lng                             float64
}

func demoDevices() []demoDeviceSeed {
	return []demoDeviceSeed{
		{demoDeviceAID, demoDeviceACredsID, "DHT22 Demo Sensor", "default", "DEMO_DHT22_TOKEN", 45.5169, -73.5680},
		{demoDeviceBID, demoDeviceBCredsID, "Raspberry Pi Demo", "default", "DEMO_RPI_TOKEN", 45.5173, -73.5673},
		// The stock Thermostats dashboard resolves devices by type
		// "thermostat"; keep this lower-case to match the dashboard JSON.
		{demoDeviceCID, demoDeviceCCredsID, "Thermostat Demo", "thermostat", "DEMO_THERMOSTAT_TOKEN", 45.5177, -73.5666},
		{demoDeviceDID, demoDeviceDCredsID, "Energy Meter Demo", "energy_meter", "DEMO_ENERGY_METER_TOKEN", 45.5181, -73.5659},
		{demoDeviceEID, demoDeviceECredsID, "Motion Sensor Demo", "motion_sensor", "DEMO_MOTION_SENSOR_TOKEN", 45.5185, -73.5652},
		{demoDeviceFID, demoDeviceFCredsID, "Air Quality Demo", "air_quality", "DEMO_AIR_QUALITY_TOKEN", 45.5189, -73.5645},
		{demoDeviceGID, demoDeviceGCredsID, "Water Tank Demo", "water_tank", "DEMO_WATER_TANK_TOKEN", 45.5193, -73.5638},
		{demoDeviceHID, demoDeviceHCredsID, "Pump Demo", "pump", "DEMO_PUMP_TOKEN", 45.5197, -73.5631},
		{demoDeviceIID, demoDeviceICredsID, "Flow Meter Demo", "flow_meter", "DEMO_FLOW_METER_TOKEN", 45.5201, -73.5624},
		{demoDeviceJID, demoDeviceJCredsID, "Pressure Sensor Demo", "pressure_sensor", "DEMO_PRESSURE_SENSOR_TOKEN", 45.5205, -73.5617},
		{demoDeviceKID, demoDeviceKCredsID, "Valve Demo", "valve", "DEMO_VALVE_TOKEN", 45.5209, -73.5610},
		{demoDeviceLID, demoDeviceLCredsID, "Office Thermostat Demo", "office_thermostat", "DEMO_OFFICE_THERMOSTAT_TOKEN", 45.5213, -73.5603},
		{demoDeviceMID, demoDeviceMCredsID, "Office Occupancy Demo", "office_occupancy", "DEMO_OFFICE_OCCUPANCY_TOKEN", 45.5217, -73.5596},
		{demoDeviceNID, demoDeviceNCredsID, "Office IAQ Demo", "office_air_quality", "DEMO_OFFICE_IAQ_TOKEN", 45.5221, -73.5589},
		{demoDeviceOID, demoDeviceOCredsID, "Office Smart Light Demo", "smart_light", "DEMO_OFFICE_LIGHT_TOKEN", 45.5225, -73.5582},
		{demoDevicePID, demoDevicePCredsID, "Office Door Access Demo", "door_access", "DEMO_OFFICE_DOOR_TOKEN", 45.5229, -73.5575},
		{demoDeviceQID, demoDeviceQCredsID, "Conference Room Meter Demo", "conference_room", "DEMO_CONFERENCE_ROOM_TOKEN", 45.5233, -73.5568},
		{demoDeviceRID, demoDeviceRCredsID, "Elevator Monitor Demo", "elevator_monitor", "DEMO_ELEVATOR_TOKEN", 45.5237, -73.5561},
	}
}

type demoOtaSeed struct {
	id, packageType, title, version, tag, fileName, contentType, payload string
}

func demoOtaPackages() []demoOtaSeed {
	return []demoOtaSeed{
		{
			id:          demoFirmwarePkgID,
			packageType: "FIRMWARE",
			title:       "Demo Thermostat Firmware",
			version:     "1.0.0",
			tag:         "classic-demo",
			fileName:    "demo-thermostat-fw.bin",
			contentType: "application/octet-stream",
			payload:     "thingsflow classic demo thermostat firmware 1.0.0\n",
		},
		{
			id:          demoSoftwarePkgID,
			packageType: "SOFTWARE",
			title:       "Demo Gateway Software",
			version:     "2026.05",
			tag:         "classic-demo",
			fileName:    "demo-gateway-sw.tar.gz",
			contentType: "application/gzip",
			payload:     "thingsflow classic demo gateway software 2026.05\n",
		},
	}
}

// LoadDemo seeds a TB-style demo dataset onto the default tenant when
// THINGSFLOW_LOAD_DEMO=true. Adds: Customer A, customer@thingsboard.org user
// (password "customer"), classic demo devices with access tokens, one
// demo asset, OTA packages, and CONTAINS relations from the asset to
// each device.
//
// Safe to call on every boot — the function checks for the demo
// customer row and short-circuits if it's already present.
func LoadDemo() {
	if !demoOptedIn() {
		return
	}
	if dbpkg.Pool == nil {
		log.Println("WARN demo: dbpkg.Pool nil — skipping demo bootstrap")
		return
	}
	// Each step uses INSERT ... ON CONFLICT DO NOTHING so re-running on
	// a populated demo set is a cheap no-op. We do NOT early-exit on
	// "customer exists" because a partial seed (e.g. fk_asset_profile
	// failure on a pre-existing tenant) needs to self-heal on the
	// next boot once the missing dependency is in place.

	// The default device + asset profile UUIDs differ between clusters:
	// fresh installs get the deterministic IDs from 98_seed-tenant.sql,
	// but pre-existing tenants get a random UUID via migration 0003. Look
	// them up at runtime so the demo dataset works on both.
	deviceProfileID, err := lookupDefaultProfileID("device_profile", demoTenantID)
	if err != nil {
		log.Printf("WARN demo: default device_profile lookup: %v", err)
		return
	}
	assetProfileID, err := lookupDefaultProfileID("asset_profile", demoTenantID)
	if err != nil {
		log.Printf("WARN demo: default asset_profile lookup: %v", err)
		return
	}

	now := time.Now().UnixMilli()

	if err := seedDemoCustomer(now); err != nil {
		log.Printf("WARN demo: seed customer: %v", err)
		return
	}
	if err := seedDemoCustomerUser(now); err != nil {
		log.Printf("WARN demo: seed customer user: %v", err)
		return
	}
	if err := seedDemoAsset(now, assetProfileID); err != nil {
		log.Printf("WARN demo: seed asset: %v", err)
		return
	}
	notifyTwinRegistry("ASSET", demoAssetID)
	for _, d := range demoDevices() {
		if err := seedDemoDevice(now, d, deviceProfileID); err != nil {
			log.Printf("WARN demo: seed device %s: %v", d.name, err)
			continue
		}
		notifyTwinRegistry("DEVICE", d.id)
		if err := seedDemoRelation(demoAssetID, d.id); err != nil {
			log.Printf("WARN demo: relation asset→%s: %v", d.name, err)
		}
		if err := seedDemoDeviceAttributes(d); err != nil {
			log.Printf("WARN demo: seed attrs %s: %v", d.name, err)
		}
	}
	for _, pkg := range demoOtaPackages() {
		if err := seedDemoOtaPackage(now, pkg, deviceProfileID); err != nil {
			log.Printf("WARN demo: seed OTA %s: %v", pkg.title, err)
		}
	}

	log.Println("demo: seeded Customer A + customer user + asset + classic devices + OTA packages on default tenant")
}

func demoOptedIn() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("THINGSFLOW_LOAD_DEMO")))
	return v == "true" || v == "1" || v == "yes"
}

func seedDemoCustomer(now int64) error {
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO customer (id, created_time, tenant_id, title, country, email, is_public)
		VALUES ($1, $2, $3, 'Customer A', 'US', 'customer@thingsboard.org', false)
		ON CONFLICT (id) DO NOTHING`,
		demoCustomerID, now, demoTenantID,
	)
	return err
}

func seedDemoCustomerUser(now int64) error {
	hash, err := bcrypt.GenerateFromPassword([]byte("customer"), 10)
	if err != nil {
		return err
	}
	if _, err := dbpkg.Pool.Exec(`
		INSERT INTO tb_user (id, created_time, tenant_id, customer_id, authority, email, first_name, last_name)
		VALUES ($1, $2, $3, $4, 'CUSTOMER_USER', 'customer@thingsboard.org', 'Customer', 'A')
		ON CONFLICT (id) DO NOTHING`,
		demoCustomerUserID, now, demoTenantID, demoCustomerID,
	); err != nil {
		return err
	}
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO user_credentials (id, created_time, enabled, password, user_id)
		VALUES ($1, $2, true, $3, $4)
		ON CONFLICT (user_id) DO NOTHING`,
		demoCustomerCredsID, now, string(hash), demoCustomerUserID,
	)
	return err
}

func seedDemoAsset(now int64, assetProfileID string) error {
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO asset (id, created_time, tenant_id, customer_id, asset_profile_id, name, type, label)
		VALUES ($1, $2, $3, $4, $5, 'Demo Building', 'default', 'Building demo')
		ON CONFLICT (id) DO NOTHING`,
		demoAssetID, now, demoTenantID, demoCustomerID, assetProfileID,
	)
	return err
}

func seedDemoDevice(now int64, d demoDeviceSeed, deviceProfileID string) error {
	if _, err := dbpkg.Pool.Exec(`
		INSERT INTO device (id, created_time, tenant_id, customer_id, device_profile_id, name, type, label)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			tenant_id = EXCLUDED.tenant_id,
			customer_id = EXCLUDED.customer_id,
			device_profile_id = EXCLUDED.device_profile_id,
			name = EXCLUDED.name,
			type = EXCLUDED.type,
			label = EXCLUDED.label`,
		d.id, now, demoTenantID, demoCustomerID, deviceProfileID, d.name, d.deviceType, d.name,
	); err != nil {
		return err
	}
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO device_credentials (id, created_time, device_id, credentials_type, credentials_id, credentials_value)
		VALUES ($1, $2, $3, 'ACCESS_TOKEN', $4, $5)
		ON CONFLICT (device_id) DO UPDATE SET
			credentials_type = EXCLUDED.credentials_type,
			credentials_id = EXCLUDED.credentials_id,
			credentials_value = EXCLUDED.credentials_value`,
		d.credsID, now, d.id, d.token, d.token,
	)
	return err
}

func seedDemoDeviceAttributes(d demoDeviceSeed) error {
	if err := setNumericAttr(d.id, "latitude", d.lat); err != nil {
		return err
	}
	if err := setNumericAttr(d.id, "longitude", d.lng); err != nil {
		return err
	}
	if d.deviceType != "thermostat" {
		if d.deviceType == "energy_meter" {
			if err := setStringAttr(d.id, "fw_title", "Demo Meter Firmware"); err != nil {
				return err
			}
			if err := setStringAttr(d.id, "fw_version", "1.0.0"); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "powerAlarmThreshold", 2500); err != nil {
				return err
			}
		}
		if d.deviceType == "motion_sensor" {
			if err := setBoolAttr(d.id, "motionAlarmFlag", true); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "luxAlarmThreshold", 15); err != nil {
				return err
			}
		}
		if d.deviceType == "air_quality" {
			if err := setNumericAttr(d.id, "co2AlarmThreshold", 1000); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "pm25AlarmThreshold", 35); err != nil {
				return err
			}
		}
		if d.deviceType == "water_tank" {
			if err := setNumericAttr(d.id, "levelAlarmThreshold", 15); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "overflowAlarmThreshold", 92); err != nil {
				return err
			}
			if err := setBoolAttr(d.id, "leakAlarmFlag", true); err != nil {
				return err
			}
		}
		if d.deviceType == "pump" {
			if err := setNumericAttr(d.id, "vibrationAlarmThreshold", 7.5); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "motorTempAlarmThreshold", 85); err != nil {
				return err
			}
			if err := setBoolAttr(d.id, "autoMode", true); err != nil {
				return err
			}
		}
		if d.deviceType == "flow_meter" {
			if err := setNumericAttr(d.id, "flowLowThreshold", 80); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "flowHighThreshold", 420); err != nil {
				return err
			}
		}
		if d.deviceType == "pressure_sensor" {
			if err := setNumericAttr(d.id, "pressureLowThreshold", 2.2); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "pressureHighThreshold", 8.5); err != nil {
				return err
			}
		}
		if d.deviceType == "valve" {
			if err := setBoolAttr(d.id, "remoteControlEnabled", true); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "positionAlarmThreshold", 95); err != nil {
				return err
			}
		}
		if d.deviceType == "office_thermostat" {
			if err := setNumericAttr(d.id, "comfortMinTemp", 20); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "comfortMaxTemp", 24); err != nil {
				return err
			}
			if err := setStringAttr(d.id, "zone", "Office floor 1"); err != nil {
				return err
			}
		}
		if d.deviceType == "office_occupancy" {
			if err := setNumericAttr(d.id, "capacity", 120); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "occupancyAlarmThreshold", 96); err != nil {
				return err
			}
		}
		if d.deviceType == "office_air_quality" {
			if err := setNumericAttr(d.id, "co2AlarmThreshold", 900); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "vocAlarmThreshold", 350); err != nil {
				return err
			}
		}
		if d.deviceType == "smart_light" {
			if err := setBoolAttr(d.id, "autoDimmingEnabled", true); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "targetLux", 420); err != nil {
				return err
			}
		}
		if d.deviceType == "door_access" {
			if err := setBoolAttr(d.id, "tailgateAlarmEnabled", true); err != nil {
				return err
			}
			if err := setStringAttr(d.id, "entryPoint", "Main lobby"); err != nil {
				return err
			}
		}
		if d.deviceType == "conference_room" {
			if err := setNumericAttr(d.id, "roomCapacity", 16); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "meetingUtilizationTarget", 70); err != nil {
				return err
			}
		}
		if d.deviceType == "elevator_monitor" {
			if err := setNumericAttr(d.id, "vibrationAlarmThreshold", 5.5); err != nil {
				return err
			}
			if err := setNumericAttr(d.id, "motorTempAlarmThreshold", 75); err != nil {
				return err
			}
		}
		return nil
	}
	if err := setStringAttr(d.id, "fw_title", "Demo Thermostat Firmware"); err != nil {
		return err
	}
	if err := setStringAttr(d.id, "fw_version", "1.0.0"); err != nil {
		return err
	}
	if err := setStringAttr(d.id, "fw_state", "UPDATED"); err != nil {
		return err
	}
	if err := setStringAttr(d.id, "sw_title", "Demo Gateway Software"); err != nil {
		return err
	}
	if err := setStringAttr(d.id, "sw_version", "2026.05"); err != nil {
		return err
	}
	if err := setStringAttr(d.id, "sw_state", "UPDATED"); err != nil {
		return err
	}
	if err := setBoolAttr(d.id, "temperatureAlarmFlag", true); err != nil {
		return err
	}
	if err := setNumericAttr(d.id, "temperatureAlarmThreshold", 28); err != nil {
		return err
	}
	if err := setBoolAttr(d.id, "humidityAlarmFlag", true); err != nil {
		return err
	}
	return setNumericAttr(d.id, "humidityAlarmThreshold", 30)
}

func seedDemoOtaPackage(now int64, pkg demoOtaSeed, deviceProfileID string) error {
	data := []byte(pkg.payload)
	sum := sha256.Sum256(data)
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO ota_package (
			id, created_time, tenant_id, device_profile_id, type, title, version, tag,
			file_name, content_type, checksum_algorithm, checksum, data, data_size, additional_info
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'SHA256', $11, $12, $13, $14)
		ON CONFLICT (tenant_id, title, version) DO UPDATE SET
			device_profile_id = EXCLUDED.device_profile_id,
			tag = EXCLUDED.tag,
			file_name = EXCLUDED.file_name,
			content_type = EXCLUDED.content_type,
			checksum_algorithm = EXCLUDED.checksum_algorithm,
			checksum = EXCLUDED.checksum,
			data = EXCLUDED.data,
			data_size = EXCLUDED.data_size,
			additional_info = EXCLUDED.additional_info`,
		pkg.id, now, demoTenantID, deviceProfileID, pkg.packageType, pkg.title, pkg.version,
		pkg.tag, pkg.fileName, pkg.contentType, hex.EncodeToString(sum[:]), data, len(data),
		`{"description":"Classic ThingsBoard demo OTA package seeded by thingsflow"}`,
	)
	return err
}

func setBoolAttr(entityID, key string, val bool) error {
	keyID := dbpkg.GetOrInsertKeyID(key)
	if keyID == -1 {
		return fmt.Errorf("key dictionary failed for %s", key)
	}
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, bool_v, last_update_ts)
		VALUES ($1, 2, $2, $3, $4)
		ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET
			bool_v = EXCLUDED.bool_v,
			last_update_ts = EXCLUDED.last_update_ts`,
		entityID, keyID, val, time.Now().UnixMilli(),
	)
	return err
}

func setNumericAttr(entityID, key string, val float64) error {
	keyID := dbpkg.GetOrInsertKeyID(key)
	if keyID == -1 {
		return fmt.Errorf("key dictionary failed for %s", key)
	}
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, dbl_v, last_update_ts)
		VALUES ($1, 2, $2, $3, $4)
		ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET
			dbl_v = EXCLUDED.dbl_v,
			last_update_ts = EXCLUDED.last_update_ts`,
		entityID, keyID, val, time.Now().UnixMilli(),
	)
	return err
}

func setStringAttr(entityID, key, val string) error {
	keyID := dbpkg.GetOrInsertKeyID(key)
	if keyID == -1 {
		return fmt.Errorf("key dictionary failed for %s", key)
	}
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, str_v, last_update_ts)
		VALUES ($1, 2, $2, $3, $4)
		ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET
			str_v = EXCLUDED.str_v,
			last_update_ts = EXCLUDED.last_update_ts`,
		entityID, keyID, val, time.Now().UnixMilli(),
	)
	return err
}

// lookupDefaultProfileID resolves the default device_profile or
// asset_profile for the given tenant. Fresh installs seed deterministic
// UUIDs (98_seed-tenant.sql); pre-existing tenants get random UUIDs
// via migration 0003 — both paths set is_default=true so we filter on
// that. Returns the row's id as a string ready to plug into FK fields.
func lookupDefaultProfileID(table, tenantID string) (string, error) {
	var id string
	err := dbpkg.Pool.QueryRow(
		`SELECT id::text FROM `+table+` WHERE tenant_id = $1 AND is_default = true LIMIT 1`,
		tenantID,
	).Scan(&id)
	return id, err
}

func seedDemoRelation(assetID, deviceID string) error {
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type_group, relation_type)
		VALUES ($1, 'ASSET', $2, 'DEVICE', 'COMMON', 'Contains')
		ON CONFLICT DO NOTHING`,
		assetID, deviceID,
	)
	return err
}
