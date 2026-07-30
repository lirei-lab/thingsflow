package main

import (
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	dbpkg "flow-core/internal/db"
)

// DeviceState tracks the last activity timestamp for inactivity detection.
type DeviceState struct {
	DeviceID     string
	TenantID     string
	LastActivity time.Time
	Active       bool
}

var (
	deviceStates   = make(map[string]*DeviceState) // deviceId -> state
	deviceStatesMu sync.RWMutex

	// Default inactivity timeout (can be overridden per-device via shared attributes later)
	defaultInactivityTimeout = 60 * time.Second
)

// HandleDeviceConnect processes a DEVICE_CONNECT event.
// It sets the `active` server attribute to true and records the connection time.
func HandleDeviceConnect(tenantId, deviceId string) {
	deviceStatesMu.Lock()
	deviceStates[deviceId] = &DeviceState{
		DeviceID:     deviceId,
		TenantID:     tenantId,
		LastActivity: time.Now(),
		Active:       true,
	}
	deviceStatesMu.Unlock()

	// Update SERVER_SCOPE attribute `active` = true
	SaveServerAttribute(tenantId, deviceId, "active", true)
	SaveServerAttribute(tenantId, deviceId, "lastConnectTime", time.Now().UnixMilli())

	slog.Debug("device connected",
		slog.String("device_id", deviceId),
		slog.String("tenant_id", tenantId),
	)
}

// HandleDeviceDisconnect processes a DEVICE_DISCONNECT event.
// It sets the `active` server attribute to false and records the disconnect time.
func HandleDeviceDisconnect(tenantId, deviceId string) {
	deviceStatesMu.Lock()
	if state, ok := deviceStates[deviceId]; ok {
		state.Active = false
	}
	deviceStatesMu.Unlock()

	// Update SERVER_SCOPE attribute `active` = false
	SaveServerAttribute(tenantId, deviceId, "active", false)
	SaveServerAttribute(tenantId, deviceId, "lastDisconnectTime", time.Now().UnixMilli())

	slog.Debug("device disconnected",
		slog.String("device_id", deviceId),
		slog.String("tenant_id", tenantId),
	)
}

// UpdateDeviceActivity is called on every telemetry message to track last activity.
func UpdateDeviceActivity(deviceId string) {
	deviceStatesMu.Lock()
	if state, ok := deviceStates[deviceId]; ok {
		state.LastActivity = time.Now()
	}
	deviceStatesMu.Unlock()
}

// SaveServerAttribute saves a single key-value pair as a SERVER_SCOPE attribute (attribute_type=2).
func SaveServerAttribute(tenantId, deviceId, key string, value interface{}) {
	if dbpkg.Pool == nil {
		return
	}

	keyId := dbpkg.GetOrInsertKeyID(key)
	if keyId == -1 {
		return
	}

	ts := time.Now().UnixMilli()

	var boolV *bool
	var strV *string
	var longV *int64
	var dblV *float64
	var jsonV *string

	switch v := value.(type) {
	case bool:
		boolV = &v
	case int:
		val := int64(v)
		longV = &val
	case int64:
		longV = &v
	case float64:
		dblV = &v
	case string:
		strV = &v
	default:
		s := fmt.Sprintf("%v", v)
		strV = &s
	}

	// attribute_type = 2 is SERVER_SCOPE
	query := `INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, bool_v, str_v, long_v, dbl_v, json_v, last_update_ts) 
			  VALUES ($1, 2, $2, $3, $4, $5, $6, $7, $8) 
			  ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET 
			  bool_v = EXCLUDED.bool_v, str_v = EXCLUDED.str_v, long_v = EXCLUDED.long_v, 
			  dbl_v = EXCLUDED.dbl_v, json_v = EXCLUDED.json_v, last_update_ts = EXCLUDED.last_update_ts`

	_, err := dbpkg.Pool.Exec(query, deviceId, keyId, boolV, strV, longV, dblV, jsonV, ts)
	if err != nil {
		log.Printf("WARN: Failed to save server attribute '%s' for device %s: %v", key, deviceId, err)
	}
}

// StartInactivityMonitor periodically checks all tracked devices for inactivity.
// If a device hasn't sent data within the timeout, an INACTIVITY alarm is raised.
func StartInactivityMonitor() {
	timeoutStr := getEnv("DEVICE_INACTIVITY_TIMEOUT_SECONDS", "60")
	var timeoutSec int
	fmt.Sscanf(timeoutStr, "%d", &timeoutSec)
	if timeoutSec <= 0 {
		timeoutSec = 60
	}
	defaultInactivityTimeout = time.Duration(timeoutSec) * time.Second

	checkInterval := defaultInactivityTimeout / 2
	if checkInterval < 10*time.Second {
		checkInterval = 10 * time.Second
	}

	log.Printf("Inactivity Monitor started. Timeout=%v, Check interval=%v", defaultInactivityTimeout, checkInterval)

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		deviceStatesMu.RLock()
		// Snapshot the state to avoid holding the lock during DB operations
		snapshot := make(map[string]*DeviceState, len(deviceStates))
		for k, v := range deviceStates {
			snapshot[k] = &DeviceState{
				DeviceID:     v.DeviceID,
				TenantID:     v.TenantID,
				LastActivity: v.LastActivity,
				Active:       v.Active,
			}
		}
		deviceStatesMu.RUnlock()

		for _, state := range snapshot {
			if !state.Active {
				continue // Already disconnected, alarm should already be active
			}

			if now.Sub(state.LastActivity) > defaultInactivityTimeout {
				// Device is active but hasn't sent data — raise inactivity alarm
				decision := &AlarmDecision{
					CreateAlarm: true,
					AlarmType:   "INACTIVITY",
					Severity:    "MAJOR",
					Details:     fmt.Sprintf("Device %s has been inactive for over %v", state.DeviceID, defaultInactivityTimeout),
				}
				go ProcessAlarmDecision(state.TenantID, state.DeviceID, decision)

				// Also update active=false since the device is effectively dead
				SaveServerAttribute(state.TenantID, state.DeviceID, "active", false)
				SaveServerAttribute(state.TenantID, state.DeviceID, "inactivityAlarmTime", time.Now().UnixMilli())

				deviceStatesMu.Lock()
				if s, ok := deviceStates[state.DeviceID]; ok {
					s.Active = false
				}
				deviceStatesMu.Unlock()

				log.Printf("INACTIVITY detected for device %s (last activity: %v)", state.DeviceID, state.LastActivity)
			}
		}
	}
}
