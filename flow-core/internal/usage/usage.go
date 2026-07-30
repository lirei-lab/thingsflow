// Package usage tracks per-tenant ingestion / processing metrics that
// the "Utilisation de l'API" dashboard reads from api_usage_state
// telemetry. Mirrors TB-Java's UsageStatsService: counters tick on
// every transport message + rule eval, get persisted as ts_kv points
// once per minute, hourly counters reset on the wall clock.
package usage

import (
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
)

type tenantUsage struct {
	transportMsg            atomic.Int64 // cumulative this hour cycle
	transportMsgHourly      atomic.Int64 // resets every hour
	transportDataPoints     atomic.Int64
	transportDataPointsHour atomic.Int64
	ruleEngineExec          atomic.Int64
	ruleEngineExecHour      atomic.Int64
	storageDataPoints       atomic.Int64
	storageDataPointsHour   atomic.Int64
}

var (
	mu       sync.RWMutex
	byTenant = map[string]*tenantUsage{}
)

func usageFor(tenantID string) *tenantUsage {
	if tenantID == "" {
		return nil
	}
	mu.RLock()
	u, ok := byTenant[tenantID]
	mu.RUnlock()
	if ok {
		return u
	}
	mu.Lock()
	defer mu.Unlock()
	if u, ok := byTenant[tenantID]; ok {
		return u
	}
	u = &tenantUsage{}
	byTenant[tenantID] = u
	return u
}

// RecordTransportMessage is invoked once per accepted device payload.
// dataPoints is the count of leaf metric values that the payload
// contributed to storage (telemetry keys + attribute keys).
func RecordTransportMessage(tenantID string, dataPoints int) {
	u := usageFor(tenantID)
	if u == nil {
		return
	}
	u.transportMsg.Add(1)
	u.transportMsgHourly.Add(1)
	u.transportDataPoints.Add(int64(dataPoints))
	u.transportDataPointsHour.Add(int64(dataPoints))
	u.storageDataPoints.Add(int64(dataPoints))
	u.storageDataPointsHour.Add(int64(dataPoints))
}

// RecordRuleEngineExecution is incremented every time the Flow alarm
// engine or rule engine evaluates a payload.
func RecordRuleEngineExecution(tenantID string) {
	u := usageFor(tenantID)
	if u == nil {
		return
	}
	u.ruleEngineExec.Add(1)
	u.ruleEngineExecHour.Add(1)
}

// ensureApiUsageStates creates the api_usage_state row for every
// tenant that doesn't have one. Stock TB populates this lazily inside
// its ApiUsageStateService when the first transport message arrives;
// we provision it eagerly at boot so the "Utilisation de l'API"
// dashboard finds an entity to bind its widgets to even before any
// device sends data.
func ensureApiUsageStates() {
	if dbpkg.Pool == nil {
		return
	}
	rows, err := dbpkg.Pool.Query("SELECT id FROM tenant")
	if err != nil {
		log.Printf("WARN UsageReporter list tenants: %v", err)
		return
	}
	defer rows.Close()
	now := time.Now().UnixMilli()
	created := 0
	for rows.Next() {
		var tenantID string
		if err := rows.Scan(&tenantID); err != nil {
			continue
		}
		res, err := dbpkg.Pool.Exec(`
			INSERT INTO api_usage_state (
				id, created_time, tenant_id, entity_type, entity_id,
				transport, db_storage, re_exec, js_exec, tbel_exec,
				email_exec, sms_exec, alarm_exec, version
			)
			VALUES ($1, $2, $3, 'TENANT', $3,
			        'ENABLED','ENABLED','ENABLED','ENABLED','ENABLED',
			        'ENABLED','ENABLED','ENABLED', 1)
			ON CONFLICT (tenant_id, entity_id) DO NOTHING`,
			uuid.New().String(), now, tenantID,
		)
		if err != nil {
			log.Printf("WARN api_usage_state insert tenant=%s: %v", tenantID, err)
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			created++
		}
	}
	if created > 0 {
		log.Printf("UsageReporter: provisioned api_usage_state for %d tenant(s)", created)
	}
}

// StartReporter spawns the background goroutine that persists usage
// counters to api_usage_state's ts_kv stream.
//
//   - every minute  → snapshot of counters + active/inactive device counts
//   - every hour    → reset *Hourly counters back to zero
func StartReporter() {
	if dbpkg.Pool == nil {
		log.Println("UsageReporter disabled: dbpkg.Pool nil")
		return
	}
	ensureApiUsageStates()
	go func() {
		time.Sleep(2 * time.Second)
		writeSnapshot()

		now := time.Now()
		nextMinute := now.Truncate(time.Minute).Add(time.Minute)
		nextHour := now.Truncate(time.Hour).Add(time.Hour)
		time.AfterFunc(time.Until(nextMinute), func() {
			writeSnapshot()
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				writeSnapshot()
			}
		})
		time.AfterFunc(time.Until(nextHour), func() {
			resetHourly()
			hour := time.NewTicker(time.Hour)
			defer hour.Stop()
			for range hour.C {
				resetHourly()
			}
		})
	}()
	log.Println("UsageReporter started: snapshotting api_usage_state every minute, hourly reset on the hour")
}

func resetHourly() {
	mu.RLock()
	defer mu.RUnlock()
	for _, u := range byTenant {
		u.transportMsgHourly.Store(0)
		u.transportDataPointsHour.Store(0)
		u.ruleEngineExecHour.Store(0)
		u.storageDataPointsHour.Store(0)
	}
}

func writeSnapshot() {
	rows, err := dbpkg.Pool.Query("SELECT id, tenant_id FROM api_usage_state")
	if err != nil {
		log.Printf("WARN UsageReporter list api_usage_state: %v", err)
		return
	}
	defer rows.Close()

	type tenantRow struct{ id, tenantID string }
	var stateRows []tenantRow
	for rows.Next() {
		var t tenantRow
		if err := rows.Scan(&t.id, &t.tenantID); err == nil {
			stateRows = append(stateRows, t)
		}
	}

	now := time.Now().UnixMilli()
	for _, s := range stateRows {
		u := usageFor(s.tenantID)
		if u == nil {
			continue
		}
		active, inactive := countActive(s.tenantID)

		points := map[string]int64{
			"transportMsgCount":              u.transportMsg.Load(),
			"transportMsgCountHourly":        u.transportMsgHourly.Load(),
			"transportDataPointsCount":       u.transportDataPoints.Load(),
			"transportDataPointsCountHourly": u.transportDataPointsHour.Load(),
			"storageDataPointsCount":         u.storageDataPoints.Load(),
			"storageDataPointsCountHourly":   u.storageDataPointsHour.Load(),
			"ruleEngineExecutionCount":       u.ruleEngineExec.Load(),
			"ruleEngineExecutionCountHourly": u.ruleEngineExecHour.Load(),
			"jsExecutionCount":               0,
			"jsExecutionCountHourly":         0,
			"tbelExecutionCount":             0,
			"tbelExecutionCountHourly":       0,
			"emailCount":                     0,
			"emailCountHourly":               0,
			"smsCount":                       0,
			"smsCountHourly":                 0,
			"createdAlarmsCount":             countAlarms(s.tenantID),
			"createdAlarmsCountHourly":       0,
			"activeDevicesCount":             active,
			"activeDevicesCountHourly":       active,
			"inactiveDevicesCount":           inactive,
			"inactiveDevicesCountHourly":     inactive,
		}
		for key, val := range points {
			persistPoint(s.id, s.tenantID, key, val, now)
		}

		// flow-core implements transport, db storage, rule engine,
		// alarms; JS/TBEL user code + email + SMS aren't in scope.
		// Reporting them as DISABLED matches reality.
		apiStates := map[string]string{
			"transportApiState":     "ENABLED",
			"dbApiState":            "ENABLED",
			"ruleEngineApiState":    "ENABLED",
			"alarmApiState":         "ENABLED",
			"jsExecutionApiState":   "DISABLED",
			"tbelExecutionApiState": "DISABLED",
			"emailApiState":         "DISABLED",
			"smsApiState":           "DISABLED",
		}
		for key, val := range apiStates {
			persistString(s.id, s.tenantID, key, val, now)
		}
	}
}

// persistPoint publishes one numeric usage counter to the entity telemetry
// subject (D-02 producer path). entityID = api_usage_state.id (the row id,
// NOT the tenant_id — Pitfall 2); tenantID is carried as its own field so the
// ILP tenant_id tag is populated for tenant-scoped reads. No Postgres write
// remains here — flow-core touches only the message bus (CLAUDE.md no-Go-in-
// the-DB-write-path rule).
func persistPoint(entityID, tenantID, key string, value int64, ts int64) {
	publishEntityPoint(tenantID, "API_USAGE_STATE", entityID, key, strconv.FormatInt(value, 10), "number", ts)
}

// persistString publishes one api-state label (ENABLED/DISABLED). value_kind
// is always "string" so a state label can never be returned to the dashboard
// as a numeric 0/NaN (T-03-06 / Pitfall 4).
func persistString(entityID, tenantID, key, value string, ts int64) {
	publishEntityPoint(tenantID, "API_USAGE_STATE", entityID, key, value, "string", ts)
}

func countAlarms(tenantID string) int64 {
	var n int64
	_ = dbpkg.Pool.QueryRow("SELECT count(*) FROM alarm WHERE tenant_id = $1", tenantID).Scan(&n)
	return n
}

func countActive(tenantID string) (int64, int64) {
	var active, total int64
	_ = dbpkg.Pool.QueryRow(
		`SELECT count(DISTINCT d.id) FROM device d
		 JOIN attribute_kv a ON a.entity_id = d.id
		 JOIN key_dictionary k ON k.key_id = a.attribute_key
		 WHERE d.tenant_id = $1 AND k.key = 'active' AND a.bool_v = true`,
		tenantID).Scan(&active)
	_ = dbpkg.Pool.QueryRow("SELECT count(*) FROM device WHERE tenant_id = $1", tenantID).Scan(&total)
	return active, total - active
}
