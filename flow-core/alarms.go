package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/ws"
)

// ThingsBoard Java enum EntityType ordinal for DEVICE is 5
// (0 = TENANT, 1 = CUSTOMER, 2 = USER, 3 = DASHBOARD, 4 = ASSET, 5 = DEVICE)
const originatorTypeDevice = 5

func alarmEventLogLevel() slog.Level {
	switch strings.ToLower(os.Getenv("FLOW_ALARM_EVENT_LOG_LEVEL")) {
	case "debug":
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

func logAlarmEvent(message string, attrs ...any) {
	if alarmEventLogLevel() == slog.LevelDebug {
		slog.Debug(message, attrs...)
		return
	}
	slog.Info(message, attrs...)
}

// severityRank maps an alarm severity to a numeric rank so we can detect
// escalations when the same alarm is re-evaluated against new telemetry.
// Higher number = more severe. Unknown values rank below WARNING so the
// extend path still updates them when a real severity arrives.
func severityRank(s string) int {
	switch s {
	case "CRITICAL":
		return 4
	case "MAJOR":
		return 3
	case "MINOR":
		return 2
	case "WARNING":
		return 1
	case "INDETERMINATE":
		return 0
	default:
		return -1
	}
}

// UpsertAlarm creates a new alarm or updates the end timestamp of an existing active alarm.
// Returns the alarm UUID so we can broadcast it via WebSocket.
func UpsertAlarm(tenantId, deviceId string, decision *AlarmDecision) (string, error) {
	if dbpkg.Pool == nil {
		return "", fmt.Errorf("postgres not initialized")
	}

	// 1. Check for an existing ACTIVE alarm of this type for this device.
	// We look up by alarm.originator_id directly instead of JOINing through
	// entity_alarm — the entity_alarm row is inserted in a separate
	// statement after the alarm row, so back-to-back publishes can race
	// each other and both see "no existing alarm" via the JOIN. The
	// originator_id column on the alarm row is populated atomically with
	// the INSERT, so reading from it eliminates the race.
	var existingAlarmId string
	var existingStartTs int64
	var existingSeverity string
	err := dbpkg.Pool.QueryRow(`
		SELECT id, start_ts, COALESCE(severity, '')
		FROM alarm
		WHERE originator_id = $1
		  AND originator_type = $2
		  AND type = $3
		  AND cleared = false
		ORDER BY start_ts DESC
		LIMIT 1`,
		deviceId, originatorTypeDevice, decision.AlarmType,
	).Scan(&existingAlarmId, &existingStartTs, &existingSeverity)

	now := time.Now().UnixMilli()

	if err == nil {
		// Alarm already active — extend it. If the new decision carries a
		// higher severity, escalate; otherwise just bump end_ts. We never
		// downgrade an active alarm — that's what ClearAlarm is for.
		newSeverity := decision.Severity
		if newSeverity != "" && severityRank(newSeverity) > severityRank(existingSeverity) {
			_, err = dbpkg.Pool.Exec(
				`UPDATE alarm SET end_ts = $1, severity = $2 WHERE id = $3`,
				now, newSeverity, existingAlarmId,
			)
			if err != nil {
				return "", fmt.Errorf("failed to escalate alarm %s: %w", existingAlarmId, err)
			}
			logAlarmEvent("alarm escalated",
				slog.String("alarm_id", existingAlarmId),
				slog.String("type", decision.AlarmType),
				slog.String("device_id", deviceId),
				slog.String("from", existingSeverity),
				slog.String("to", newSeverity),
			)
			return existingAlarmId, nil
		}
		_, err = dbpkg.Pool.Exec(`UPDATE alarm SET end_ts = $1 WHERE id = $2`, now, existingAlarmId)
		if err != nil {
			return "", fmt.Errorf("failed to extend alarm %s: %w", existingAlarmId, err)
		}
		// Dedup heartbeat — fires on every continuing-alarm telemetry.
		// Continuous and high-frequency, so DEBUG. The state-transition
		// lines (CREATED, CLEARED) stay at INFO above and below.
		slog.Debug("alarm extended",
			slog.String("alarm_id", existingAlarmId),
			slog.String("type", decision.AlarmType),
			slog.String("device_id", deviceId),
			slog.String("severity", existingSeverity),
		)
		return existingAlarmId, nil
	}

	if err != sql.ErrNoRows {
		return "", fmt.Errorf("failed to query existing alarm: %w", err)
	}

	// 2. No active alarm found — create a new one
	alarmId := uuid.New().String()

	detailsJSON := "{}"
	if decision.Details != "" {
		// ThingsBoard requires additional_info to be a valid JSON object (not a raw string or unquoted text)
		obj := map[string]string{"message": decision.Details}
		bytes, _ := json.Marshal(obj)
		detailsJSON = string(bytes)
	}

	severity := "WARNING"
	if decision.Severity != "" {
		severity = decision.Severity
	}

	// Insert into alarm table
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO alarm (
			id, created_time, start_ts, end_ts,
			type, severity, originator_id, originator_type,
			tenant_id, customer_id,
			acknowledged, cleared,
			propagate, propagate_to_owner, propagate_to_tenant,
			additional_info,
			ack_ts, clear_ts, assign_ts
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8,
			$9, NULL,
			false, false,
			false, false, false,
			$10,
			0, 0, 0
		)`,
		alarmId, now, now, now,
		decision.AlarmType, severity, deviceId, originatorTypeDevice,
		tenantId,
		detailsJSON,
	)
	if err != nil {
		return "", fmt.Errorf("failed to insert alarm: %w", err)
	}

	// Insert into entity_alarm linking table
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO entity_alarm (tenant_id, entity_type, entity_id, created_time, alarm_type, alarm_id)
		VALUES ($1, 'DEVICE', $2, $3, $4, $5)`,
		tenantId, deviceId, now, decision.AlarmType, alarmId,
	)
	if err != nil {
		return "", fmt.Errorf("failed to insert entity_alarm link: %w", err)
	}

	logAlarmEvent("alarm created",
		slog.String("alarm_id", alarmId),
		slog.String("type", decision.AlarmType),
		slog.String("severity", severity),
		slog.String("device_id", deviceId),
	)
	return alarmId, nil
}

// ClearAlarm marks an active alarm as cleared in the database.
// Returns the alarm UUID that was cleared.
func ClearAlarm(tenantId, deviceId string, alarmType string) (string, error) {
	if dbpkg.Pool == nil {
		return "", fmt.Errorf("postgres not initialized")
	}

	var alarmId string
	err := dbpkg.Pool.QueryRow(`
		SELECT id FROM alarm
		WHERE originator_id = $1
		  AND originator_type = $2
		  AND type = $3
		  AND cleared = false
		ORDER BY start_ts DESC
		LIMIT 1`,
		deviceId, originatorTypeDevice, alarmType,
	).Scan(&alarmId)

	if err == sql.ErrNoRows {
		// Nothing to clear — silent no-op
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to find alarm to clear: %w", err)
	}

	now := time.Now().UnixMilli()
	_, err = dbpkg.Pool.Exec(`
		UPDATE alarm SET cleared = true, clear_ts = $1
		WHERE id = $2`, now, alarmId)
	if err != nil {
		return "", fmt.Errorf("failed to clear alarm %s: %w", alarmId, err)
	}

	logAlarmEvent("alarm cleared",
		slog.String("alarm_id", alarmId),
		slog.String("type", alarmType),
		slog.String("device_id", deviceId),
	)
	return alarmId, nil
}

// propagateAlarmCount walks UP from the device through Contains relations
// to every ancestor (zone → floor → building → district) and writes
// the live count of active alarms below each one as the SERVER_SCOPE
// `alarm_count` attribute. Map markers + entity_count widgets pick
// this up so the visual cue (red building, raised count) stays in
// sync with reality without requiring the UI to query alarms per
// asset on every refresh. Best-effort — failures are logged but do
// not block the alarm flow.
func propagateAlarmCount(tenantId, deviceId string) {
	if dbpkg.Pool == nil {
		return
	}
	rows, err := dbpkg.Pool.Query(`
		WITH RECURSIVE ancestors AS (
		  SELECT from_id::text AS id, from_type AS type
		    FROM relation
		   WHERE to_id = $1 AND to_type = 'DEVICE'
		     AND relation_type_group = 'COMMON' AND relation_type = 'Contains'
		  UNION
		  SELECT r.from_id::text, r.from_type
		    FROM relation r
		    JOIN ancestors a ON a.id::uuid = r.to_id AND r.to_type = a.type
		   WHERE r.relation_type_group = 'COMMON' AND r.relation_type = 'Contains'
		)
		SELECT id FROM ancestors
	`, deviceId)
	if err != nil {
		log.Printf("WARN propagateAlarmCount walk-up: %v", err)
		return
	}
	defer rows.Close()
	var ancestors []string
	for rows.Next() {
		var aid string
		if err := rows.Scan(&aid); err == nil {
			ancestors = append(ancestors, aid)
		}
	}
	for _, aid := range ancestors {
		// Count alarms on devices reachable downward from this ancestor.
		var count int
		err := dbpkg.Pool.QueryRow(`
			WITH RECURSIVE descendants AS (
			  SELECT $1::uuid AS id, 'ASSET'::text AS type
			  UNION
			  SELECT r.to_id, r.to_type
			    FROM relation r
			    JOIN descendants d ON d.id = r.from_id AND d.type = r.from_type
			   WHERE r.relation_type_group = 'COMMON' AND r.relation_type = 'Contains'
			)
			SELECT count(*) FROM alarm a
			 WHERE a.cleared = false
			   AND a.originator_id IN (SELECT id FROM descendants WHERE type = 'DEVICE')
		`, aid).Scan(&count)
		if err != nil {
			log.Printf("WARN propagateAlarmCount count for %s: %v", aid[:8], err)
			continue
		}
		// Upsert SERVER_SCOPE attribute alarm_count = count on this ancestor.
		keyId := dbpkg.GetOrInsertKeyID("alarm_count")
		if keyId == -1 {
			continue
		}
		_, _ = dbpkg.Pool.Exec(`
			INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, long_v, last_update_ts)
			VALUES ($1, 2, $2, $3, $4)
			ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET
				long_v = EXCLUDED.long_v,
				last_update_ts = EXCLUDED.last_update_ts`,
			aid, keyId, count, time.Now().UnixMilli(),
		)
	}
}

// ProcessAlarmDecision orchestrates alarm creation or clearing based on a Flow decision.
// It also broadcasts the alarm event to any connected WebSocket sessions.
func ProcessAlarmDecision(tenantId, deviceId string, decision *AlarmDecision) {
	if decision == nil {
		return
	}

	var alarmId string
	var err error

	if decision.CreateAlarm {
		alarmId, err = UpsertAlarm(tenantId, deviceId, decision)
		if err != nil {
			log.Printf("ERROR ProcessAlarmDecision (Create): device=%s %v", deviceId, err)
			return
		}
		propagateAlarmCount(tenantId, deviceId)

		// Broadcast alarm event to subscribed WS sessions
		if alarmId != "" {
			event := map[string]interface{}{
				"alarmId":  alarmId,
				"type":     decision.AlarmType,
				"severity": decision.Severity,
				"status":   "ACTIVE",
				"deviceId": deviceId,
				"tenantId": tenantId,
			}
			ws.BroadcastAlarmEvent(deviceId, event)
		}
	}

	if decision.ClearAlarm {
		alarmId, err = ClearAlarm(tenantId, deviceId, decision.AlarmType)
		if err != nil {
			log.Printf("ERROR ProcessAlarmDecision (Clear): device=%s %v", deviceId, err)
			return
		}
		propagateAlarmCount(tenantId, deviceId)

		if alarmId != "" {
			event := map[string]interface{}{
				"alarmId":  alarmId,
				"type":     decision.AlarmType,
				"status":   "CLEARED",
				"deviceId": deviceId,
				"tenantId": tenantId,
			}
			slog.Debug("alarm clear event",
				slog.String("alarm_id", alarmId),
				slog.String("type", decision.AlarmType),
				slog.String("device_id", deviceId),
				slog.String("tenant_id", tenantId),
			)
			ws.BroadcastAlarmEvent(deviceId, event)
		}
	}
}
