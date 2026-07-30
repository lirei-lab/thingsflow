package alarmmaterializer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	dbpkg "flow-core/internal/db"
)

type PostgresRepository struct {
	DB *sql.DB
}

func (r PostgresRepository) FindActiveAlarm(ctx context.Context, deviceID, alarmType string) (ActiveAlarm, error) {
	var alarm ActiveAlarm
	err := r.DB.QueryRowContext(ctx, `
		SELECT id::text, originator_id::text, type, COALESCE(severity, '')
		FROM alarm
		WHERE originator_id = $1
		  AND originator_type = $2
		  AND type = $3
		  AND cleared = false
		ORDER BY start_ts DESC
		LIMIT 1`,
		deviceID, originatorTypeDevice, alarmType,
	).Scan(&alarm.ID, &alarm.DeviceID, &alarm.AlarmType, &alarm.Severity)
	return alarm, err
}

func (r PostgresRepository) InsertAlarm(ctx context.Context, alarm AlarmRecord) error {
	res, err := r.DB.ExecContext(ctx, `
		INSERT INTO alarm (
			id, created_time, start_ts, end_ts,
			type, severity, originator_id, originator_type,
			tenant_id, customer_id,
			acknowledged, cleared,
			propagate, propagate_to_owner, propagate_to_tenant,
			additional_info,
			ack_ts, clear_ts, assign_ts
		)
		SELECT $1, $2, $3, $4,
		       $5, $6, $7, $8,
		       $9, NULL,
		       false, false,
		       false, false, false,
		       $10,
		       0, 0, 0
		 WHERE EXISTS (SELECT 1 FROM device WHERE id = $7 AND tenant_id = $9)`,
		alarm.ID, alarm.CreatedTime, alarm.StartTS, alarm.EndTS,
		alarm.AlarmType, alarm.Severity, alarm.DeviceID, originatorTypeDevice,
		alarm.TenantID, alarm.AdditionalInfo,
	)
	if err != nil {
		return fmt.Errorf("insert alarm row: %w", err)
	}
	// The EXISTS guard makes this a no-op for an originator that is not a live
	// device of that tenant. Telemetry outlives the device row — GreptimeDB keeps
	// a decommissioned meter's history until retention ages it out — so anything
	// deriving alarms from the timeseries can legitimately name a device that no
	// longer exists. An alarm on a deleted device is noise, and alarm noise is
	// what teaches operators to stop reading alarms.
	if n, rerr := res.RowsAffected(); rerr == nil && n == 0 {
		return ErrOriginatorMissing
	}
	return nil
}

// ErrOriginatorMissing reports an intent whose device is not a live device of
// the named tenant. Callers treat it as a skip, not a failure: the intent was
// well-formed, its subject simply no longer exists.
var ErrOriginatorMissing = errors.New("alarm originator is not a live device of this tenant")

func (r PostgresRepository) LinkEntityAlarm(ctx context.Context, link EntityAlarmLink) error {
	_, err := r.DB.ExecContext(ctx, `
		INSERT INTO entity_alarm (tenant_id, entity_type, entity_id, created_time, alarm_type, alarm_id)
		VALUES ($1, 'DEVICE', $2, $3, $4, $5)
		ON CONFLICT (entity_id, alarm_id) DO NOTHING`,
		link.TenantID, link.DeviceID, link.CreatedTime, link.AlarmType, link.AlarmID,
	)
	if err != nil {
		return fmt.Errorf("insert entity_alarm row: %w", err)
	}
	return nil
}

func (r PostgresRepository) ExtendAlarm(ctx context.Context, alarmID string, endTS int64) error {
	_, err := r.DB.ExecContext(ctx, `UPDATE alarm SET end_ts = $1 WHERE id = $2`, endTS, alarmID)
	return err
}

func (r PostgresRepository) EscalateAlarm(ctx context.Context, alarmID, severity string, endTS int64) error {
	_, err := r.DB.ExecContext(ctx, `UPDATE alarm SET end_ts = $1, severity = $2 WHERE id = $3`, endTS, severity, alarmID)
	return err
}

func (r PostgresRepository) ClearAlarm(ctx context.Context, alarmID string, clearTS int64) error {
	_, err := r.DB.ExecContext(ctx, `
		UPDATE alarm
		SET cleared = true, clear_ts = $1, end_ts = GREATEST(COALESCE(end_ts, 0), $1)
		WHERE id = $2`,
		clearTS, alarmID,
	)
	return err
}

func (r PostgresRepository) RefreshAncestorAlarmCounts(ctx context.Context, tenantID, deviceID string) error {
	rows, err := r.DB.QueryContext(ctx, `
		WITH RECURSIVE ancestors AS (
		  SELECT from_id::text AS id, from_type AS type
		    FROM relation
		   WHERE to_id = $1 AND to_type = 'DEVICE'
		     AND relation_type_group = 'COMMON' AND relation_type = 'Contains'
		  UNION
		  SELECT rel.from_id::text, rel.from_type
		    FROM relation rel
		    JOIN ancestors a ON a.id::uuid = rel.to_id AND rel.to_type = a.type
		   WHERE rel.relation_type_group = 'COMMON' AND rel.relation_type = 'Contains'
		)
		SELECT id FROM ancestors`,
		deviceID,
	)
	if err != nil {
		return fmt.Errorf("walk alarm ancestors: %w", err)
	}
	defer rows.Close()

	var ancestors []string
	for rows.Next() {
		var assetID string
		if err := rows.Scan(&assetID); err != nil {
			return fmt.Errorf("scan alarm ancestor: %w", err)
		}
		ancestors = append(ancestors, assetID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate alarm ancestors: %w", err)
	}

	keyID := dbpkg.GetOrInsertKeyID("alarm_count")
	if keyID == -1 {
		log.Printf("WARN alarm materializer could not resolve alarm_count key id")
		return nil
	}

	now := time.Now().UnixMilli()
	for _, assetID := range ancestors {
		var count int
		err := r.DB.QueryRowContext(ctx, `
			WITH RECURSIVE descendants AS (
			  SELECT $1::uuid AS id, 'ASSET'::text AS type
			  UNION
			  SELECT rel.to_id, rel.to_type
			    FROM relation rel
			    JOIN descendants d ON d.id = rel.from_id AND d.type = rel.from_type
			   WHERE rel.relation_type_group = 'COMMON' AND rel.relation_type = 'Contains'
			)
			SELECT count(*) FROM alarm a
			 WHERE a.tenant_id = $2
			   AND a.cleared = false
			   AND a.originator_id IN (SELECT id FROM descendants WHERE type = 'DEVICE')`,
			assetID, tenantID,
		).Scan(&count)
		if err != nil {
			return fmt.Errorf("count active alarms under asset %s: %w", assetID, err)
		}
		_, err = r.DB.ExecContext(ctx, `
			INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, long_v, last_update_ts)
			VALUES ($1, 2, $2, $3, $4)
			ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET
				long_v = EXCLUDED.long_v,
				last_update_ts = EXCLUDED.last_update_ts`,
			assetID, keyID, count, now,
		)
		if err != nil {
			return fmt.Errorf("upsert alarm_count for asset %s: %w", assetID, err)
		}
	}
	return nil
}
