package alarmmaterializer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

const originatorTypeDevice = 5

type Repository interface {
	FindActiveAlarm(ctx context.Context, deviceID, alarmType string) (ActiveAlarm, error)
	InsertAlarm(ctx context.Context, alarm AlarmRecord) error
	LinkEntityAlarm(ctx context.Context, link EntityAlarmLink) error
	ExtendAlarm(ctx context.Context, alarmID string, endTS int64) error
	EscalateAlarm(ctx context.Context, alarmID, severity string, endTS int64) error
	ClearAlarm(ctx context.Context, alarmID string, clearTS int64) error
	RefreshAncestorAlarmCounts(ctx context.Context, tenantID, deviceID string) error
}

type ActiveAlarm struct {
	ID        string
	DeviceID  string
	AlarmType string
	Severity  string
}

type AlarmRecord struct {
	ID             string
	TenantID       string
	DeviceID       string
	AlarmType      string
	Severity       string
	CreatedTime    int64
	StartTS        int64
	EndTS          int64
	AdditionalInfo string
}

type EntityAlarmLink struct {
	TenantID    string
	DeviceID    string
	CreatedTime int64
	AlarmType   string
	AlarmID     string
}

type ApplyResult struct {
	Action  string
	AlarmID string
}

func ApplyIntent(ctx context.Context, repo Repository, intent Intent, now int64, newID func() string) (ApplyResult, error) {
	if err := intent.Validate(); err != nil {
		return ApplyResult{}, err
	}

	active, err := repo.FindActiveAlarm(ctx, intent.DeviceID, intent.AlarmType)
	if err != nil && err != sql.ErrNoRows {
		return ApplyResult{}, fmt.Errorf("find active alarm: %w", err)
	}

	if intent.Action == ActionClear {
		if err == sql.ErrNoRows {
			return ApplyResult{Action: "noop"}, nil
		}
		if err := repo.ClearAlarm(ctx, active.ID, now); err != nil {
			return ApplyResult{}, fmt.Errorf("clear alarm: %w", err)
		}
		if err := repo.RefreshAncestorAlarmCounts(ctx, intent.TenantID, intent.DeviceID); err != nil {
			return ApplyResult{}, fmt.Errorf("refresh alarm counts: %w", err)
		}
		return ApplyResult{Action: "cleared", AlarmID: active.ID}, nil
	}

	if err == nil {
		if severityRank(intent.Severity) > severityRank(active.Severity) {
			if err := repo.EscalateAlarm(ctx, active.ID, intent.Severity, now); err != nil {
				return ApplyResult{}, fmt.Errorf("escalate alarm: %w", err)
			}
			if err := repo.RefreshAncestorAlarmCounts(ctx, intent.TenantID, intent.DeviceID); err != nil {
				return ApplyResult{}, fmt.Errorf("refresh alarm counts: %w", err)
			}
			return ApplyResult{Action: "escalated", AlarmID: active.ID}, nil
		}
		if err := repo.ExtendAlarm(ctx, active.ID, now); err != nil {
			return ApplyResult{}, fmt.Errorf("extend alarm: %w", err)
		}
		return ApplyResult{Action: "extended", AlarmID: active.ID}, nil
	}

	alarmID := newID()
	info, err := additionalInfoJSON(intent)
	if err != nil {
		return ApplyResult{}, err
	}
	alarm := AlarmRecord{
		ID:             alarmID,
		TenantID:       intent.TenantID,
		DeviceID:       intent.DeviceID,
		AlarmType:      intent.AlarmType,
		Severity:       intent.Severity,
		CreatedTime:    now,
		StartTS:        now,
		EndTS:          now,
		AdditionalInfo: info,
	}
	if err := repo.InsertAlarm(ctx, alarm); err != nil {
		// A well-formed intent for a device that no longer exists is a skip, not
		// a failure: returning an error here would nack the message and have
		// JetStream redeliver it up to MaxDeliver times, wedging the consumer on
		// something that can never succeed.
		if errors.Is(err, ErrOriginatorMissing) {
			return ApplyResult{Action: "noop"}, nil
		}
		return ApplyResult{}, fmt.Errorf("insert alarm: %w", err)
	}
	if err := repo.LinkEntityAlarm(ctx, EntityAlarmLink{
		TenantID:    intent.TenantID,
		DeviceID:    intent.DeviceID,
		CreatedTime: now,
		AlarmType:   intent.AlarmType,
		AlarmID:     alarmID,
	}); err != nil {
		return ApplyResult{}, fmt.Errorf("link entity alarm: %w", err)
	}
	if err := repo.RefreshAncestorAlarmCounts(ctx, intent.TenantID, intent.DeviceID); err != nil {
		return ApplyResult{}, fmt.Errorf("refresh alarm counts: %w", err)
	}
	return ApplyResult{Action: "created", AlarmID: alarmID}, nil
}

func additionalInfoJSON(intent Intent) (string, error) {
	info := map[string]any{
		"source": "bento-nats-alarms",
	}
	if intent.TS > 0 {
		info["eventTs"] = intent.TS
	}
	if len(intent.Details) > 0 {
		info["details"] = intent.Details
	}
	data, err := json.Marshal(info)
	if err != nil {
		return "", fmt.Errorf("marshal additional info: %w", err)
	}
	return string(data), nil
}

func severityRank(severity string) int {
	switch severity {
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
