package alarmmaterializer

import (
	"encoding/json"
	"fmt"
)

const (
	ActionCreateOrUpdate = "create_or_update"
	ActionClear          = "clear"
)

type Intent struct {
	Action    string         `json:"action"`
	TenantID  string         `json:"tenantId"`
	DeviceID  string         `json:"deviceId"`
	AlarmType string         `json:"alarmType"`
	Severity  string         `json:"severity,omitempty"`
	TS        int64          `json:"ts,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

func ParseIntent(data []byte) (Intent, error) {
	var intent Intent
	if err := json.Unmarshal(data, &intent); err != nil {
		return Intent{}, fmt.Errorf("parse alarm intent: %w", err)
	}
	if intent.Details == nil {
		intent.Details = map[string]any{}
	}
	if err := intent.Validate(); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

func (i Intent) Validate() error {
	if i.Action != ActionCreateOrUpdate && i.Action != ActionClear {
		return fmt.Errorf("invalid alarm action %q", i.Action)
	}
	if i.TenantID == "" {
		return fmt.Errorf("missing tenantId")
	}
	if i.DeviceID == "" {
		return fmt.Errorf("missing deviceId")
	}
	if i.AlarmType == "" {
		return fmt.Errorf("missing alarmType")
	}
	if i.Action == ActionCreateOrUpdate && i.Severity == "" {
		return fmt.Errorf("missing severity for create_or_update")
	}
	return nil
}
