package main

type TbMessage struct {
	TenantId string                 `json:"tenantId"`
	DeviceId string                 `json:"deviceId"`
	Data     map[string]interface{} `json:"data"`
}

// AlarmDecision is the normalized alarm intent consumed by the alarm lifecycle
// writer. Bento creates these intents from NATS telemetry events; flow-core is
// responsible only for materializing them into dashboard-compatible tables.
type AlarmDecision struct {
	CreateAlarm bool                   `json:"createAlarm"`
	ClearAlarm  bool                   `json:"clearAlarm"`
	AlarmType   string                 `json:"alarmType"`
	Severity    string                 `json:"severity,omitempty"`
	Details     string                 `json:"details,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}
