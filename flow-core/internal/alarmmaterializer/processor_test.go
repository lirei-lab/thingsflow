package alarmmaterializer

import (
	"context"
	"database/sql"
	"testing"
)

type fakeRepo struct {
	active          *ActiveAlarm
	inserted        []AlarmRecord
	linked          []EntityAlarmLink
	extended        []string
	escalated       []struct{ id, severity string }
	cleared         []string
	refreshedCounts []string
}

func (r *fakeRepo) FindActiveAlarm(_ context.Context, deviceID, alarmType string) (ActiveAlarm, error) {
	if r.active == nil || r.active.DeviceID != deviceID || r.active.AlarmType != alarmType {
		return ActiveAlarm{}, sql.ErrNoRows
	}
	return *r.active, nil
}

func (r *fakeRepo) InsertAlarm(_ context.Context, alarm AlarmRecord) error {
	r.inserted = append(r.inserted, alarm)
	return nil
}

func (r *fakeRepo) LinkEntityAlarm(_ context.Context, link EntityAlarmLink) error {
	r.linked = append(r.linked, link)
	return nil
}

func (r *fakeRepo) ExtendAlarm(_ context.Context, alarmID string, _ int64) error {
	r.extended = append(r.extended, alarmID)
	return nil
}

func (r *fakeRepo) EscalateAlarm(_ context.Context, alarmID, severity string, _ int64) error {
	r.escalated = append(r.escalated, struct{ id, severity string }{alarmID, severity})
	return nil
}

func (r *fakeRepo) ClearAlarm(_ context.Context, alarmID string, _ int64) error {
	r.cleared = append(r.cleared, alarmID)
	return nil
}

func (r *fakeRepo) RefreshAncestorAlarmCounts(_ context.Context, tenantID, deviceID string) error {
	r.refreshedCounts = append(r.refreshedCounts, tenantID+"/"+deviceID)
	return nil
}

func TestParseIntentAcceptsBentoAlarmEvent(t *testing.T) {
	intent, err := ParseIntent([]byte(`{
		"action": "create_or_update",
		"tenantId": "aaaaaaaa-1dd2-11b2-8080-808080808080",
		"deviceId": "11111111-2222-3333-4444-555555555555",
		"alarmType": "HighTemperature",
		"severity": "MAJOR",
		"ts": 1779300000000,
		"details": {"temperature": 33.5, "threshold": 30}
	}`))
	if err != nil {
		t.Fatalf("ParseIntent returned error: %v", err)
	}
	if intent.Action != ActionCreateOrUpdate {
		t.Fatalf("Action = %q, want %q", intent.Action, ActionCreateOrUpdate)
	}
	if intent.Details["temperature"] != 33.5 {
		t.Fatalf("temperature detail = %#v", intent.Details["temperature"])
	}
}

func TestApplyIntentCreatesNewAlarm(t *testing.T) {
	repo := &fakeRepo{}
	intent := Intent{
		Action:    ActionCreateOrUpdate,
		TenantID:  "aaaaaaaa-1dd2-11b2-8080-808080808080",
		DeviceID:  "11111111-2222-3333-4444-555555555555",
		AlarmType: "HighTemperature",
		Severity:  "MAJOR",
		TS:        1779300000000,
		Details:   map[string]any{"temperature": 33.5},
	}

	result, err := ApplyIntent(context.Background(), repo, intent, 1779300000123, func() string {
		return "99999999-8888-7777-6666-555555555555"
	})
	if err != nil {
		t.Fatalf("ApplyIntent returned error: %v", err)
	}
	if result.Action != "created" || result.AlarmID != "99999999-8888-7777-6666-555555555555" {
		t.Fatalf("result = %#v", result)
	}
	if len(repo.inserted) != 1 {
		t.Fatalf("inserted count = %d, want 1", len(repo.inserted))
	}
	if repo.inserted[0].Severity != "MAJOR" {
		t.Fatalf("severity = %q", repo.inserted[0].Severity)
	}
	if repo.inserted[0].AdditionalInfo == "" || repo.inserted[0].AdditionalInfo == "{}" {
		t.Fatalf("additional info was not populated: %q", repo.inserted[0].AdditionalInfo)
	}
	if len(repo.linked) != 1 {
		t.Fatalf("linked count = %d, want 1", len(repo.linked))
	}
	if len(repo.refreshedCounts) != 1 {
		t.Fatalf("alarm count refresh count = %d, want 1", len(repo.refreshedCounts))
	}
}

func TestApplyIntentExtendsExistingAlarmWithoutDowngrade(t *testing.T) {
	repo := &fakeRepo{active: &ActiveAlarm{
		ID:        "active-alarm",
		DeviceID:  "11111111-2222-3333-4444-555555555555",
		AlarmType: "HighTemperature",
		Severity:  "CRITICAL",
	}}
	intent := Intent{
		Action:    ActionCreateOrUpdate,
		TenantID:  "aaaaaaaa-1dd2-11b2-8080-808080808080",
		DeviceID:  "11111111-2222-3333-4444-555555555555",
		AlarmType: "HighTemperature",
		Severity:  "MAJOR",
	}

	result, err := ApplyIntent(context.Background(), repo, intent, 1779300000123, func() string {
		return "new-id"
	})
	if err != nil {
		t.Fatalf("ApplyIntent returned error: %v", err)
	}
	if result.Action != "extended" {
		t.Fatalf("result action = %q", result.Action)
	}
	if len(repo.extended) != 1 || repo.extended[0] != "active-alarm" {
		t.Fatalf("extended = %#v", repo.extended)
	}
	if len(repo.escalated) != 0 {
		t.Fatalf("unexpected escalation: %#v", repo.escalated)
	}
}

func TestApplyIntentEscalatesExistingAlarm(t *testing.T) {
	repo := &fakeRepo{active: &ActiveAlarm{
		ID:        "active-alarm",
		DeviceID:  "11111111-2222-3333-4444-555555555555",
		AlarmType: "HighTemperature",
		Severity:  "WARNING",
	}}
	intent := Intent{
		Action:    ActionCreateOrUpdate,
		TenantID:  "aaaaaaaa-1dd2-11b2-8080-808080808080",
		DeviceID:  "11111111-2222-3333-4444-555555555555",
		AlarmType: "HighTemperature",
		Severity:  "CRITICAL",
	}

	result, err := ApplyIntent(context.Background(), repo, intent, 1779300000123, func() string {
		return "new-id"
	})
	if err != nil {
		t.Fatalf("ApplyIntent returned error: %v", err)
	}
	if result.Action != "escalated" {
		t.Fatalf("result action = %q", result.Action)
	}
	if len(repo.escalated) != 1 || repo.escalated[0].severity != "CRITICAL" {
		t.Fatalf("escalated = %#v", repo.escalated)
	}
}

func TestApplyIntentClearsExistingAlarm(t *testing.T) {
	repo := &fakeRepo{active: &ActiveAlarm{
		ID:        "active-alarm",
		DeviceID:  "11111111-2222-3333-4444-555555555555",
		AlarmType: "HighTemperature",
		Severity:  "MAJOR",
	}}
	intent := Intent{
		Action:    ActionClear,
		TenantID:  "aaaaaaaa-1dd2-11b2-8080-808080808080",
		DeviceID:  "11111111-2222-3333-4444-555555555555",
		AlarmType: "HighTemperature",
	}

	result, err := ApplyIntent(context.Background(), repo, intent, 1779300000123, func() string {
		return "new-id"
	})
	if err != nil {
		t.Fatalf("ApplyIntent returned error: %v", err)
	}
	if result.Action != "cleared" {
		t.Fatalf("result action = %q", result.Action)
	}
	if len(repo.cleared) != 1 || repo.cleared[0] != "active-alarm" {
		t.Fatalf("cleared = %#v", repo.cleared)
	}
}

func TestApplyIntentRejectsInvalidIntent(t *testing.T) {
	_, err := ApplyIntent(context.Background(), &fakeRepo{}, Intent{
		Action:    ActionCreateOrUpdate,
		TenantID:  "aaaaaaaa-1dd2-11b2-8080-808080808080",
		AlarmType: "HighTemperature",
	}, 1779300000123, func() string {
		return "new-id"
	})
	if err == nil {
		t.Fatal("ApplyIntent returned nil error for missing device id")
	}
}
