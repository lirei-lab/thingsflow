package desiredstate

import (
	"errors"
	"testing"
)

// errTestNotFound signals an unresolvable device in DeviceLookup tests.
var errTestNotFound = errors.New("device not found")

// publishSpy records retained-publish calls so unit tests assert what
// DeliverDesired would send without a live broker.
type publishSpy struct {
	calls []publishCall
}

type publishCall struct {
	mqttID  string
	payload []byte
}

func (s *publishSpy) record(mqttID string, payload []byte) error {
	s.calls = append(s.calls, publishCall{mqttID, payload})
	return nil
}

// resetConfig forces the delivery config to a known state between tests. The
// config is sync.Once-guarded in production; burning the Once (mirroring the
// rpc test pattern) makes loadConfig() return the cfg set here instead of
// re-reading env.
func resetConfig(enabled bool) {
	cfgOnce.Do(func() {}) // burn the Once so loadConfig() returns what we set
	cfg = config{enabled: enabled}
}

func TestDesiredTopic(t *testing.T) {
	if got := DesiredTopic("abc123"); got != "thingsflow/devices/abc123/desired" {
		t.Fatalf("DesiredTopic = %q", got)
	}
}

// TestDeliverDesiredPublishesRetained — DeliverDesired routes to the retained
// publish sink with the correct topic and payload; the sink is the function var
// production swaps for the rmqtt HTTP API (retain:true asserted at the call
// site by publishRetained, and the topic is DesiredTopic's contract).
func TestDeliverDesiredPublishesRetained(t *testing.T) {
	orig := publishDesiredFn
	defer func() { publishDesiredFn = orig }()
	spy := &publishSpy{}
	publishDesiredFn = spy.record

	resetConfig(true)
	DeliverDesired("dev-1", []byte(`{"features":{"energy":{"desiredProperties":{"target_kwh":50}}}}`))

	if len(spy.calls) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(spy.calls))
	}
	if spy.calls[0].mqttID != "dev-1" {
		t.Fatalf("mqttID = %q, want dev-1", spy.calls[0].mqttID)
	}
	if string(spy.calls[0].payload) != `{"features":{"energy":{"desiredProperties":{"target_kwh":50}}}}` {
		t.Fatalf("payload = %s", spy.calls[0].payload)
	}
}

// TestDeliverDesiredEmptyIdentitySkipped — an empty mqttId or payload is a
// silent no-op (defensive).
func TestDeliverDesiredEmptyIdentitySkipped(t *testing.T) {
	orig := publishDesiredFn
	defer func() { publishDesiredFn = orig }()
	spy := &publishSpy{}
	publishDesiredFn = spy.record

	resetConfig(true)
	DeliverDesired("", []byte(`{}`))
	DeliverDesired("dev-1", nil)
	DeliverDesired("dev-1", []byte{})

	if len(spy.calls) != 0 {
		t.Fatalf("publish calls = %d, want 0 (empty identity/payload skipped)", len(spy.calls))
	}
}

// TestDeliverDesiredDisabledNoop — disabled delivery never reaches the sink.
func TestDeliverDesiredDisabledNoop(t *testing.T) {
	orig := publishDesiredFn
	defer func() { publishDesiredFn = orig }()
	spy := &publishSpy{}
	publishDesiredFn = spy.record

	resetConfig(false)
	DeliverDesired("dev-1", []byte(`{}`))

	if len(spy.calls) != 0 {
		t.Fatalf("publish calls = %d, want 0 (disabled)", len(spy.calls))
	}
}

// TestDeliverDesiredForEntityUsesLookup — the UUID→mqttId convenience resolves
// via DeviceLookup and skips an unresolvable device without panicking.
func TestDeliverDesiredForEntityUsesLookup(t *testing.T) {
	orig := publishDesiredFn
	origLookup := DeviceLookup
	defer func() {
		publishDesiredFn = orig
		DeviceLookup = origLookup
	}()
	spy := &publishSpy{}
	publishDesiredFn = spy.record
	resetConfig(true)

	DeviceLookup = func(deviceID string) (string, string, error) {
		if deviceID == "33333333-3333-3333-3333-333333333333" {
			return "33333333333333333333333333333333", "tenant-a", nil
		}
		return "", "", errTestNotFound
	}

	DeliverDesiredForEntity("33333333-3333-3333-3333-333333333333", []byte(`{"x":1}`))
	if len(spy.calls) != 1 || spy.calls[0].mqttID != "33333333333333333333333333333333" {
		t.Fatalf("calls = %+v, want one publish to stripped mqttId", spy.calls)
	}

	DeliverDesiredForEntity("missing-device", []byte(`{"x":1}`))
	if len(spy.calls) != 1 {
		t.Fatalf("unresolvable device reached the sink: %+v", spy.calls)
	}
}
