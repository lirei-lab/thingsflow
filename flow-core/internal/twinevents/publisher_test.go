package twinevents

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
)

// spyCall is the captured argument tuple of one publish call.
type spyCall struct {
	tenantID, entityType, entityID, eventType string
	payload                                   map[string]interface{}
}

// publishSpy records published event calls so unit tests can assert what
// Publish would have sent without a NATS server.
type publishSpy struct {
	calls []spyCall
}

func (s *publishSpy) record(tenantID, entityType, entityID, eventType string, payload map[string]interface{}) {
	s.calls = append(s.calls, spyCall{tenantID, entityType, entityID, eventType, payload})
}

// fakeConn is a non-nil *nats.Conn used only to pass the disabled guard in
// Publish during unit tests; no NATS method is ever called on it.
func fakeConn() *nats.Conn { return &nats.Conn{} }

// resetState returns the package under test to a clean, disabled default.
func resetState() {
	initOnce = sync.Once{}
	nc = nil
	subject = ""
	publishFn = publishViaNATS
}

func TestPublishDisabledNoop(t *testing.T) {
	resetState()
	spy := &publishSpy{}
	publishFn = spy.record

	// Publisher disabled (nc nil) — Publish must be a silent no-op and the
	// event sink must never be reached.
	Publish("tenant-a", "DEVICE", "device-1", EventAttributeSaved, map[string]interface{}{"k": "v"})

	if len(spy.calls) != 0 {
		t.Fatalf("Publish with a disabled publisher reached the sink: %+v", spy.calls)
	}
}

func TestPublishEmptyIdentitySkipped(t *testing.T) {
	resetState()
	spy := &publishSpy{}
	publishFn = spy.record
	nc = fakeConn() // non-nil so the skip is attributable to the empty identity

	Publish("", "DEVICE", "device-1", EventAttributeSaved, map[string]interface{}{"k": "v"})
	Publish("tenant-a", "DEVICE", "", EventAttributeSaved, map[string]interface{}{"k": "v"})

	if len(spy.calls) != 0 {
		t.Fatalf("Publish with an empty tenant/entity reached the sink: %+v", spy.calls)
	}
}

func TestPublishFnSpyInjection(t *testing.T) {
	resetState()
	spy := &publishSpy{}
	publishFn = spy.record
	nc = fakeConn()

	Publish("tenant-a", "DEVICE", "device-1", EventAttributeSaved, map[string]interface{}{"k": "v"})

	if len(spy.calls) != 1 {
		t.Fatalf("Publish reached the sink %d times, want 1", len(spy.calls))
	}
	got := spy.calls[0]
	if got.tenantID != "tenant-a" || got.entityType != "DEVICE" || got.entityID != "device-1" || got.eventType != EventAttributeSaved {
		t.Fatalf("unexpected event args: %+v", got)
	}
	if got.payload["k"] != "v" {
		t.Fatalf("payload not forwarded: %+v", got.payload)
	}
}

func TestComposeSubject(t *testing.T) {
	if got := composeSubject("tf.twin.events", "tenant-a", "DEVICE", "uuid-1"); got != "tf.twin.events.tenant-a.device.uuid-1" {
		t.Fatalf("composeSubject = %q, want %q", got, "tf.twin.events.tenant-a.device.uuid-1")
	}
	// Type is lowercased; an already-lowercase type stays unchanged.
	if got := composeSubject("tf.twin.events", "t", "asset", "id"); got != "tf.twin.events.t.asset.id" {
		t.Fatalf("composeSubject (lower type) = %q, want %q", got, "tf.twin.events.t.asset.id")
	}
}

func TestBasePrefix(t *testing.T) {
	if got := basePrefix("tf.twin.events.>"); got != "tf.twin.events" {
		t.Fatalf("basePrefix(tf.twin.events.>) = %q, want %q", got, "tf.twin.events")
	}
	if got := basePrefix("tf.twin.events"); got != "tf.twin.events" {
		t.Fatalf("basePrefix(no wildcard) = %q, want %q", got, "tf.twin.events")
	}
	if got := basePrefix(""); got != "tf.twin.events" {
		t.Fatalf("basePrefix(empty) = %q, want %q", got, "tf.twin.events")
	}
}

func TestEnvelopeFields(t *testing.T) {
	payload := map[string]interface{}{"k": "v", "n": float64(1)}
	ev := composeEvent("tenant-a", "DEVICE", "device-1", EventFeatureSaved, payload, 1234)

	if ev.TenantID != "tenant-a" || ev.EntityType != "DEVICE" || ev.EntityID != "device-1" || ev.Type != EventFeatureSaved || ev.TS != 1234 {
		t.Fatalf("unexpected envelope: %+v", ev)
	}

	// The JSON shape is the TB camelCase contract the fan-out consumer parses.
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"tenantId", "entityType", "entityId", "type", "ts", "payload"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("envelope missing field %q in %s", key, string(b))
		}
	}
	if m["type"] != EventFeatureSaved {
		t.Fatalf("envelope type = %v, want %s", m["type"], EventFeatureSaved)
	}
	if _, ok := m["tenant_id"]; ok {
		t.Fatalf("envelope must use camelCase tenantId, got snake_case key in %s", string(b))
	}
}
