package ws

// Multi-réplica fan-out coverage for the TF_TWIN_EVENTS journal consumer
// (R4, Plan 04-02). These tests pin the three behaviours the fan-out contract
// depends on:
//   - routing: each event type dispatches to the expected broadcaster with the
//     expected arguments (spied via the broadcast function-var pattern, the
//     same approach twin_state_test.go uses for the KV watch);
//   - resilience: an unknown/invalid/empty envelope is skipped (never panics)
//     and a dead subscription triggers a resubscribe without crashing;
//   - cross-replica semantics: a message that "came from another replica"
//     reaches THIS replica's local broadcasters, and the no-queue-group choice
//     that makes that true is pinned.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"flow-core/internal/twinevents"
)

// spyCall records a single broadcast invocation captured by the broadcast
// spies. kind distinguishes which broadcaster fired.
type spyCall struct {
	kind     string // "telemetry" | "attributes" | "alarm"
	entityID string
	scope    string
	data     map[string]interface{}
	ts       int64
}

type broadcastSpy struct {
	calls []spyCall
}

func (s *broadcastSpy) telemetry(entityID string, data map[string]interface{}, ts int64) {
	s.calls = append(s.calls, spyCall{kind: "telemetry", entityID: entityID, data: data, ts: ts})
}

func (s *broadcastSpy) attributes(entityID, scope string, data map[string]interface{}) {
	s.calls = append(s.calls, spyCall{kind: "attributes", entityID: entityID, scope: scope, data: data})
}

func (s *broadcastSpy) alarm(entityID string, event map[string]interface{}) {
	s.calls = append(s.calls, spyCall{kind: "alarm", entityID: entityID, data: event})
}

// installBroadcastSpies swaps the journal broadcast vars for a spy and restores
// them on test cleanup. Tests never need real WS sessions — routing correctness
// is proven by which spy fired with which arguments.
func installBroadcastSpies(t *testing.T) *broadcastSpy {
	t.Helper()
	spy := &broadcastSpy{}
	origTelemetry := journalBroadcastTelemetry
	origAttributes := journalBroadcastAttributes
	origAlarm := journalBroadcastAlarm
	journalBroadcastTelemetry = spy.telemetry
	journalBroadcastAttributes = spy.attributes
	journalBroadcastAlarm = spy.alarm
	t.Cleanup(func() {
		journalBroadcastTelemetry = origTelemetry
		journalBroadcastAttributes = origAttributes
		journalBroadcastAlarm = origAlarm
	})
	return spy
}

// encodeJournalMsg marshals an event into a NATS message with the canonical
// TF_TWIN_EVENTS subject shape. Routing uses the envelope fields, not the
// subject, so the subject here is informational.
func encodeJournalMsg(t *testing.T, ev journalEvent) *nats.Msg {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return &nats.Msg{
		Subject: "tf.twin.events." + ev.TenantID + "." + ev.EntityType + "." + ev.EntityID,
		Data:    b,
	}
}

// TestJournalEnvelopeDecodeAndRoute — a valid twinevents envelope decodes into
// the expected fields and dispatches to the correct broadcaster with those
// fields. Decode correctness is proven through routing, the only observable
// side effect of decoding.
func TestJournalEnvelopeDecodeAndRoute(t *testing.T) {
	spy := installBroadcastSpies(t)
	ev := journalEvent{
		TenantID: "tenant-a", EntityType: "DEVICE", EntityID: "dev-1",
		Type: twinevents.EventAttributeSaved, TS: 1234567890123,
		Payload: map[string]interface{}{"scope": "SERVER_SCOPE", "values": map[string]interface{}{"config": "v1"}},
	}
	routeJournalMessage(encodeJournalMsg(t, ev))

	if len(spy.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(spy.calls))
	}
	got := spy.calls[0]
	if got.kind != "attributes" || got.entityID != "dev-1" || got.scope != "SERVER_SCOPE" {
		t.Fatalf("call = %#v, want attributes/dev-1/SERVER_SCOPE", got)
	}
	if got.data["config"] != "v1" {
		t.Fatalf("data = %#v, want config=v1 (decoded from JSON)", got.data)
	}
}

// TestJournalRoutingByType — the routing table maps every event type to exactly
// one broadcaster (see the plan SUMMARY for the evidence-backed table).
func TestJournalRoutingByType(t *testing.T) {
	cases := []struct {
		name string
		ev   journalEvent
		kind string
	}{
		{"attribute_saved", journalEvent{EntityID: "d1", Type: twinevents.EventAttributeSaved,
			Payload: map[string]interface{}{"scope": "SHARED_SCOPE", "values": map[string]interface{}{"k": "v"}}}, "attributes"},
		{"feature_saved", journalEvent{EntityID: "d1", Type: twinevents.EventFeatureSaved,
			Payload: map[string]interface{}{"energy": "on"}}, "attributes"},
		{"device_state_saved", journalEvent{EntityID: "d1", Type: twinevents.EventDeviceStateSaved, TS: 5,
			Payload: map[string]interface{}{"key": "active", "value": true}}, "telemetry"},
		{"alarm_saved", journalEvent{EntityID: "d1", Type: twinevents.EventAlarmSaved,
			Payload: map[string]interface{}{"id": "al-1"}}, "alarm"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spy := installBroadcastSpies(t)
			routeJournalEvent(tc.ev)
			if len(spy.calls) != 1 {
				t.Fatalf("calls = %d, want 1", len(spy.calls))
			}
			got := spy.calls[0]
			if got.kind != tc.kind {
				t.Fatalf("kind = %q, want %q (call=%#v)", got.kind, tc.kind, got)
			}
			if got.entityID != "d1" {
				t.Fatalf("entityID = %q, want d1", got.entityID)
			}
		})
	}
}

// TestJournalAttributePassesScopeAndValues — an attribute event's scope and
// values travel through unchanged into BroadcastAttributes (the SaveAttributesKV
// payload shape).
func TestJournalAttributePassesScopeAndValues(t *testing.T) {
	spy := installBroadcastSpies(t)
	routeJournalEvent(journalEvent{EntityID: "d1", Type: twinevents.EventAttributeSaved,
		Payload: map[string]interface{}{"scope": "CLIENT_SCOPE", "values": map[string]interface{}{"a": float64(1), "b": "x"}}})
	got := spy.calls[0]
	if got.scope != "CLIENT_SCOPE" {
		t.Fatalf("scope = %q, want CLIENT_SCOPE", got.scope)
	}
	if got.data["a"] != float64(1) || got.data["b"] != "x" {
		t.Fatalf("data = %#v, want {a:1, b:x}", got.data)
	}
}

// TestJournalFeatureUsesServerScope — feature events fan out as SERVER_SCOPE
// attributes (features persist through SaveAttributesKV(SERVER_SCOPE, flat)).
func TestJournalFeatureUsesServerScope(t *testing.T) {
	spy := installBroadcastSpies(t)
	routeJournalEvent(journalEvent{EntityID: "d1", Type: twinevents.EventFeatureSaved,
		Payload: map[string]interface{}{"energy": "on"}})
	got := spy.calls[0]
	if got.scope != "SERVER_SCOPE" {
		t.Fatalf("scope = %q, want SERVER_SCOPE (features persist as SERVER_SCOPE)", got.scope)
	}
	if got.data["energy"] != "on" {
		t.Fatalf("data = %#v, want energy=on", got.data)
	}
}

// TestJournalDeviceStateUsesEventTS — a device-state tick carries the event's
// own ts as the sample time and its key/value as the telemetry pair.
func TestJournalDeviceStateUsesEventTS(t *testing.T) {
	spy := installBroadcastSpies(t)
	routeJournalEvent(journalEvent{EntityID: "d1", Type: twinevents.EventDeviceStateSaved, TS: 999,
		Payload: map[string]interface{}{"key": "active", "value": true}})
	got := spy.calls[0]
	if got.ts != 999 {
		t.Fatalf("ts = %d, want 999 (event ts is the sample time)", got.ts)
	}
	if got.data["active"] != true {
		t.Fatalf("data = %#v, want {active:true}", got.data)
	}
}

// TestJournalAlarmPassesPayload — an alarm event's payload travels unchanged
// into BroadcastAlarmEvent.
func TestJournalAlarmPassesPayload(t *testing.T) {
	spy := installBroadcastSpies(t)
	payload := map[string]interface{}{"id": "al-1", "severity": "CRITICAL"}
	routeJournalEvent(journalEvent{EntityID: "d1", Type: twinevents.EventAlarmSaved, Payload: payload})
	got := spy.calls[0]
	if got.kind != "alarm" || got.data["id"] != "al-1" || got.data["severity"] != "CRITICAL" {
		t.Fatalf("call = %#v, want alarm with id/severity passed through", got)
	}
}

// TestJournalRelationSkipped — relations have no WS broadcaster today (they
// surface via the twin API / topology REST), so the event is logged and skipped,
// never broadcast and never a panic.
func TestJournalRelationSkipped(t *testing.T) {
	spy := installBroadcastSpies(t)
	routeJournalEvent(journalEvent{EntityID: "d1", Type: twinevents.EventRelationSaved,
		Payload: map[string]interface{}{"from": "a", "to": "b"}})
	if len(spy.calls) != 0 {
		t.Fatalf("calls = %d, want 0 (relations have no WS broadcaster)", len(spy.calls))
	}
}

// TestJournalUnknownTypeSkipped — a forward-compatible unknown event type is
// logged and skipped, never broadcast and never a panic.
func TestJournalUnknownTypeSkipped(t *testing.T) {
	spy := installBroadcastSpies(t)
	routeJournalEvent(journalEvent{EntityID: "d1", Type: "twin.unknown.future",
		Payload: map[string]interface{}{}})
	if len(spy.calls) != 0 {
		t.Fatalf("calls = %d, want 0 (unknown type skipped)", len(spy.calls))
	}
}

// TestJournalEmptyEnvelopeSkipped — an empty NATS payload is logged and skipped.
func TestJournalEmptyEnvelopeSkipped(t *testing.T) {
	spy := installBroadcastSpies(t)
	routeJournalMessage(&nats.Msg{Subject: "tf.twin.events.t.DEVICE.d", Data: nil})
	if len(spy.calls) != 0 {
		t.Fatalf("calls = %d, want 0 (empty envelope skipped)", len(spy.calls))
	}
}

// TestJournalInvalidEnvelopeSkipped — a malformed payload is logged and skipped,
// never a panic.
func TestJournalInvalidEnvelopeSkipped(t *testing.T) {
	spy := installBroadcastSpies(t)
	routeJournalMessage(&nats.Msg{Subject: "tf.twin.events.t.DEVICE.d", Data: []byte("{not json")})
	if len(spy.calls) != 0 {
		t.Fatalf("calls = %d, want 0 (invalid envelope skipped)", len(spy.calls))
	}
}

// TestJournalMissingPayloadSkipped — an attribute event without scope/values or
// a device-state event without key is logged and skipped (defensive, best-effort).
func TestJournalMissingPayloadSkipped(t *testing.T) {
	spy := installBroadcastSpies(t)
	routeJournalEvent(journalEvent{EntityID: "d1", Type: twinevents.EventAttributeSaved, Payload: map[string]interface{}{}})
	routeJournalEvent(journalEvent{EntityID: "d1", Type: twinevents.EventDeviceStateSaved, Payload: map[string]interface{}{"value": true}})
	if len(spy.calls) != 0 {
		t.Fatalf("calls = %d, want 0 (missing required payload skipped)", len(spy.calls))
	}
}

// TestJournalQueueGroupDecision pins the fan-out choice: NO queue group. Each
// replica must consume the full journal to deliver every event to ITS OWN
// sessions (sessions are local to a replica); a queue group would hand each
// event to exactly one replica and silently starve the others. The empty
// constant is the single source of truth for the production subscribe path.
func TestJournalQueueGroupDecision(t *testing.T) {
	if journalQueueGroup != "" {
		t.Fatalf("journalQueueGroup = %q, want \"\" (no queue group — every replica consumes the full journal)", journalQueueGroup)
	}
}

// TestJournalCrossReplicaFanOut — a message "from another replica" (simulated
// by feeding the consumer a decoded event whose subject/envelope carries a
// remote tenant+entity) reaches THIS replica's local broadcasters. The
// consumer performs no origin filtering and no queue-group partitioning, so a
// change published anywhere reaches every replica's subscribers.
func TestJournalCrossReplicaFanOut(t *testing.T) {
	spy := installBroadcastSpies(t)
	remote := journalEvent{
		TenantID: "tenant-remote", EntityType: "DEVICE", EntityID: "dev-remote",
		Type: twinevents.EventDeviceStateSaved, TS: 42,
		Payload: map[string]interface{}{"key": "active", "value": true},
	}
	routeJournalMessage(encodeJournalMsg(t, remote))

	if len(spy.calls) != 1 {
		t.Fatalf("calls = %d, want 1 (a change published by another replica must reach local broadcasters)", len(spy.calls))
	}
	if spy.calls[0].entityID != "dev-remote" {
		t.Fatalf("entityID = %q, want dev-remote", spy.calls[0].entityID)
	}
}

// TestJournalResubscribeNoPanic — when the subscription channel closes the
// resubscribe loop re-subscribes (mirroring runTwinStateWatch) and never
// panics. A fake subscribe fn simulates subscription death without a NATS
// server; journalRetryBase is shrunk so the test stays fast.
func TestJournalResubscribeNoPanic(t *testing.T) {
	origFn := journalSubscribeFn
	origBase := journalRetryBase
	journalRetryBase = time.Millisecond
	defer func() {
		journalSubscribeFn = origFn
		journalRetryBase = origBase
	}()

	var subscribes int
	var lastSubject string
	deadCh := make(chan struct{})
	journalSubscribeFn = func(ctx context.Context, nc *nats.Conn, subject string) (<-chan struct{}, error) {
		subscribes++
		lastSubject = subject
		if subscribes == 1 {
			// First subscribe: return a live channel the test closes to
			// simulate subscription death.
			return deadCh, nil
		}
		// Resubscribe: stay alive until ctx is cancelled, so the loop idles
		// instead of spinning.
		<-ctx.Done()
		return deadCh, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() {
		runJournalResubscribe(ctx, nil, "tf.twin.events.>")
		close(doneCh)
	}()

	waitFor := func(cond func() bool, what string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(time.Millisecond)
		}
	}

	waitFor(func() bool { return subscribes >= 1 }, "first subscribe")
	if lastSubject != "tf.twin.events.>" {
		t.Fatalf("subscribe subject = %q, want tf.twin.events.>", lastSubject)
	}
	close(deadCh) // simulate subscription death
	waitFor(func() bool { return subscribes >= 2 }, "resubscribe after channel close")
	cancel()
	select {
	case <-doneCh:
		// clean, panic-free exit.
	case <-time.After(2 * time.Second):
		t.Fatal("runJournalResubscribe did not exit after cancel")
	}
}

// TestStartJournalConsumerDisabledWhenNATSUnreachable — an unreachable NATS
// makes StartJournalConsumer a graceful no-op: it returns without panicking
// and without starting a resubscribe loop (mirrors the 04-01 publisher
// posture). Port 1 is closed, so the 5s connect timeout is never reached.
func TestStartJournalConsumerDisabledWhenNATSUnreachable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneCh := make(chan struct{})
	go func() {
		StartJournalConsumer(ctx, "nats://127.0.0.1:1", "tf.twin.events.>")
		close(doneCh)
	}()
	select {
	case <-doneCh:
		// graceful no-op — the consumer disabled itself.
	case <-time.After(3 * time.Second):
		t.Fatal("StartJournalConsumer blocked on unreachable NATS; must disable gracefully")
	}
}
